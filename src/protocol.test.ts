// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { describe, expect, it } from "vitest";
import { handle, limit, type Subscription } from "./protocol.ts";

const GITHUB: Subscription = {
  provider: "github",
  event: "push",
  repository: "o/r",
};

function error(id: string | null, code: string, message: string) {
  return { reply: { type: "error", id, code, message }, next: null };
}

function listed(id: string, subscriptions: readonly Subscription[]) {
  return { reply: { type: "subscriptions", id, subscriptions } };
}

function frame(type: string, subscription: unknown, id = "i") {
  return JSON.stringify({ type, id, subscription });
}

describe("frames", () => {
  it("rejects a binary frame", () => {
    expect(handle([], new ArrayBuffer(3))).toStrictEqual(
      error(null, "bad_message", "messages must be text frames"),
    );
  });

  // A list message padded with a "pad" field to exactly `bytes` bytes.
  function padded(bytes: number, filler: string): string {
    const base = JSON.stringify({ type: "list", id: "i", pad: "" });
    const fillerBytes = new TextEncoder().encode(filler).byteLength;
    const count = Math.floor((bytes - base.length) / fillerBytes);
    const message = JSON.stringify({
      type: "list",
      id: "i",
      pad:
        filler.repeat(count) +
        "a".repeat(bytes - base.length - count * fillerBytes),
    });
    expect(new TextEncoder().encode(message).byteLength).toBe(bytes);
    return message;
  }

  it("accepts 4096 bytes", () => {
    expect(handle([], padded(4096, "a"))).toStrictEqual({
      ...listed("i", []),
      next: null,
    });
  });

  it.each([
    ["4097 ASCII bytes", padded(4097, "a")],
    ["4097 bytes in at most 4096 characters", padded(4097, "é")],
  ])("rejects %s", (_name, message) => {
    expect(handle([], message)).toStrictEqual(
      error(null, "bad_message", "messages must be at most 4096 bytes"),
    );
  });

  it("rejects text that is not JSON", () => {
    expect(handle([], "not json")).toStrictEqual(
      error(null, "bad_message", "messages must be JSON"),
    );
  });

  it.each(["[]", "null", "1", '"s"'])("rejects %s as not an object", (m) => {
    expect(handle([], m)).toStrictEqual(
      error(null, "bad_message", "messages must be JSON objects"),
    );
  });

  it("rejects an object with no type", () => {
    expect(handle([], "{}")).toStrictEqual(
      error(null, "bad_message", "unknown message type"),
    );
  });

  it.each([
    ['{"type":"bogus","id":"x"}', "x"],
    ['{"type":"List","id":"x"}', "x"],
    ['{"type": "ping"}', null],
    ['{"type":"bogus","id":""}', null],
    [`{"type":"bogus","id":"${"x".repeat(65)}"}`, null],
  ])("rejects the type of %s", (m, id) => {
    expect(handle([], m)).toStrictEqual(
      error(id, "bad_message", "unknown message type"),
    );
  });

  it.each([
    '{"type":"list"}',
    '{"type":"list","id":""}',
    '{"type":"list","id":5}',
    `{"type":"list","id":"${"x".repeat(65)}"}`,
  ])("rejects the id of %s", (m) => {
    expect(handle([], m)).toStrictEqual(
      error(null, "bad_message", "id must be a string of 1 to 64 characters"),
    );
  });

  it.each(["a", "x".repeat(64)])("lists under the id %s", (id) => {
    expect(handle([GITHUB], `{"type":"list","id":"${id}"}`)).toStrictEqual({
      ...listed(id, [GITHUB]),
      next: null,
    });
  });

  it.each([
    ["no subscription", JSON.stringify({ type: "subscribe", id: "i" })],
    ["a string", frame("subscribe", "x")],
    ["null", frame("subscribe", null)],
    ["an array", frame("subscribe", [])],
  ])("rejects %s as the subscription", (_name, m) => {
    expect(handle([], m)).toStrictEqual(
      error("i", "bad_message", "subscription must be an object"),
    );
  });
});

// Subscribes to `subscription` from an empty list.
function subscribe(subscription: unknown) {
  return handle([], frame("subscribe", subscription));
}

function accepted(stored: Subscription) {
  return { ...listed("i", [stored]), next: [stored] };
}

function bad(message: string) {
  return error("i", "bad_subscription", message);
}

describe("provider", () => {
  const pattern = "provider must match ^[a-z0-9-]{1,32}$";

  it.each(["a", "z", "0", "9", "-", "a".repeat(32)])(
    "accepts %s",
    (provider) => {
      expect(subscribe({ provider })).toStrictEqual(accepted({ provider }));
    },
  );

  it.each([
    "",
    "a".repeat(33),
    "A",
    "GitHub",
    "_",
    ".",
    "/",
    "github!",
    "!github",
    "git hub",
  ])("rejects %j", (provider) => {
    expect(subscribe({ provider })).toStrictEqual(bad(pattern));
  });

  it.each([{}, { provider: 1 }, { provider: null }])(
    "rejects the type of %j",
    (value) => {
      expect(subscribe(value)).toStrictEqual(
        error("i", "bad_message", "provider must be a string"),
      );
    },
  );
});

describe("event", () => {
  const pattern = "event must match ^[a-z0-9_.-]{1,64}$";

  it.each(["a", "z", "0", "9", "_", ".", "-", "pull_request", "a".repeat(64)])(
    "accepts %s",
    (event) => {
      expect(
        subscribe({ provider: "github", event, repository: "o/r" }),
      ).toStrictEqual(
        accepted({ provider: "github", event, repository: "o/r" }),
      );
    },
  );

  it.each(["", "a".repeat(65), "Push", "push!", "!push", "push/x"])(
    "rejects %j",
    (event) => {
      expect(
        subscribe({ provider: "github", event, repository: "o/r" }),
      ).toStrictEqual(bad(pattern));
    },
  );

  it.each([null, 1])("rejects the type of %j", (event) => {
    expect(
      subscribe({ provider: "github", event, repository: "o/r" }),
    ).toStrictEqual(error("i", "bad_message", "event must be a string"));
  });
});

describe("repository", () => {
  const pattern = "repository must be owner/name";
  const characters = ["A", "Z", "a", "z", "0", "9", "_", ".", "-"];

  it.each([
    ["a/b", "a/b"],
    ["Owner/Repo", "owner/repo"],
    [`${"o".repeat(39)}/r`, `${"o".repeat(39)}/r`],
    [`o/${"r".repeat(100)}`, `o/${"r".repeat(100)}`],
    ...characters.map((c) => [`${c}/b`, `${c.toLowerCase()}/b`]),
    ...characters.map((c) => [`b/${c}`, `b/${c.toLowerCase()}`]),
  ])("accepts %s as %s", (repository, stored) => {
    expect(
      subscribe({ provider: "github", event: "push", repository }),
    ).toStrictEqual(
      accepted({ provider: "github", event: "push", repository: stored }),
    );
  });

  it.each([
    "",
    "o",
    "/r",
    "o/",
    `${"o".repeat(40)}/r`,
    `o/${"r".repeat(101)}`,
    "o/r/x",
    "o r/x",
    "!o/r",
    "o!/r",
    "o/!r",
    "o/r!",
  ])("rejects %j", (repository) => {
    expect(
      subscribe({ provider: "github", event: "push", repository }),
    ).toStrictEqual(bad(pattern));
  });

  it.each([null, 1])("rejects the type of %j", (repository) => {
    expect(
      subscribe({ provider: "github", event: "push", repository }),
    ).toStrictEqual(error("i", "bad_message", "repository must be a string"));
  });
});

describe("subscribe", () => {
  it.each([
    [{ provider: "github" }, "event and repository"],
    [{ provider: "github", event: "push" }, "repository"],
    [{ provider: "github", repository: "o/r" }, "event"],
  ])(
    "refuses an incomplete github subscription %j",
    (subscription, missing) => {
      expect(subscribe(subscription)).toStrictEqual(
        bad(
          `github subscription is missing ${missing}; subscribe once per event type and repository`,
        ),
      );
    },
  );

  it.each([
    { provider: "gitlab" },
    { provider: "gitlab", event: "push" },
    { provider: "gitlab", repository: "o/r" },
  ])("keeps optional fields for other providers: %j", (subscription) => {
    expect(subscribe(subscription)).toStrictEqual(accepted(subscription));
  });

  it("ignores unknown fields", () => {
    expect(subscribe({ ...GITHUB, extra: 1 })).toStrictEqual(accepted(GITHUB));
  });

  it("changes nothing when the subscription is already present", () => {
    expect(handle([GITHUB], frame("subscribe", GITHUB))).toStrictEqual({
      ...listed("i", [GITHUB]),
      next: null,
    });
  });

  it("adds a subscription that differs only in provider", () => {
    const other = { ...GITHUB, provider: "gitlab" };
    expect(handle([GITHUB], frame("subscribe", other)).next).toStrictEqual([
      GITHUB,
      other,
    ]);
  });

  it("adds a subscription that differs in event or repository", () => {
    const withEvent = { ...GITHUB, event: "issues" };
    const withRepository = { ...GITHUB, repository: "o/other" };
    const first = handle([GITHUB], frame("subscribe", withEvent));
    expect(first.next).toStrictEqual([GITHUB, withEvent]);
    const second = handle(first.next ?? [], frame("subscribe", withRepository));
    expect(second.next).toStrictEqual([GITHUB, withEvent, withRepository]);
  });

  describe("budget", () => {
    const named = (i: number) => ({
      provider: "github",
      event: "push",
      repository: `o/${String(i).padStart(100, "r")}`,
    });
    const full: Subscription[] = Array.from({ length: 11 }, (_, i) => named(i));
    const last = (length: number) => ({
      provider: "github",
      event: "push",
      repository: `o/${"r".repeat(length)}`,
    });

    it("accepts a list of exactly 1792 bytes", () => {
      const next = [...full, last(13)];
      expect(JSON.stringify({ subscriptions: next }).length).toBe(1792);
      expect(handle(full, frame("subscribe", last(13))).next).toStrictEqual(
        next,
      );
    });

    it("refuses a list of 1793 bytes", () => {
      expect(
        JSON.stringify({ subscriptions: [...full, last(14)] }).length,
      ).toBe(1793);
      expect(handle(full, frame("subscribe", last(14)))).toStrictEqual({
        reply: limit("i"),
        next: null,
      });
    });

    it("words the limit error", () => {
      expect(limit("x")).toStrictEqual({
        type: "error",
        id: "x",
        code: "limit",
        message: "subscriptions would exceed 1792 bytes",
      });
    });
  });
});

describe("unsubscribe", () => {
  it.each([
    { provider: "github" },
    { provider: "github", event: "push" },
    { provider: "github", repository: "o/r" },
  ])("removes an older incomplete subscription %j", (subscription) => {
    expect(
      handle([subscription], frame("unsubscribe", subscription)),
    ).toStrictEqual({
      ...listed("i", []),
      next: [],
    });
  });

  it("removes a present subscription", () => {
    const other = { ...GITHUB, event: "issues" };
    expect(handle([GITHUB, other], frame("unsubscribe", GITHUB))).toStrictEqual(
      { ...listed("i", [other]), next: [other] },
    );
  });

  it("refuses an absent subscription", () => {
    expect(handle([], frame("unsubscribe", GITHUB))).toStrictEqual(
      error("i", "not_subscribed", "no such subscription"),
    );
  });

  it.each([
    ["provider", { ...GITHUB, provider: "gitlab" }],
    ["event", { ...GITHUB, event: "issues" }],
    ["repository", { ...GITHUB, repository: "o/other" }],
  ])("refuses a subscription that differs only in %s", (_name, stored) => {
    expect(handle([stored], frame("unsubscribe", GITHUB))).toStrictEqual(
      error("i", "not_subscribed", "no such subscription"),
    );
    expect(handle([GITHUB], frame("unsubscribe", stored))).toStrictEqual(
      error("i", "not_subscribed", "no such subscription"),
    );
  });

  it("matches a repository without case", () => {
    const stored = { provider: "github", repository: "o/r" };
    expect(
      handle(
        [stored],
        frame("unsubscribe", { provider: "github", repository: "O/R" }),
      ).next,
    ).toStrictEqual([]);
  });
});
