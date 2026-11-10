// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { describe, expect, it } from "vitest";
import { handle, limit, type Subscription, utf8Bytes } from "./protocol.ts";

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
      repository: `o/${String(i).padStart(67, "r")}`,
    });
    const full: Subscription[] = Array.from({ length: 100 }, (_, i) =>
      named(i),
    );
    const last = (length: number) => ({
      provider: "github",
      event: "push",
      repository: `o/${"r".repeat(length)}`,
    });

    it("accepts a list of exactly 12288 bytes", () => {
      const next = [...full, last(14)];
      expect(utf8Bytes(JSON.stringify({ subscriptions: next }))).toBe(12288);
      expect(handle(full, frame("subscribe", last(14))).next).toStrictEqual(
        next,
      );
    });

    it("refuses a list of 12289 bytes", () => {
      expect(
        utf8Bytes(JSON.stringify({ subscriptions: [...full, last(15)] })),
      ).toBe(12289);
      expect(handle(full, frame("subscribe", last(15)))).toStrictEqual(
        error("i", "limit", "subscriptions would exceed 12288 bytes"),
      );
    });

    it("counts UTF-8 bytes rather than UTF-16 code units", () => {
      const value = "é😀";
      expect(value.length).toBe(3);
      expect(utf8Bytes(value)).toBe(6);
    });

    it("words the limit error", () => {
      expect(limit("x")).toStrictEqual({
        type: "error",
        id: "x",
        code: "limit",
        message: "subscriptions would exceed 12288 bytes",
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

describe("subscription filters", () => {
  const base = { ...GITHUB, event: "workflow_run" };
  for (const field of ["action", "conclusion"] as const) {
    const message = `${field} must be a list of 1 to 8 strings matching ^[a-z0-9_.-]{1,64}$`;
    it.each(
      [
        ["a"],
        ["z"],
        ["0"],
        ["9"],
        ["_"],
        ["."],
        ["-"],
        ["a".repeat(64)],
        ["future_value.2-0"],
        Array.from({ length: 8 }, (_, i) => String(i)),
        Array<string>(8).fill("completed"),
      ].map((values) => ({ values })),
    )(`accepts ${field} $values`, ({ values }) => {
      expect(subscribe({ ...base, [field]: values })).toStrictEqual(
        accepted({ ...base, [field]: [...new Set(values)].sort() }),
      );
    });
    it.each(
      [
        null,
        false,
        1,
        "completed",
        {},
        [],
        [null],
        [1],
        [false],
        [["completed"]],
        [""],
        ["a".repeat(65)],
        ["A"],
        ["/"],
        [" "],
        [":"],
        ["`"],
        ["{"],
        ["é"],
        ["!a"],
        ["a!"],
        ["a\n"],
        ["a", "invalid!"],
        Array<string>(9).fill("completed"),
      ].map((input) => ({ input })),
    )(`rejects invalid ${field} $input`, ({ input }) => {
      expect(subscribe({ ...base, [field]: input })).toStrictEqual(
        bad(message),
      );
    });
  }

  for (const field of ["name", "workflow_path"] as const) {
    const values =
      field === "name"
        ? [" ", "~", "!", '"', "\\", "*", "?", " / test ", "x".repeat(128)]
        : ["A", "Z", "a", "z", "0", "9", "_", ".", "/", "-", "x".repeat(256)];
    it.each(values)(`preserves exact ${field} %j`, (input) => {
      expect(subscribe({ ...base, [field]: input })).toStrictEqual(
        accepted({ ...base, [field]: input }),
      );
    });
    const invalid =
      field === "name"
        ? ["x".repeat(129), "\x1f", "\x7f", "é", "a\n", "\ta"]
        : [
            "x".repeat(257),
            " ",
            "\\",
            ":",
            "@",
            "[",
            "`",
            "{",
            "é",
            "!a",
            "a!",
            "a\n",
          ];
    it.each(
      [null, 1, false, [], ["valid"], {}, "", ...invalid].map((input) => ({
        input,
      })),
    )(`rejects invalid ${field} $input`, ({ input }) => {
      const pattern =
        field === "name"
          ? "^[\\x20-\\x7e]{1,128}$"
          : "^[A-Za-z0-9_./-]{1,256}$";
      expect(subscribe({ ...base, [field]: input })).toStrictEqual(
        bad(`${field} must match ${pattern}`),
      );
    });
  }
});

describe("filter event support", () => {
  const base = { ...GITHUB, event: "workflow_run" };
  const valid = {
    action: ["completed"],
    conclusion: ["failure"],
    name: " CI / test ",
    workflow_path: ".github/workflows/ci.yml",
  };
  const actionEvents = [
    "branch_protection_configuration",
    "branch_protection_rule",
    "check_run",
    "check_suite",
    "code_scanning_alert",
    "commit_comment",
    "custom_property",
    "custom_property_values",
    "dependabot_alert",
    "deploy_key",
    "deployment",
    "deployment_protection_rule",
    "deployment_review",
    "deployment_status",
    "discussion",
    "discussion_comment",
    "github_app_authorization",
    "installation",
    "installation_repositories",
    "installation_target",
    "issue_comment",
    "issue_dependencies",
    "issue_relates_to",
    "issues",
    "label",
    "marketplace_purchase",
    "member",
    "membership",
    "merge_group",
    "meta",
    "milestone",
    "org_block",
    "organization",
    "package",
    "personal_access_token_request",
    "project",
    "project_card",
    "project_column",
    "projects_v2",
    "projects_v2_item",
    "projects_v2_status_update",
    "pull_request",
    "pull_request_review",
    "pull_request_review_comment",
    "pull_request_review_thread",
    "registry_package",
    "release",
    "repository",
    "repository_advisory",
    "repository_dispatch",
    "repository_ruleset",
    "repository_vulnerability_alert",
    "secret_scanning_alert",
    "secret_scanning_alert_location",
    "secret_scanning_scan",
    "security_advisory",
    "sponsorship",
    "star",
    "sub_issues",
    "team",
    "watch",
    "workflow_job",
    "workflow_run",
  ];
  it.each(actionEvents)("supports action on %s", (event) => {
    const subscription = { ...base, event, action: ["future_action"] };
    expect(subscribe(subscription)).toStrictEqual(accepted(subscription));
  });
  it.each([
    "push",
    "create",
    "delete",
    "fork",
    "gollum",
    "page_build",
    "ping",
    "public",
    "repository_import",
    "security_and_analysis",
    "status",
    "team_add",
    "workflow_dispatch",
    "unknown",
  ])("rejects action on %s", (event) => {
    expect(subscribe({ ...base, event, action: ["created"] })).toStrictEqual(
      bad(`action is not supported for github event ${event}`),
    );
    expect(subscribe({ ...base, event })).toStrictEqual(
      accepted({ ...base, event }),
    );
  });
  for (const [field, input] of Object.entries(valid)) {
    it(`rejects ${field} on another provider`, () => {
      expect(
        subscribe({ ...base, provider: "gitlab", [field]: input }),
      ).toStrictEqual(bad(`${field} is supported only for github`));
    });
    it.each([
      { provider: "github" },
      { provider: "github", event: "workflow_run" },
      { provider: "github", repository: "o/r" },
    ])(
      `requires a complete pair for ${field} on subscribe and unsubscribe: %j`,
      (incomplete) => {
        for (const type of ["subscribe", "unsubscribe"]) {
          expect(
            handle([], frame(type, { ...incomplete, [field]: input })),
          ).toStrictEqual(
            bad("filtered subscriptions require event and repository"),
          );
        }
      },
    );
    for (const event of [
      "push",
      "check_run",
      "check_suite",
      "workflow_run",
      "workflow_job",
    ]) {
      const support = {
        action: ["check_run", "check_suite", "workflow_run", "workflow_job"],
        conclusion: [
          "check_run",
          "check_suite",
          "workflow_run",
          "workflow_job",
        ],
        name: ["check_run", "workflow_run", "workflow_job"],
        workflow_path: ["workflow_run"],
      };
      const supported = support[field as keyof typeof support].includes(event);
      it(`${supported ? "accepts" : "rejects"} ${field} on ${event}`, () => {
        const subscription = { ...base, event, [field]: input };
        expect(subscribe(subscription)).toStrictEqual(
          supported
            ? accepted(subscription)
            : bad(`${field} is not supported for github event ${event}`),
        );
      });
    }
  }
});

describe("filter identity and replies", () => {
  const base = { ...GITHUB, event: "workflow_run" };
  const valid = {
    action: ["completed"],
    conclusion: ["failure"],
    name: " CI / test ",
    workflow_path: ".github/workflows/ci.yml",
  };
  it("canonicalizes every list, preserves strings and ignores unknown keys", () => {
    const input = {
      ...base,
      repository: "O/R",
      ...valid,
      action: ["requested", "completed", "completed"],
      conclusion: ["success", "failure", "success"],
      extra: 1,
    };
    const canonical = {
      ...base,
      ...valid,
      action: ["completed", "requested"],
      conclusion: ["failure", "success"],
    };
    expect(subscribe(input)).toStrictEqual(accepted(canonical));
    expect(handle([canonical], frame("subscribe", input))).toStrictEqual({
      ...listed("i", [canonical]),
      next: null,
    });
    expect(handle([canonical], frame("unsubscribe", input))).toStrictEqual({
      ...listed("i", []),
      next: [],
    });
  });

  for (const [field, input] of Object.entries(valid)) {
    it(`includes ${field} in identity and exact removal`, () => {
      const filtered = { ...base, [field]: input };
      const different = {
        ...base,
        [field]: Array.isArray(input) ? ["other"] : "other",
      };
      const current = [base, filtered, different];
      expect(handle([base], frame("subscribe", filtered)).next).toStrictEqual([
        base,
        filtered,
      ]);
      expect(handle([filtered], frame("subscribe", base)).next).toStrictEqual([
        filtered,
        base,
      ]);
      expect(
        handle([filtered], frame("subscribe", different)).next,
      ).toStrictEqual([filtered, different]);
      expect(
        handle(current, frame("unsubscribe", filtered)).next,
      ).toStrictEqual([base, different]);
      expect(handle(current, frame("unsubscribe", base)).next).toStrictEqual([
        filtered,
        different,
      ]);
      expect(handle([filtered], frame("unsubscribe", base))).toStrictEqual(
        error("i", "not_subscribed", "no such subscription"),
      );
      expect(handle([base], frame("unsubscribe", filtered))).toStrictEqual(
        error("i", "not_subscribed", "no such subscription"),
      );
    });
  }

  it.each(["list", "subscribe", "unsubscribe"])(
    "echoes capability only for literal true on %s",
    (type) => {
      const subscription = { ...base, ...valid };
      for (const flag of [true, false, null, "true", 1, undefined]) {
        const current = type === "subscribe" ? [] : [subscription];
        const outcome = handle(
          current,
          JSON.stringify({
            type,
            id: "i",
            subscription,
            subscription_filters: flag,
          }),
        );
        const entries = type === "unsubscribe" ? [] : [subscription];
        expect(outcome.reply).toStrictEqual({
          ...listed("i", entries).reply,
          ...(flag === true ? { subscription_filters: true } : {}),
        });
      }
    },
  );

  it("counts canonical filters and JSON escapes at the attachment boundary", () => {
    const full: Subscription[] = Array.from({ length: 96 }, (_, i) => ({
      ...base,
      action: ["completed"],
      repository: `o/${String(i).padStart(40, "r")}`,
    }));
    const gap = 12288 - utf8Bytes(JSON.stringify({ subscriptions: full }));
    const finalBase = { ...base, ...valid, name: "" };
    const length = gap - utf8Bytes(JSON.stringify(finalBase)) - 1;
    expect(length).toBeGreaterThan(0);
    expect(length).toBeLessThanOrEqual(128);
    const final = { ...finalBase, name: '"'.repeat(length / 2) };
    const next = [...full, final];
    expect(utf8Bytes(JSON.stringify({ subscriptions: next }))).toBe(12288);
    expect(handle(full, frame("subscribe", final)).next).toStrictEqual(next);
    expect(
      handle(
        full,
        JSON.stringify({
          type: "subscribe",
          id: "i",
          subscription_filters: true,
          subscription: { ...final, action: ["completed", "completed"] },
        }),
      ),
    ).toStrictEqual({
      reply: { ...listed("i", next).reply, subscription_filters: true },
      next,
    });
    expect(
      handle(full, frame("subscribe", { ...final, name: `${final.name}x` })),
    ).toStrictEqual({ reply: limit("i"), next: null });
  });
});

describe("echo", () => {
  it.each(["a", "z", "A", "Z", "0", "9", "_", "-", "a".repeat(64)])(
    "accepts nonce %s without changing subscriptions",
    (nonce) => {
      const current = [GITHUB];
      expect(
        handle(current, JSON.stringify({ type: "echo", id: "e", nonce })),
      ).toStrictEqual({
        reply: { type: "echoed", id: "e" },
        next: null,
        echo: nonce,
      });
      expect(current).toStrictEqual([GITHUB]);
    },
  );

  it.each([
    undefined,
    null,
    1,
    [],
    {},
    "",
    "a".repeat(65),
    "!a",
    "a!",
    "a b",
    "a\\b",
    "a\nb",
    "é",
    "a\n",
  ])("rejects nonce %j with the request id", (nonce) => {
    expect(
      handle([], JSON.stringify({ type: "echo", id: "e", nonce })),
    ).toStrictEqual(
      error("e", "bad_message", "nonce must match ^[A-Za-z0-9_-]{1,64}$"),
    );
  });

  it.each([undefined, null, 1, "", "x".repeat(65)])("rejects id %j", (id) => {
    expect(
      handle([], JSON.stringify({ type: "echo", id, nonce: "n" })),
    ).toStrictEqual(
      error(null, "bad_message", "id must be a string of 1 to 64 characters"),
    );
  });

  it("accepts the longest id and ignores extra fields", () => {
    expect(
      handle(
        [],
        JSON.stringify({
          type: "echo",
          id: "i".repeat(64),
          nonce: "n",
          subscription_filters: true,
          extra: 1,
        }),
      ),
    ).toStrictEqual({
      reply: { type: "echoed", id: "i".repeat(64) },
      next: null,
      echo: "n",
    });
  });

  it("applies the message byte cap to echo", () => {
    const request = { type: "echo", id: "e", nonce: "n", pad: "" };
    const size = JSON.stringify(request).length;
    expect(
      handle([], JSON.stringify({ ...request, pad: "a".repeat(4096 - size) })),
    ).toStrictEqual({
      reply: { type: "echoed", id: "e" },
      next: null,
      echo: "n",
    });
    expect(
      handle([], JSON.stringify({ ...request, pad: "a".repeat(4097 - size) })),
    ).toStrictEqual(
      error(null, "bad_message", "messages must be at most 4096 bytes"),
    );
  });
});
