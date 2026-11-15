// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { describe, expect, it } from "vitest";
import {
  CAP_EVENTS,
  CAP_MINUTES,
  charge,
  delivered,
  dequeue,
  enqueue,
  forget,
  forgetOthers,
  MAX_PENDING,
  refund,
  retain,
} from "./cap.ts";
import type { Subscription } from "./protocol.ts";

const A: Subscription = {
  provider: "github",
  event: "push",
  repository: "o/a",
};
const B: Subscription = {
  provider: "github",
  event: "push",
  repository: "o/b",
};

// A SyncKvStorage over a Map, with the listing order the real one has.
function memory(): SyncKvStorage & { readonly map: Map<string, unknown> } {
  const map = new Map<string, unknown>();
  const kv = {
    map,
    get: (key: string) => map.get(key),
    put: (key: string, value: unknown) => {
      map.set(key, structuredClone(value));
    },
    delete: (key: string) => map.delete(key),
    list: (options?: SyncKvListOptions) =>
      [...map.entries()]
        .filter(([key]) => key.startsWith(options?.prefix ?? ""))
        .sort(([a], [b]) => a.localeCompare(b)),
  };
  return kv as unknown as SyncKvStorage & {
    readonly map: Map<string, unknown>;
  };
}

describe("charge", () => {
  it("fixes the cap at 100 events in 5 minutes", () => {
    expect([CAP_EVENTS, CAP_MINUTES]).toStrictEqual([100, 5]);
  });

  it("counts a delivery for each subscription and keeps them apart", () => {
    const kv = memory();
    expect(charge(kv, "c", [A, B], 10, [A, B])).toStrictEqual({
      within: [A, B],
      tripped: [],
    });
    charge(kv, "c", [A], 10, [A, B]);
    expect(delivered(kv, "c", [A, B], 10)).toStrictEqual([2, 1]);
  });

  it("allows the 100th event in the window and trips on the 101st", () => {
    const kv = memory();
    for (let i = 0; i < CAP_EVENTS; i += 1) {
      expect(charge(kv, "c", [A], 10, [A]).tripped).toStrictEqual([]);
    }
    expect(charge(kv, "c", [A], 10, [A])).toStrictEqual({
      within: [],
      tripped: [A],
    });
    // The tripped subscription's counts are gone; the other one is not.
    expect(delivered(kv, "c", [A], 10)).toStrictEqual([0]);
  });

  it("trips only the subscription at the cap", () => {
    const kv = memory();
    for (let i = 0; i < CAP_EVENTS; i += 1) charge(kv, "c", [A], 10, [A]);
    charge(kv, "c", [B], 10, [A, B]);
    expect(charge(kv, "c", [A, B], 10, [A, B])).toStrictEqual({
      within: [B],
      tripped: [A],
    });
  });

  it("counts the current minute and the four before it", () => {
    const kv = memory();
    for (let i = 0; i < CAP_EVENTS; i += 1) charge(kv, "c", [A], 10, [A]);
    expect(charge(kv, "c", [A], 14, [A]).tripped).toStrictEqual([A]);
    for (let i = 0; i < CAP_EVENTS; i += 1) charge(kv, "d", [A], 10, [A]);
    expect(charge(kv, "d", [A], 15, [A]).tripped).toStrictEqual([]);
  });

  it("keeps each connection's counts apart", () => {
    const kv = memory();
    for (let i = 0; i < CAP_EVENTS; i += 1) charge(kv, "c", [A], 10, [A]);
    expect(charge(kv, "d", [A], 10, [A]).tripped).toStrictEqual([]);
  });

  it("writes nothing for no subscriptions", () => {
    const kv = memory();
    expect(charge(kv, "c", [], 10, [])).toStrictEqual({
      within: [],
      tripped: [],
    });
    expect(kv.map.size).toBe(0);
  });

  it("tells subscriptions apart by every filter", () => {
    const kv = memory();
    const variants: Subscription[] = [
      A,
      { ...A, provider: "gitlab" },
      { ...A, event: "issues" },
      { ...A, repository: "o/z" },
      { ...A, action: ["opened"] },
      { ...A, conclusion: ["failure"] },
      { ...A, name: "build" },
      { ...A, workflow_path: "ci.yml" },
    ];
    charge(kv, "c", variants, 10, variants);
    expect(delivered(kv, "c", variants, 10)).toStrictEqual(
      variants.map(() => 1),
    );
  });
});

describe("delivered", () => {
  it("counts the last hour and no further back", () => {
    const kv = memory();
    charge(kv, "c", [A], 100, [A]);
    charge(kv, "c", [A], 130, [A]);
    expect(delivered(kv, "c", [A], 159)).toStrictEqual([2]);
    expect(delivered(kv, "c", [A], 160)).toStrictEqual([1]);
    expect(delivered(kv, "c", [A], 190)).toStrictEqual([0]);
  });

  it("drops the hour's old minutes when it stores new ones", () => {
    const kv = memory();
    charge(kv, "c", [A], 100, [A]);
    charge(kv, "c", [B], 200, [B]);
    expect(kv.get("c:c")).toStrictEqual({
      [JSON.stringify([
        "github",
        "push",
        "o/b",
        undefined,
        undefined,
        undefined,
        undefined,
      ])]: { "200": 1 },
    });
  });

  it("answers zero for a connection with no id yet or no counts", () => {
    const kv = memory();
    expect(delivered(kv, undefined, [A, B], 10)).toStrictEqual([0, 0]);
    expect(delivered(kv, "none", [A], 10)).toStrictEqual([0]);
  });
});

describe("forget", () => {
  it("drops one connection's counts", () => {
    const kv = memory();
    charge(kv, "c", [A], 10, [A]);
    charge(kv, "d", [A], 10, [A]);
    forget(kv, "c");
    expect([...kv.map.keys()]).toStrictEqual(["c:d"]);
  });

  it("does nothing for a connection with no id", () => {
    const kv = memory();
    charge(kv, "c", [A], 10, [A]);
    forget(kv, undefined);
    expect([...kv.map.keys()]).toStrictEqual(["c:c"]);
  });

  it("drops the counts of connections that are not live", () => {
    const kv = memory();
    charge(kv, "live", [A], 10, [A]);
    charge(kv, "gone", [A], 10, [A]);
    kv.put("other", 1);
    forgetOthers(kv, new Set(["live"]));
    expect([...kv.map.keys()].sort()).toStrictEqual(["c:live", "other"]);
  });
});

describe("the hour boundary", () => {
  it("drops a minute exactly an hour old and keeps the one after it", () => {
    const kv = memory();
    charge(kv, "c", [A], 100, [A]);
    charge(kv, "c", [A], 101, [A]);
    expect(delivered(kv, "c", [A], 160)).toStrictEqual([1]);
    expect(delivered(kv, "c", [A], 161)).toStrictEqual([0]);
  });

  it("drops the old minute from the stored counters on the next charge", () => {
    const kv = memory();
    charge(kv, "c", [A], 100, [A]);
    charge(kv, "c", [A], 160, [A]);
    expect(kv.map.get("c:c")).toStrictEqual({
      [JSON.stringify(["github", "push", "o/a", null, null, null, null])]: {
        "160": 1,
      },
    });
  });

  it("reports zero and forgets nothing for a connection without an id", () => {
    const kv = memory();
    expect(delivered(kv, undefined, [A], 1)).toStrictEqual([0]);
    expect(() => {
      forget(kv, undefined);
    }).not.toThrow();
  });
});

describe("old minutes", () => {
  it("leaves no entry for a subscription whose minutes all aged out", () => {
    const kv = memory();
    charge(kv, "c", [A], 100, [A, B]);
    charge(kv, "c", [B], 200, [A, B]);
    expect(Object.keys(kv.map.get("c:c") as object)).toHaveLength(1);
  });
});

describe("refund", () => {
  it("takes back one delivery of this minute", () => {
    const kv = memory();
    charge(kv, "c", [A, B], 10, [A, B]);
    charge(kv, "c", [A], 10, [A, B]);
    refund(kv, "c", [A], 10);
    expect(delivered(kv, "c", [A, B], 10)).toStrictEqual([1, 1]);
  });

  it("leaves a subscription that was never counted alone", () => {
    const kv = memory();
    charge(kv, "c", [A], 10, [A, B]);
    refund(kv, "c", [B], 10);
    expect(delivered(kv, "c", [A, B], 10)).toStrictEqual([1, 0]);
  });
});

describe("keeping the record to live subscriptions", () => {
  it("charge drops the counts of subscriptions that left the list", () => {
    const kv = memory();
    charge(kv, "c", [A], 10, [A]);
    charge(kv, "c", [B], 10, [B]);
    expect(delivered(kv, "c", [A, B], 10)).toStrictEqual([0, 1]);
  });

  it("retain drops them without a charge", () => {
    const kv = memory();
    charge(kv, "c", [A, B], 10, [A, B]);
    retain(kv, "c", [B], 10);
    expect(delivered(kv, "c", [A, B], 10)).toStrictEqual([0, 1]);
  });

  it("retain with an empty list leaves an empty record", () => {
    const kv = memory();
    charge(kv, "c", [A], 10, [A]);
    retain(kv, "c", [], 10);
    expect(kv.map.get("c:c")).toStrictEqual({});
  });
});

describe("the notice queue", () => {
  it("returns what was queued once, then nothing", () => {
    const kv = memory();
    enqueue(kv, "c", ["a"]);
    enqueue(kv, "c", ["b"]);
    expect(dequeue(kv, "c")).toStrictEqual(["a", "b"]);
    expect(dequeue(kv, "c")).toStrictEqual([]);
  });

  it("keeps the first notices when more than the limit arrive", () => {
    const kv = memory();
    const frames = Array.from({ length: MAX_PENDING + 2 }, (_, i) => String(i));
    enqueue(kv, "c", frames);
    expect(dequeue(kv, "c")).toStrictEqual(frames.slice(0, MAX_PENDING));
  });

  it("forget drops the queue with the counts", () => {
    const kv = memory();
    charge(kv, "c", [A], 10, [A]);
    enqueue(kv, "c", ["a"]);
    forget(kv, "c");
    expect([...kv.map.keys()]).toStrictEqual([]);
  });

  it("forgetOthers drops the queues of connections that are gone", () => {
    const kv = memory();
    enqueue(kv, "live", ["a"]);
    enqueue(kv, "gone", ["b"]);
    forgetOthers(kv, new Set(["live"]));
    expect([...kv.map.keys()]).toStrictEqual(["n:live"]);
  });
});
