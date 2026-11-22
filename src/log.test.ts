// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { expect, it, vi } from "vitest";
import { log } from "./log.ts";

it("writes the line to the console", () => {
  const spy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  try {
    log("a line");
    expect(spy).toHaveBeenCalledWith("a line");
  } finally {
    spy.mockRestore();
  }
});

it("swallows a console that throws", () => {
  const spy = vi.spyOn(console, "log").mockImplementation(() => {
    throw new Error("console failed");
  });
  try {
    expect(() => {
      log("a line");
    }).not.toThrow();
  } finally {
    spy.mockRestore();
  }
});
