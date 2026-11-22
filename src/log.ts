// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

/**
 * Writes one log line. A logging failure must never reject the webhook
 * handler or the Durable Object call, so it is swallowed.
 */
export function log(line: string): void {
  try {
    console.log(line);
  } catch {
    // Dropping the line is better than failing the request it describes.
  }
}
