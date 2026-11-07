---
description: What the fixtures of the compact package are, how the golden test reads them, and how to regenerate the expected output.
generated:
  - role: generated
    harness: claude-code
    model: anthropic:claude-sonnet-5-5
    at: "2026-11-06T04:32:19Z"
status: draft
tags: [testing, fixtures, compact]
title: Compact payload fixtures
type: Readme
---

# Compact payload fixtures

Most fixtures are recordings from spike awrel/S-000001, taken from a 24-hour stg capture. The spike scrubbed them and put pseudonyms in place of logins, IDs, and repositories. Each recording is the one nearest the median size for its event and action.

The capture has nothing for `issue_comment`, `ping`, or `pull_request_review_comment`. Those events keep their synthetic placeholders, hand-built shapes with fake logins (`user-1`), fake IDs, `example-org/example-repo`, and the example app `example-app` (ID 1001). Fixtures of other providers are synthetic too.

## Layout

The golden test finds every fixture under this directory. Replacement fixtures work without code changes.

- `<event>/<name>.json` is the payload of a GitHub event, where `<event>` is the X-GitHub-Event value.
- `<event>/<provider>/<name>.json` is the payload of an event of another provider.
- `<name>.golden.json` next to a fixture is the expected output of `compact.Payload`.

A fixture without a golden file fails the test. The test compares the JSON of the golden file and the output with whitespace removed, so the formatting of a golden file doesn't matter.

## Regenerating the golden files

Run the test with `-update` from the repository root, then read the diff of the golden files before committing it:

```console
go test ./internal/compact -run TestGolden -update
```

The size test logs the input and output size of each fixture. It asserts a reduction for every GitHub fixture.
