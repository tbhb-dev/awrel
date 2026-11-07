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

Most fixtures are recordings from spike awrel/S-000001, taken from a 24-hour stg capture. Spike scrubbing turned logins and people into pseudonyms, `user-N` for people and `app-bot-N` for apps, and gave one person one pseudonym across all recordings. Public values stay real: the repository `tbhb-dev/awrel` and its commit and tree SHAs. IDs, node IDs and `external_id` are fake but consistent. Each recording is the one nearest the median size for its event and action.

Synthetic placeholders cover `issue_comment`, `ping`, and `pull_request_review_comment`, which the capture lacks. They're hand-built shapes with fake logins (`user-1`, `user-2`), fake IDs, and `example-org/example-repo`. `push/example-provider` is synthetic as well, a push of another provider. In the synthetic fixtures `user-1` and `user-2` are two different people, which has nothing to do with the pseudonyms of the recordings.

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
