// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// Package compact reduces a webhook payload to the identifiers and state that
// a session needs to notice that something happened. The agent fetches the
// details from the provider's API or command-line tool.
//
// Payload is pure and deterministic. The output keeps the key order of the
// input and has no whitespace.
//
// # Generic rules
//
// Every event gets these, whatever the provider:
//
//   - Drop the API link keys. These are the keys that end in `_url`, apart
//     from `html_url`, and the bare key spelled as the lower case of URL. The
//     key `compare` stays, and no rule drops it.
//   - Drop `node_id`, `gravatar_id`, `_links`, `performed_via_github_app`,
//     `reactions` and `author_association`.
//   - Reduce a user object to `login` and `type`. The keys `sender`, `user`,
//     `pusher`, `owner`, `author` and `committer` hold user objects, and so do
//     the items of `requested_reviewers`. An object under one of these keys
//     with no `login` is a Git identity, such as the pusher and the author in
//     a push, and a keep-list handles it.
//   - Reduce `repository` to `full_name`, `default_branch` and `private`.
//     Reduce `organization`, `installation` and `enterprise` to `id`.
//   - Cut a string over 2 KiB to its first 2 KiB, ending on a character
//     boundary. Append `…[N bytes cut]`, where N counts the bytes removed.
//
// A reduction keeps the listed members that the object has and adds none.
//
// # Keep-lists
//
// A GitHub event with a keep-list gets the generic rules first. Then only the
// listed paths stay, together with `action` and `sender.login`. A path through
// an array applies to each item and leaves the array in place. A path that the
// payload lacks is skipped. The events with a list are `push`, `pull_request`,
// `pull_request_review`, `pull_request_review_comment`, `issue_comment`,
// `issues`, `check_suite`, `check_run`, `workflow_run`, `workflow_job`,
// `create` and `delete`, and keep.go holds the lists. No list names
// `repository`, because the delivery attribute `repository` already does.
//
// A push and an `issue_comment` also change values. A push keeps at most 20
// commits, each as its `id` and the first line of its `message`, and adds
// `commits_total` after `commits` with the count of commits the payload had.
// An `issue_comment` reduces `issue.pull_request` to `true` when it has a
// value.
//
// An event without a keep-list, and any event of another provider, gets the
// generic rules only.
package compact

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	// maxString is the longest string kept whole.
	maxString = 2048
	// maxCommits is the most commits of a push that stay in the payload.
	maxCommits = 20

	githubProvider = "github"
)

// ErrInvalidPayload means the payload isn't valid JSON.
var ErrInvalidPayload = errors.New("payload is not valid JSON")

// The tables below are read-only.
//
//nolint:gochecknoglobals // lookup tables that nothing writes
var (
	userKeys = map[string]bool{
		"sender": true, "user": true, "pusher": true, "owner": true, "author": true,
		"committer": true, "requested_reviewers": true,
	}
	userFields = []string{"login", "type"}

	// reductions lists the fields that an object under a key is reduced to.
	// The user keys are absent: they apply only to an object with a `login`.
	reductions = map[string][]string{
		"repository":   {"full_name", "default_branch", "private"},
		"organization": {"id"},
		"installation": {"id"},
		"enterprise":   {"id"},
	}

	// droppedKeys are left out wherever they appear. The function dropped
	// also checks for the link keys.
	droppedKeys = map[string]bool{
		"node_id": true, "gravatar_id": true, "_links": true, "performed_via_github_app": true,
		"reactions": true, "author_association": true,
	}
)

// Payload returns the compact form of payload, the JSON body of the event of
// the given provider and name. It returns ErrInvalidPayload when payload isn't
// valid JSON, and the caller should deliver the whole payload then.
func Payload(provider, event string, payload json.RawMessage) (json.RawMessage, error) {
	if !json.Valid(payload) {
		return nil, ErrInvalidPayload
	}

	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()

	root := simplify((&parser{dec: dec, limit: len(payload)}).parse(), "")

	if provider == githubProvider {
		list, ok := keepLists[event]
		if ok {
			root = list.apply(root)
		}
	}

	return render(root), nil
}

// simplify applies the generic rules to `value`, the value of the member `key`.
// The items of an array take the key of the array.
//
//nolint:exhaustive // a null and a literal stay as they are
func simplify(value node, key string) node {
	switch value.kind {
	case kindObject:
		return simplifyObject(value, key)
	case kindArray:
		items := make([]node, len(value.items))
		for i, item := range value.items {
			items[i] = simplify(item, key)
		}

		return node{kind: kindArray, text: "", members: nil, items: items}
	case kindString:
		return stringNode(cut(value.text))
	default:
		return value
	}
}

func simplifyObject(value node, key string) node {
	switch {
	case userKeys[key] && value.has("login"):
		value = value.pick(userFields...)
	case reductions[key] != nil:
		value = value.pick(reductions[key]...)
	}

	return value.rewrite(func(entry member) []member {
		if dropped(entry.key) {
			return nil
		}

		return []member{{key: entry.key, val: simplify(entry.val, entry.key)}}
	})
}

// dropped reports whether the generic rules leave out a member named `key`.
func dropped(key string) bool {
	if droppedKeys[key] {
		return true
	}

	return key == "url" || (strings.HasSuffix(key, "_url") && key != "html_url")
}

// cut shortens `text` to `maxString` bytes at most, ending on a character
// boundary. The suffix tells how many bytes it removed.
func cut(text string) string {
	if len(text) <= maxString {
		return text
	}

	// A character takes four bytes at most, so the start of the character that
	// spans the limit is no more than three bytes back. The loop runs the
	// length of the longest character.
	end := maxString

	for range utf8.UTFMax {
		if !utf8.RuneStart(text[end]) {
			end--
		}
	}

	return text[:end] + "…[" + strconv.Itoa(len(text)-end) + " bytes cut]"
}
