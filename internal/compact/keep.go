// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package compact

import (
	"strconv"
	"strings"
)

// keepList is the tree of paths that stay in the payload of one event. A list
// without paths stands for a whole value.
type keepList struct {
	// prepare changes the payload before the paths apply, for the lists that
	// do more than pick paths. Most lists leave it nil.
	prepare func(node) node
	paths   map[string]*keepList
}

// newKeepList builds a list from paths in dotted form. A path through an array
// has no mark for it: the array stays and each item is picked. A path ends at
// a value that the list keeps unchanged.
func newKeepList(prepare func(node) node, paths ...string) *keepList {
	root := &keepList{prepare: prepare, paths: map[string]*keepList{}}

	for _, path := range append([]string{"action", "sender.login"}, paths...) {
		cursor := root

		for key := range strings.SplitSeq(path, ".") {
			child, ok := cursor.paths[key]
			if !ok {
				child = &keepList{prepare: nil, paths: map[string]*keepList{}}
				cursor.paths[key] = child
			}

			cursor = child
		}
	}

	return root
}

// apply keeps what the list names of n.
func (l *keepList) apply(value node) node {
	if l.prepare != nil {
		value = l.prepare(value)
	}

	return l.pick(value)
}

// pick keeps the paths of the list in `value`. A scalar comes back as given, since
// nothing in it can match. The end of a path has no paths below it and leaves
// the value unchanged.
//
//nolint:exhaustive // a string, a null and a literal stay as they are
func (l *keepList) pick(value node) node {
	if len(l.paths) == 0 {
		return value
	}

	switch value.kind {
	case kindObject:
		return value.rewrite(func(entry member) []member {
			child, ok := l.paths[entry.key]
			if !ok {
				return nil
			}

			return []member{{key: entry.key, val: child.pick(entry.val)}}
		})
	case kindArray:
		items := make([]node, len(value.items))
		for i, item := range value.items {
			items[i] = l.pick(item)
		}

		return node{kind: kindArray, text: "", members: nil, items: items}
	default:
		return value
	}
}

// workflowFields lists the paths of the `workflow_run` and `workflow_job`
// events, under their own key.
func workflowFields(key string) []string {
	fields := []string{
		"id",
		"name",
		"status",
		"conclusion",
		"head_branch",
		"head_sha",
		"event",
		"run_attempt",
		"html_url",
		"pull_requests.number",
	}

	paths := make([]string, len(fields))
	for i, field := range fields {
		paths[i] = key + "." + field
	}

	return paths
}

// `keepLists` holds the lists of the GitHub events that have one, by the value
// of the X-GitHub-Event header.
//
//nolint:gochecknoglobals // a lookup table that nothing writes
var keepLists = map[string]*keepList{
	"push": newKeepList(
		prepareCommits,
		"ref", "before", "after", "created", "deleted", "forced", "base_ref", "compare", "pusher.name",
		"head_commit.id", "head_commit.message", "head_commit.timestamp", "head_commit.author.username",
		"commits", "commits_total",
	),
	"pull_request": newKeepList(
		nil,
		"number",
		"pull_request.number", "pull_request.title", "pull_request.state", "pull_request.draft",
		"pull_request.merged", "pull_request.merged_at", "pull_request.merge_commit_sha",
		"pull_request.html_url", "pull_request.user.login",
		"pull_request.head.ref", "pull_request.head.sha", "pull_request.base.ref", "pull_request.base.sha",
		"pull_request.labels.name", "pull_request.requested_reviewers.login", "pull_request.body",
		"label.name", "requested_reviewer.login", "changes",
	),
	"pull_request_review": newKeepList(
		nil,
		"review.id", "review.state", "review.body", "review.commit_id", "review.html_url",
		"review.submitted_at", "review.user.login",
		"pull_request.number", "pull_request.title", "pull_request.html_url", "pull_request.head.sha",
	),
	"pull_request_review_comment": newKeepList(
		nil,
		"comment.id", "comment.body", "comment.path", "comment.line", "comment.commit_id",
		"comment.in_reply_to_id", "comment.html_url", "comment.user.login",
		"pull_request.number", "pull_request.title",
	),
	"issue_comment": newKeepList(
		prepareIssuePullRequest,
		"comment.id", "comment.body", "comment.html_url", "comment.user.login",
		"issue.number", "issue.title", "issue.state", "issue.html_url", "issue.pull_request",
	),
	"issues": newKeepList(
		nil,
		"issue.number", "issue.title", "issue.state", "issue.html_url", "issue.body", "issue.user.login",
		"issue.labels.name", "label.name",
	),
	"check_suite": newKeepList(
		nil,
		"check_suite.id", "check_suite.status", "check_suite.conclusion", "check_suite.head_branch",
		"check_suite.head_sha", "check_suite.app.slug", "check_suite.pull_requests.number",
	),
	"check_run": newKeepList(
		nil,
		"check_run.id", "check_run.name", "check_run.status", "check_run.conclusion", "check_run.head_sha",
		"check_run.html_url", "check_run.app.slug", "check_run.check_suite.id", "check_run.pull_requests.number",
	),
	"workflow_run": newKeepList(nil, append(workflowFields("workflow_run"), "workflow_run.path")...),
	"workflow_job": newKeepList(nil, workflowFields("workflow_job")...),
	"create":       newKeepList(nil, "ref", "ref_type"),
	"delete":       newKeepList(nil, "ref", "ref_type"),
}

// prepareCommits limits the commits of a push to `maxCommits`. It reduces each
// to its `id` and the first line of its `message`, and adds `commits_total`
// after them. A `commits_total` of the input goes from any position.
func prepareCommits(root node) node {
	return root.rewrite(func(entry member) []member {
		if entry.key == "commits_total" {
			return nil
		}

		if entry.key != "commits" || entry.val.kind != kindArray {
			return []member{entry}
		}

		shown := entry.val.items[:min(len(entry.val.items), maxCommits)]
		items := make([]node, len(shown))

		for i, commit := range shown {
			items[i] = commit.rewrite(firstLineOfMessage).pick("id", "message")
		}

		return []member{
			{key: entry.key, val: node{kind: kindArray, text: "", members: nil, items: items}},
			{key: "commits_total", val: literal(strconv.Itoa(len(entry.val.items)))},
		}
	})
}

// firstLineOfMessage cuts the `message` of a commit down to its first line.
func firstLineOfMessage(entry member) []member {
	if entry.key == "message" && entry.val.kind == kindString {
		line, _, _ := strings.Cut(entry.val.text, "\n")
		entry.val = stringNode(strings.TrimSuffix(line, "\r"))
	}

	return []member{entry}
}

// prepareIssuePullRequest reduces `issue.pull_request`, an object of links to
// the pull request, to `true`. The member's presence says that the issue is a
// pull request.
func prepareIssuePullRequest(root node) node {
	return root.rewrite(func(entry member) []member {
		if entry.key == "issue" {
			entry.val = entry.val.rewrite(flagPullRequest)
		}

		return []member{entry}
	})
}

func flagPullRequest(entry member) []member {
	if entry.key == "pull_request" && entry.val.kind != kindNull {
		entry.val = literal("true")
	}

	return []member{entry}
}
