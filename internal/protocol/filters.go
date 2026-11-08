// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package protocol

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
)

// AttachmentBudget is the relay's canonical attachment limit in UTF-8 bytes.
const AttachmentBudget = 12288

const (
	eventCheckRun    = "check_run"
	eventWorkflowRun = "workflow_run"
	eventWorkflowJob = "workflow_job"
	maxFilterValues  = 8
)

//nolint:gochecknoglobals // immutable validation tables
var (
	errFilter    = errors.New("bad subscription filter")
	filterToken  = regexp.MustCompile(`^[a-z0-9_.-]{1,64}$`)
	filterName   = regexp.MustCompile(`^[\x20-\x7e]{1,128}$`)
	filterPath   = regexp.MustCompile(`^[A-Za-z0-9_./-]{1,256}$`)
	actionEvents = []string{
		"branch_protection_configuration",
		"branch_protection_rule",
		eventCheckRun,
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
		eventWorkflowJob,
		eventWorkflowRun,
	}
)

// ValidateFilters checks filters before normalization, so duplicates count
// against the input list limit. Unfiltered legacy subscriptions remain valid.
func (s Subscription) ValidateFilters() error {
	if !s.HasFilters() {
		return nil
	}

	if s.Provider != "github" || s.Event == "" || s.Repository == "" {
		return fmt.Errorf("%w: filters require github, event and repository", errFilter)
	}

	for _, list := range []struct {
		name   string
		values []string
		events []string
	}{
		{"action", s.Action, actionEvents},
		{"conclusion", s.Conclusion, []string{eventCheckRun, "check_suite", eventWorkflowRun, eventWorkflowJob}},
	} {
		err := validateList(list.name, list.values, list.events, s.Event)
		if err != nil {
			return err
		}
	}

	if s.Name != "" {
		if !slices.Contains([]string{eventCheckRun, eventWorkflowRun, eventWorkflowJob}, s.Event) ||
			!filterName.MatchString(s.Name) {
			return fmt.Errorf("%w: name needs a supported event and 1 to 128 printable ASCII characters", errFilter)
		}
	}

	if s.WorkflowPath != "" {
		if s.Event != eventWorkflowRun || !filterPath.MatchString(s.WorkflowPath) {
			return fmt.Errorf("%w: workflow_path needs workflow_run and 1 to 256 path characters", errFilter)
		}
	}

	return nil
}

func validateList(name string, values, events []string, event string) error {
	if values == nil {
		return nil
	}

	if !slices.Contains(events, event) {
		return fmt.Errorf("%w: %s is unsupported for %s", errFilter, name, event)
	}

	if len(values) < 1 || len(values) > maxFilterValues {
		return fmt.Errorf("%w: %s needs 1 to 8 values", errFilter, name)
	}

	for _, value := range values {
		if !filterToken.MatchString(value) {
			return fmt.Errorf("%w: invalid %s value", errFilter, name)
		}
	}

	return nil
}
