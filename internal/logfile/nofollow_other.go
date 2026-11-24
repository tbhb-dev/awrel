// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

//go:build !unix

package logfile

// noFollow is zero here, since the platform has no O_NOFOLLOW.
const noFollow = 0
