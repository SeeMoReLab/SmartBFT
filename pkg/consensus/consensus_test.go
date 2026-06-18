// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package consensus

import (
	"testing"
	"time"
)

func TestSplitRequestTimeout(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		timeout       time.Duration
		wantForward   time.Duration
		wantComplain  time.Duration
		wantTotalTime time.Duration
	}{
		{
			name:          "even",
			timeout:       400 * time.Millisecond,
			wantForward:   200 * time.Millisecond,
			wantComplain:  200 * time.Millisecond,
			wantTotalTime: 400 * time.Millisecond,
		},
		{
			name:          "odd",
			timeout:       401 * time.Millisecond,
			wantForward:   200*time.Millisecond + 500*time.Microsecond,
			wantComplain:  200*time.Millisecond + 500*time.Microsecond,
			wantTotalTime: 401 * time.Millisecond,
		},
		{
			name:          "tiny",
			timeout:       time.Nanosecond,
			wantForward:   time.Nanosecond,
			wantComplain:  time.Nanosecond,
			wantTotalTime: 2 * time.Nanosecond,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			forward, complain := splitRequestTimeout(testCase.timeout)
			if forward != testCase.wantForward {
				t.Fatalf("forward = %s, want %s", forward, testCase.wantForward)
			}
			if complain != testCase.wantComplain {
				t.Fatalf("complain = %s, want %s", complain, testCase.wantComplain)
			}
			if forward+complain != testCase.wantTotalTime {
				t.Fatalf("total = %s, want %s", forward+complain, testCase.wantTotalTime)
			}
		})
	}
}
