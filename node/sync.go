// Copyright (C) 2024-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-only

package node

// Constants for pod status messages when a pod disappears from the provider.
const (
	podStatusReasonNotFound          = "NotFound"
	podStatusMessageNotFound         = "The pod status was not found and may have been deleted"
	containerStatusReasonNotFound    = "NotFound"
	containerStatusMessageNotFound   = "Container was not found and was likely deleted"
	containerStatusExitCodeNotFound  = -137
	statusTerminatedReason           = "Terminated"
	containerStatusTerminatedMessage = "Container was terminated. The exit code may not reflect the real exit code"
)
