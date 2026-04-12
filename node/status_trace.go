package node

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

func summarizeContainerStatuses(statuses []corev1.ContainerStatus) string {
	if len(statuses) == 0 {
		return "none"
	}

	parts := make([]string, 0, len(statuses))
	for _, cs := range statuses {
		state := "unknown"
		switch {
		case cs.State.Running != nil:
			state = "running"
		case cs.State.Waiting != nil:
			if cs.State.Waiting.Reason != "" {
				state = "waiting:" + cs.State.Waiting.Reason
			} else {
				state = "waiting"
			}
		case cs.State.Terminated != nil:
			if cs.State.Terminated.Reason != "" {
				state = fmt.Sprintf("terminated:%s(%d)", cs.State.Terminated.Reason, cs.State.Terminated.ExitCode)
			} else {
				state = fmt.Sprintf("terminated(%d)", cs.State.Terminated.ExitCode)
			}
		}

		parts = append(parts, fmt.Sprintf("%s=%s ready=%t restart=%d", cs.Name, state, cs.Ready, cs.RestartCount))
	}

	return strings.Join(parts, ", ")
}
