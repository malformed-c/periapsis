// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-only

package pawn

import (
	"log/slog"

	"github.com/malformed-c/periapsis/internal/config"
	"github.com/malformed-c/periapsis/internal/runtime"
	"github.com/malformed-c/periapsis/internal/server"
	"github.com/malformed-c/periapsis/node"
	corev1 "k8s.io/api/core/v1"
)

type Pawn struct {
	Config     *config.PawnConfig
	Runtime    runtime.Runtime
	Node       *corev1.Node
	Controller *node.NodeController
	Server     *server.PawnServer
	Logger     *slog.Logger
}
