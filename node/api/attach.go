// Copyright © 2017 The virtual-kubelet authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/malformed-c/periapsis/errdefs"
	"k8s.io/apimachinery/pkg/types"
	remoteconstants "k8s.io/apimachinery/pkg/util/remotecommand"
	remoteutils "k8s.io/client-go/tools/remotecommand"
	"k8s.io/kubelet/pkg/cri/streaming/remotecommand"
)

// ContainerAttachHandlerFunc defines the handler function used for "execing" into a
// container in a pod.
type ContainerAttachHandlerFunc func(ctx context.Context, namespace, podName, containerName string, attach AttachIO) error

// HandleContainerAttach makes an http handler func from a Provider which execs a command in a pod's container
// Note that this handler currently depends on gorrilla/mux to get url parts as variables.
// TODO(@cpuguy83): don't force gorilla/mux on consumers of this function
func HandleContainerAttach(h ContainerAttachHandlerFunc, opts ...ContainerExecHandlerOption) http.HandlerFunc {
	if h == nil {
		return NotImplemented
	}

	var cfg ContainerExecHandlerConfig
	for _, o := range opts {
		o(&cfg)
	}

	if cfg.StreamIdleTimeout == 0 {
		cfg.StreamIdleTimeout = 30 * time.Second
	}
	if cfg.StreamCreationTimeout == 0 {
		cfg.StreamCreationTimeout = 30 * time.Second
	}

	return handleError(func(w http.ResponseWriter, req *http.Request) error {
		vars := mux.Vars(req)

		namespace := vars["namespace"]
		pod := vars["pod"]
		container := vars["container"]

		clientSupportedStreamProtocols := strings.Split(req.Header.Get("X-Stream-Protocol-Version"), ",")

		streamOpts, err := getExecOptions(req)
		if err != nil {
			return errdefs.AsInvalidInput(err)
		}

		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()

		slog.Debug("exec: stream negotiation",
			"pod", pod, "container", container,
			"protocols", clientSupportedStreamProtocols,
			"stdin", streamOpts.Stdin,
			"stdout", streamOpts.Stdout,
			"stderr", streamOpts.Stderr,
			"tty", streamOpts.TTY,
			"upgrade", req.Header.Get("Upgrade"),
			"connection", req.Header.Get("Connection"),
			"proto", req.Proto,
		)

		attach := &containerAttachContext{
			ctx:       ctx,
			h:         h,
			pod:       pod,
			namespace: namespace,
			container: container,
		}

		remotecommand.ServeAttach(
			w,
			req,
			attach,
			"",
			"",
			container,
			streamOpts,
			cfg.StreamIdleTimeout,
			cfg.StreamCreationTimeout,
			remoteconstants.SupportedStreamingProtocols,
		)

		return nil
	})
}

type containerAttachContext struct {
	h                         ContainerAttachHandlerFunc
	namespace, pod, container string
	ctx                       context.Context
}

// AttachContainer Implements remotecommand.Attacher
// This is called by remotecommand.ServeAttach
func (c *containerAttachContext) AttachContainer(
	ctx context.Context,
	name string,
	uid types.UID,
	container string,
	in io.Reader,
	out io.WriteCloser,
	err io.WriteCloser,
	tty bool,
	resize <-chan remoteutils.TerminalSize,
) error {

	// Map the provided streams to internal AttachIO implementation
	eio := &execIO{
		tty:    tty,
		stdin:  in,
		stdout: out,
		stderr: err,
	}

	if tty {
		eio.chResize = make(chan TermSize)

		// Handle terminal resizing in the background
		go func() {
			for {
				select {
				case s, ok := <-resize:
					if !ok {
						return
					}

					select {
					case eio.chResize <- TermSize{Width: s.Width, Height: s.Height}:

					case <-ctx.Done():
						return
					}

				case <-ctx.Done():
					return
				}
			}
		}()
	}

	return c.h(ctx, c.namespace, c.pod, c.container, eio)
}
