// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/malformed-c/periapsis/internal/control"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// rolloutArgs holds parsed flags for the rollout subcommand.
type rolloutArgs struct {
	deployment string
	namespace  string
	replicas   int
	step       int
	timeout    time.Duration
	kubeconfig string
	dryRun     bool
}

func cmdRollout(ctx context.Context, client *control.Client, args []string) error {
	ra, remaining, err := parseRolloutArgs(args)
	if err != nil {
		return err
	}
	if len(remaining) > 0 {
		return fmt.Errorf("unexpected arguments: %v", remaining)
	}

	if ra.deployment == "" {
		return fmt.Errorf("--deployment is required")
	}
	if ra.replicas < 0 {
		return fmt.Errorf("--replicas must be >= 0")
	}
	if ra.step <= 0 {
		return fmt.Errorf("--step must be > 0")
	}

	// Build k8s client.
	loadRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if ra.kubeconfig != "" {
		loadRules.ExplicitPath = ra.kubeconfig
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadRules,
		&clientcmd.ConfigOverrides{},
	).ClientConfig()
	if err != nil {
		return fmt.Errorf("kubeconfig: %w", err)
	}
	kc, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("k8s client: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nInterrupted - deployment left at current replica count.")
		cancel()
	}()

	return runRollout(ctx, kc, client, ra)
}

func runRollout(ctx context.Context, kc *kubernetes.Clientset, pClient *control.Client, ra rolloutArgs) error {
	deployClient := kc.AppsV1().Deployments(ra.namespace)

	dep, err := deployClient.Get(ctx, ra.deployment, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get deployment %s/%s: %w", ra.namespace, ra.deployment, err)
	}

	current := 0
	if dep.Spec.Replicas != nil {
		current = int(*dep.Spec.Replicas)
	}
	target := ra.replicas

	fmt.Printf("Deployment:  %s/%s\n", ra.namespace, ra.deployment)
	fmt.Printf("Current:     %d replica(s)\n", current)
	fmt.Printf("Target:      %d replica(s)\n", target)
	fmt.Printf("Step size:   %d\n", ra.step)
	fmt.Printf("Timeout:     %s per step\n", ra.timeout)
	fmt.Println()

	if current == target {
		fmt.Println("Already at target replica count. Nothing to do.")
		return nil
	}

	if ra.dryRun {
		steps := stepsFor(current, target, ra.step)
		fmt.Println("Dry run - steps that would be applied:")
		for i, s := range steps {
			fmt.Printf("  Step %d: %d -> %d replica(s)\n", i+1, s.from, s.to)
		}
		return nil
	}

	steps := stepsFor(current, target, ra.step)
	for i, s := range steps {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fmt.Printf("-- Step %d/%d: scaling %d -> %d --\n", i+1, len(steps), s.from, s.to)

		// Re-fetch to avoid update conflicts.
		dep, err = deployClient.Get(ctx, ra.deployment, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("refresh deployment: %w", err)
		}
		n := int32(s.to)
		dep.Spec.Replicas = &n
		if _, err = deployClient.Update(ctx, dep, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update deployment: %w", err)
		}
		fmt.Printf("  ✓ Set replicas to %d\n", s.to)

		if err := waitForReady(ctx, kc, pClient, ra.namespace, ra.deployment, s.to, ra.timeout); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("step %d wait: %w", i+1, err)
		}
	}

	fmt.Printf("\n✓ Rollout complete: %s/%s is at %d replica(s).\n", ra.namespace, ra.deployment, target)
	return nil
}

// waitForReady polls deployment status until ReadyReplicas reaches desired,
// using the deployment's own conditions as the source of truth.
func waitForReady(ctx context.Context, kc *kubernetes.Clientset, pClient *control.Client, namespace, name string, desired int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		dep, err := kc.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get deployment: %w", err)
		}

		ready := int(dep.Status.ReadyReplicas)
		updated := int(dep.Status.UpdatedReplicas)
		available := int(dep.Status.AvailableReplicas)

		// Cross-reference with the perigeos socket (best-effort).
		runningLocal := 0
		prefix := name + "-"
		if pr, perr := pClient.Pods(ctx); perr == nil {
			for _, p := range pr.Pods {
				if p.Namespace == namespace && strings.HasPrefix(p.Name, prefix) && p.Phase == "Running" {
					runningLocal++
				}
			}
		}

		fmt.Printf("  ready=%d updated=%d available=%d perigeos=%d / %d desired\n",
			ready, updated, available, runningLocal, desired)

		if ready >= desired && available >= desired {
			fmt.Printf("  ✓ %d/%d replicas ready\n", ready, desired)
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for %d ready replicas (got %d ready)", desired, ready)
		}
	}
}

// step represents a single replica transition.
type step struct {
	from, to int
}

// stepsFor builds the list of transitions from current to target, each at most
// stepSize replicas at a time. Works for both scale-up and scale-down.
func stepsFor(current, target, stepSize int) []step {
	var steps []step
	cur := current
	if target > current {
		for cur < target {
			next := cur + stepSize
			if next > target {
				next = target
			}
			steps = append(steps, step{cur, next})
			cur = next
		}
	} else {
		for cur > target {
			next := cur - stepSize
			if next < target {
				next = target
			}
			steps = append(steps, step{cur, next})
			cur = next
		}
	}
	return steps
}

// parseRolloutArgs parses the flag list for the rollout subcommand.
func parseRolloutArgs(args []string) (rolloutArgs, []string, error) {
	ra := rolloutArgs{
		namespace: "default",
		step:      1,
		timeout:   2 * time.Minute,
		replicas:  -1,
	}
	var remaining []string

	for i := 0; i < len(args); i++ {
		arg := args[i]

		var key, val string
		hasEq := false
		if eqIdx := strings.IndexByte(arg, '='); eqIdx >= 0 {
			key = arg[:eqIdx]
			val = arg[eqIdx+1:]
			hasEq = true
		} else {
			key = arg
		}

		isBool := key == "--dry-run" || key == "-dry-run" || key == "--dry" || key == "-dry"

		if !hasEq && !isBool {
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				val = args[i+1]
				i++
			}
		}

		var err error
		switch key {
		case "--deployment", "-deployment":
			ra.deployment = val
		case "--namespace", "-namespace", "-n":
			ra.namespace = val
		case "--replicas", "-replicas":
			_, err = fmt.Sscanf(val, "%d", &ra.replicas)
		case "--step", "-step":
			_, err = fmt.Sscanf(val, "%d", &ra.step)
		case "--timeout", "-timeout":
			ra.timeout, err = time.ParseDuration(val)
		case "--kubeconfig", "-kubeconfig":
			ra.kubeconfig = val
		case "--dry-run", "-dry-run", "--dry", "-dry":
			ra.dryRun = true
		default:
			if !strings.HasPrefix(arg, "-") {
				remaining = append(remaining, arg)
			} else {
				return ra, nil, fmt.Errorf("unknown flag: %s", key)
			}
		}
		if err != nil {
			return ra, nil, fmt.Errorf("flag %s: %w", key, err)
		}
	}

	if ra.replicas < 0 {
		return ra, remaining, fmt.Errorf("--replicas is required")
	}

	return ra, remaining, nil
}

func rolloutUsage() {
	fmt.Fprintf(os.Stderr, `Usage: apsis rollout --deployment=<n> --replicas=<n> [flags]

Stepped scale/rollout for a Kubernetes Deployment. Scales toward the target
replica count one step at a time, waiting for the deployment to report all
replicas ready before moving to the next step.

Flags:
  --deployment   Deployment name (required)
  --replicas     Target replica count (required)
  --namespace    Kubernetes namespace (default: default)
  --step         Replicas to add/remove per step (default: 1)
  --timeout      Per-step readiness timeout, e.g. 2m, 5m (default: 2m)
  --kubeconfig   Path to kubeconfig (default: KUBECONFIG env / ~/.kube/config)
  --dry-run      Print steps without applying them

Examples:
  # Scale my-app to 10 replicas, 2 at a time
  apsis rollout --deployment=my-app --replicas=10 --step=2

  # Run in the background (plain bash)
  apsis rollout --deployment=my-app --replicas=10 --step=2 &

  # Scale down to 0
  apsis rollout --deployment=my-app --replicas=0

  # Preview without applying
  apsis rollout --deployment=my-app --replicas=8 --step=2 --dry-run
`)
}
