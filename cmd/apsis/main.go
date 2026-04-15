package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"text/tabwriter"
	"time"

	"github.com/malformed-c/periapsis/internal/control"
)

func shortUID(uid string) string {
	if len(uid) > 12 {
		return uid[:12]
	}
	return uid
}

func main() {
	socketPath := flag.String("socket", control.DefaultSocketPath, "Path to control socket")
	jsonOutput := flag.Bool("json", false, "Output raw JSON")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: apsis [flags] <command>\n  apsis <command> [flags]\n\nCommands:\n  status    Daemon status overview\n  pawns     List pawns with state and pod counts\n  pods      List pods across all pawns\n  showcase  Visual breakdown of cluster state\n  doctor    Compare pod state across all sources and report discrepancies\n  images    List cached OCI images and on-disk sizes\n  drain     Mark nodes NotReady and let scheduler evict pods (passive)\n  stop      Active drain (NotReady + stop all pods) then systemctl stop perigeos\n  top       Live per-pawn cgroup stats (refreshes every 2s)\n  rollout   Stepped scale/rollout for a Deployment\n  version   Version info\n\nFlags:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	// Support flags after the subcommand (e.g. "apsis doctor --json").
	// flag.Parse() stops at the first non-flag arg, so trailing flags
	// end up in flag.Args(). Scan remaining args for --json.
	cmd := ""
	for _, arg := range flag.Args() {
		if arg == "--json" || arg == "-json" {
			*jsonOutput = true
		} else if cmd == "" {
			cmd = arg
		}
	}

	if cmd == "" {
		flag.Usage()
		os.Exit(1)
	}

	client := control.NewClient(*socketPath)

	// "top" runs an interactive loop - use a cancellable context without timeout.
	// "rollout" also needs a long-lived context - no timeout.
	var ctx context.Context
	var cancel context.CancelFunc
	if cmd == "top" || cmd == "rollout" || cmd == "drain" || cmd == "stop" {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	}
	defer cancel()

	var err error
	switch cmd {
	case "status":
		err = cmdStatus(ctx, client, *jsonOutput)
	case "pawns":
		err = cmdPawns(ctx, client, *jsonOutput)
	case "pods":
		err = cmdPods(ctx, client, *jsonOutput)
	case "showcase":
		err = cmdShowcase(ctx, client, *jsonOutput)
	case "doctor":
		err = cmdDoctor(ctx, client, *jsonOutput)
	case "images":
		err = cmdImages(ctx, client, *jsonOutput)
	case "drain":
		err = cmdDrain(ctx, client)
	case "stop":
		err = cmdStop(ctx, client)
	case "version":
		err = cmdVersion(ctx, client, *jsonOutput)
	case "top":
		err = cmdTop(ctx, client, *jsonOutput)
	case "rollout":
		// rollout parses its own flags from the remaining args after the subcommand.
		// It does not use --json; pass raw tail args and the socket path for background re-exec.
		var tail []string
		for _, a := range flag.Args() {
			if a != "rollout" {
				tail = append(tail, a)
			}
		}
		if len(tail) == 0 {
			rolloutUsage()
			os.Exit(1)
		}
		err = cmdRollout(ctx, client, tail)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", flag.Arg(0))
		flag.Usage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func cmdStatus(ctx context.Context, c *control.Client, asJSON bool) error {
	s, err := c.Status(ctx)
	if err != nil {
		return err
	}
	if asJSON {
		return printJSON(s)
	}

	uptime := time.Duration(s.UptimeSecs) * time.Second
	fmt.Printf("Hostname:    %s\n", s.Hostname)
	fmt.Printf("Version:     %s\n", s.Version)
	fmt.Printf("Uptime:      %s\n", uptime)
	fmt.Printf("Pawns:       %d\n", s.PawnCount)
	fmt.Printf("Pods:        %d\n", s.PodCount)
	fmt.Printf("Kernel:      %s\n", s.Kernel)
	fmt.Printf("Arch:        %s/%s\n", s.OS, s.Arch)
	fmt.Printf("Go:          %s\n", s.GoVersion)
	fmt.Printf("Memory:      %d / %d MiB\n", s.MemUsedMiB, s.MemTotalMiB)
	fmt.Printf("CPU cores:   %d\n", s.CPUCores)
	fmt.Printf("Load avg:    %s\n", s.LoadAvg)
	fmt.Printf("PSI cpu:     %.1f%%\n", s.PSICPUSome)
	fmt.Printf("PSI memory:  %.1f%%\n", s.PSIMemFull)
	fmt.Println()
	fmt.Printf("Machines:    %d\n", s.Machines)
	fmt.Printf("Disk dirs:   %d\n", s.DiskDirs)
	fmt.Printf("Units:       %d\n", s.SystemdUnits)
	fmt.Printf("RSS:         %d MiB\n", s.PerigeosRSSMiB)
	fmt.Printf("LXC veths:   %d\n", s.LxcVeths)
	fmt.Printf("Netns:       %d\n", s.NetnsCount)
	return nil
}

func cmdPawns(ctx context.Context, c *control.Client, asJSON bool) error {
	r, err := c.Pawns(ctx)
	if err != nil {
		return err
	}
	if asJSON {
		return printJSON(r)
	}

	if len(r.Pawns) == 0 {
		fmt.Fprintln(os.Stderr, "No pawns found.")
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tROLE\tPORT\tIP\tPODS\tCPU(ms)\tMEM(MiB)")
	for _, p := range r.Pawns {
		role := "pawn"
		if p.IsPrimary {
			role = "primary"
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%d\t%d\t%d\n",
			p.Name, role, p.Port, p.NodeIP, p.PodCount, p.CPUUsageMs, p.MemoryMiB)
	}
	return w.Flush()
}

func cmdPods(ctx context.Context, c *control.Client, asJSON bool) error {
	r, err := c.Pods(ctx)
	if err != nil {
		return err
	}
	if asJSON {
		return printJSON(r)
	}

	if len(r.Pods) == 0 {
		fmt.Fprintln(os.Stderr, "No pods found.")
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "PAWN\tNAMESPACE\tNAME\tIP\tPHASE\tCONTAINERS")
	for _, p := range r.Pods {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\n",
			p.PawnName, p.Namespace, p.Name, p.PodIP, p.Phase, p.Containers)
	}
	return w.Flush()
}

func cmdDoctor(ctx context.Context, c *control.Client, asJSON bool) error {
	d, err := c.Doctor(ctx)
	if err != nil {
		return err
	}
	if asJSON {
		return printJSON(d)
	}

	// Header
	if d.Healthy {
		fmt.Println("Status:  HEALTHY")
	} else {
		fmt.Println("Status:  UNHEALTHY")
	}
	fmt.Printf("Sources: gambit=%d  systemd=%d  disk=%d  stale_units=%d\n",
		d.Summary.TotalGambit, d.Summary.TotalSystemd, d.Summary.TotalDisk, d.Summary.TotalStaleUnits)
	fmt.Printf("Slices:  active=%d  stale=%d\n",
		d.Summary.ActiveSlices, d.Summary.StaleSlices)
	fmt.Printf("Network: lxc_veths=%d  netns=%d\n\n",
		d.Summary.LxcVeths, d.Summary.NetnsCount)

	for _, p := range d.Pawns {
		sliceStatus := "active"
		if !p.SliceActive {
			sliceStatus = "INACTIVE"
		}
		fmt.Printf("-- %s --\n", p.Name)
		fmt.Printf("  gambit=%d  systemd=%d  disk=%d  stale_units=%d  slice=%s\n", p.GambitPods, p.SystemdUnits, p.DiskDirs, p.StaleUnits, sliceStatus)

		if len(p.GhostPods) > 0 {
			fmt.Printf("  ghost pods (gambit only, no systemd unit):\n")
			for _, e := range p.GhostPods {
				fmt.Printf("    %s  %s\n", shortUID(e.UID), e.Name)
			}
		}
		if len(p.OrphanMachines) > 0 {
			fmt.Printf("  orphan machines (systemd only, not in gambit):\n")
			for _, e := range p.OrphanMachines {
				fmt.Printf("    %s  %s\n", shortUID(e.UID), e.Name)
			}
		}
		if len(p.StaleDirs) > 0 {
			fmt.Printf("  stale dirs (disk only, not in gambit):\n")
			for _, uid := range p.StaleDirs {
				fmt.Printf("    %s\n", shortUID(uid))
			}
		}
		if len(p.MissingDirs) > 0 {
			fmt.Printf("  missing dirs (gambit only, no disk workspace):\n")
			for _, e := range p.MissingDirs {
				fmt.Printf("    %s  %s\n", shortUID(e.UID), e.Name)
			}
		}
		if len(p.GhostPods) == 0 && len(p.OrphanMachines) == 0 &&
			len(p.StaleDirs) == 0 && len(p.MissingDirs) == 0 {
			fmt.Println("  OK")
		}
		fmt.Println()
	}

	if !d.Healthy {
		os.Exit(1)
	}
	return nil
}

func cmdVersion(ctx context.Context, c *control.Client, asJSON bool) error {
	v, err := c.Version(ctx)
	if err != nil {
		return err
	}
	if asJSON {
		return printJSON(v)
	}

	fmt.Printf("perigeos %s (%s)\n", v.Version, v.GitCommit)
	fmt.Printf("  go:   %s\n", v.GoVersion)
	fmt.Printf("  os:   %s/%s\n", v.OS, v.Arch)
	return nil
}

func cmdTop(ctx context.Context, c *control.Client, asJSON bool) error {
	interval := 2 * time.Second

	// Single-shot mode for JSON output
	if asJSON {
		r, err := c.Top(ctx)
		if err != nil {
			return err
		}
		return printJSON(r)
	}

	// Two-sample loop: compute CPU rate between successive reads.
	prev, err := c.Top(ctx)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Handle Ctrl+C gracefully - restore terminal on exit.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	defer signal.Stop(sigCh)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-sigCh:
			return nil
		case <-ticker.C:
			cur, err := c.Top(ctx)
			if err != nil {
				return err
			}

			// Clear screen (ANSI: cursor home + erase display).
			fmt.Print("\033[H\033[2J")

			elapsedNs := float64(cur.TimestampNs - prev.TimestampNs)

			// Index previous by name for O(1) lookup.
			prevByName := make(map[string]control.PawnTopInfo, len(prev.Pawns))
			for _, p := range prev.Pawns {
				prevByName[p.Name] = p
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
			fmt.Printf("Refreshing every %s - Ctrl+C to exit\n\n", interval)
			fmt.Fprintln(w, "PAWN\tPODS\tCPU\tMEM(MiB)\tWS(MiB)")

			var totalPods int
			var totalCPUm float64
			var totalMemMiB, totalWSMiB int64

			for _, p := range cur.Pawns {
				var cpuMillicores float64
				if old, ok := prevByName[p.Name]; ok && elapsedNs > 0 {
					cpuNsDelta := float64(p.CPUUsageNs) - float64(old.CPUUsageNs)
					if cpuNsDelta < 0 {
						cpuNsDelta = 0
					}
					cpuMillicores = (cpuNsDelta / elapsedNs) * 1000
				}
				memMiB := int64(p.MemoryBytes / (1024 * 1024))
				wsMiB := int64(p.MemoryWSBytes / (1024 * 1024))

				fmt.Fprintf(w, "%s\t%d\t%.0fm\t%d\t%d\n",
					p.Name, p.PodCount, cpuMillicores, memMiB, wsMiB)

				totalPods += p.PodCount
				totalCPUm += cpuMillicores
				totalMemMiB += memMiB
				totalWSMiB += wsMiB
			}

			fmt.Fprintf(w, "TOTAL\t%d\t%.0fm\t%d\t%d\n",
				totalPods, totalCPUm, totalMemMiB, totalWSMiB)
			w.Flush()

			fmt.Printf("\nHost: %d/%d MiB  Load: %s\n",
				cur.MemUsedMiB, cur.MemTotalMiB, cur.LoadAvg)

			prev = cur
		}
	}
}

func cmdShowcase(ctx context.Context, c *control.Client, asJSON bool) error {
	status, err := c.Status(ctx)
	if err != nil {
		return err
	}

	pawns, err := c.Pawns(ctx)
	if err != nil {
		return err
	}

	type showcaseData struct {
		ClusterName     string           `json:"cluster_name"`
		PawnCount       int              `json:"pawn_count"`
		PodCount        int              `json:"pod_count"`
		TotalCPUCores   int              `json:"total_cpu_cores"`
		MemoryTotalMiB  int64            `json:"memory_total_mib"`
		MemoryUsedMiB   int64            `json:"memory_used_mib"`
		MemoryPercent   int              `json:"memory_percent"`
		PerigeosRSSMiB  int64            `json:"perigeos_rss_mib"`
		OverheadPercent float64          `json:"overhead_percent"`
		Pawns           []map[string]any `json:"pawns"`
	}

	// Calculate dynamic values
	avgMemPerPawn := int64(0)
	if status.PawnCount > 0 {
		avgMemPerPawn = status.MemTotalMiB / int64(status.PawnCount)
	}

	totalPodCapacity := 0
	pawnData := []map[string]any{}
	for _, p := range pawns.Pawns {
		totalPodCapacity += p.PodCount

		memPercent := 0
		if avgMemPerPawn > 0 {
			memPercent = int(math.Round(100 * float64(p.MemoryMiB) / float64(avgMemPerPawn)))
		}
		if memPercent > 100 {
			memPercent = 100
		}

		pawnData = append(pawnData, map[string]any{
			"name":           p.Name,
			"is_primary":     p.IsPrimary,
			"pod_count":      p.PodCount,
			"memory_mib":     p.MemoryMiB,
			"memory_percent": memPercent,
			"cpu_usage_ms":   p.CPUUsageMs,
		})
	}

	memPercent := int(math.Round(100 * float64(status.MemUsedMiB) / float64(status.MemTotalMiB)))
	overheadPercent := 100 * float64(status.PerigeosRSSMiB) / float64(status.MemTotalMiB)

	data := showcaseData{
		ClusterName:     status.Hostname,
		PawnCount:       status.PawnCount,
		PodCount:        status.PodCount,
		TotalCPUCores:   status.CPUCores,
		MemoryTotalMiB:  status.MemTotalMiB,
		MemoryUsedMiB:   status.MemUsedMiB,
		MemoryPercent:   memPercent,
		PerigeosRSSMiB:  status.PerigeosRSSMiB,
		OverheadPercent: overheadPercent,
		Pawns:           pawnData,
	}

	if asJSON {
		return printJSON(data)
	}

	// Print title
	fmt.Printf("Perigeos Cluster: %d pawns, %d pods\n\n", status.PawnCount, status.PodCount)

	// Print pawn list
	fmt.Println("Pawns:")
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tROLE\tPODS\tMEMORY(MiB)")
	for _, p := range pawns.Pawns {
		role := "pawn"
		if p.IsPrimary {
			role = "primary"
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\n", p.Name, role, p.PodCount, p.MemoryMiB)
	}
	w.Flush()
	fmt.Println()

	// Print cluster stats
	fmt.Println("Cluster Stats:")
	fmt.Printf("  Total pods:        %d / %d\n", status.PodCount, totalPodCapacity)
	fmt.Printf("  Total capacity:    %d CPU cores\n", status.CPUCores)
	fmt.Printf("  Memory used:       %d / %d MiB (%d%%)\n", status.MemUsedMiB, status.MemTotalMiB, memPercent)
	fmt.Printf("  Perigeos overhead: %d MiB (%.2f%%)\n", status.PerigeosRSSMiB, overheadPercent)
	fmt.Printf("  k8s node:          1 physical host\n")

	return nil
}

func cmdImages(ctx context.Context, c *control.Client, asJSON bool) error {
	resp, err := c.Images(ctx)
	if err != nil {
		return err
	}
	if asJSON {
		return json.NewEncoder(os.Stdout).Encode(resp)
	}

	fmt.Printf("Cache directory: %s\n\n", resp.CacheDir)

	if len(resp.Images) == 0 {
		fmt.Println("No cached images.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "IMAGE\tLAYERS\tSIZE\tDIGEST")
	for _, img := range resp.Images {
		digest := img.Digest
		if len(digest) > 19 {
			digest = digest[:19] + "…"
		}
		if digest == "" {
			digest = "<unknown>"
		}
		fmt.Fprintf(w, "%s\t%d\t%s\t%s\n",
			img.Name,
			img.Layers,
			formatBytes(img.SizeBytes),
			digest,
		)
	}
	w.Flush()
	return nil
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func cmdDrain(ctx context.Context, c *control.Client) error {
	resp, err := c.Drain(ctx)
	if err != nil {
		return err
	}
	fmt.Println(resp)
	return nil
}

// cmdStop drains all pawns (NotReady + actively stops every pod) and then
// invokes `systemctl stop perigeos`. Use this for clean decommission of the
// host. `systemctl restart perigeos` is unaffected and remains a fast bounce
// that leaves containers running (KillMode=process + HydrateFromRuntime).
func cmdStop(ctx context.Context, c *control.Client) error {
	fmt.Println("Draining pawns (mark NotReady + stop all pods)...")
	resp, err := c.Stop(ctx)
	if err != nil {
		return fmt.Errorf("drain via control socket: %w", err)
	}
	fmt.Println(resp)

	fmt.Println("Running: systemctl stop perigeos")
	cmd := exec.CommandContext(ctx, "systemctl", "stop", "perigeos")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("systemctl stop perigeos: %w", err)
	}
	fmt.Println("perigeos stopped.")
	return nil
}
