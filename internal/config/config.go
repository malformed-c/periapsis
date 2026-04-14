package config

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"

	toml "github.com/pelletier/go-toml"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/kubernetes/pkg/util/taints"

	"github.com/malformed-c/periapsis/internal/cgroup"
)

var (
	ErrInvalidCPUFormat    = errors.New("invalid CPU format")
	ErrInvalidPercentValue = errors.New("invalid percentage value for CPU")
)

func Load(rawConfigPath string) (*RawPerigeosConfig, error) {
	configPath, err := filepath.Abs(rawConfigPath)
	if err != nil {
		return nil, fmt.Errorf("couldn't resole config path: %w", err)
	}

	file, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("couldn't open config file '%s': %w", configPath, err)
	}

	defer file.Close()

	rawPerigeosConfig := &RawPerigeosConfig{}
	if err := toml.NewDecoder(file).Decode(rawPerigeosConfig); err != nil {
		return nil, fmt.Errorf("couldn't parse TOML config: %w", err)
	}

	return rawPerigeosConfig, nil
}

func parseCPU(c string, defaultCPU resource.Quantity) (resource.Quantity, error) {
	if c == "" {
		// Why toml returns empty string?
		return defaultCPU, nil
	}

	q, err := resource.ParseQuantity(c)
	if err == nil {
		return q, nil
	}

	// Try to handle the percentage
	r, _ := regexp.Compile(`^(?P<num>\d+)(?:%)$`)

	matches := r.FindStringSubmatch(c)

	if matches == nil {
		// The CPU value doens't look like Kubernetes Quality neither like systemd Limit
		return resource.Quantity{}, ErrInvalidCPUFormat
	}

	number := matches[1]

	// Convert percentage to millicores
	// 100% === 1000m === 1 Core
	percent, err := strconv.ParseInt(number, 0, 0)
	if err != nil {
		// This happens if the number is huge, e.g., "999999999999999999999%"
		return resource.Quantity{}, ErrInvalidPercentValue
	}

	millicores := percent * 10

	return resource.ParseQuantity(fmt.Sprintf("%dm", millicores))
}

func deriveCPUWeight(cpu resource.Quantity, configuredWeight uint64) uint64 {
	if configuredWeight > 0 {
		return configuredWeight
	}
	return cgroup.MilliCPUToCPUWeight(cpu.MilliValue())
}

func (r *RawPerigeosConfig) Process(baseDir string) (*PerigeosConfig, error) {
	perigeosConfig := PerigeosConfig{}

	baseCPU := resource.MustParse("100m")
	// baseMemory := resource.MustParse("50M")

	defaultCPU, err := parseCPU(r.Global.DefaultCPU, baseCPU)
	if err != nil {
		return &PerigeosConfig{}, fmt.Errorf("failed to parse DefaultCPU: %w", err)
	}

	defaultMemory, err := resource.ParseQuantity(r.Global.DefaultMemory)
	if err != nil {
		return &PerigeosConfig{}, fmt.Errorf("failed to parse DefaultMemory: %w", err)
	}

	perigeosConfig.Global.PerigeosPort = r.Global.PerigeosPort
	perigeosConfig.Global.DefaultCPU = defaultCPU
	perigeosConfig.Global.DefaultMemory = defaultMemory
	perigeosConfig.Global.BaseDir = baseDir
	perigeosConfig.Global.CNI = buildCNIConfig(r.Global.CNI)
	perigeosConfig.Global.Primary = r.Global.Primary

	const (
		defaultCAPath    = "/var/lib/rancher/k3s/server/tls/server-ca.crt"
		defaultCAKeyPath = "/var/lib/rancher/k3s/server/tls/server-ca.key"
	)
	perigeosConfig.Global.ServerCAPath = r.Global.ServerCAPath
	if perigeosConfig.Global.ServerCAPath == "" {
		perigeosConfig.Global.ServerCAPath = defaultCAPath
	}
	perigeosConfig.Global.ServerCAKeyPath = r.Global.ServerCAKeyPath
	if perigeosConfig.Global.ServerCAKeyPath == "" {
		perigeosConfig.Global.ServerCAKeyPath = defaultCAKeyPath
	}

	// --- Pawn Sets ---
	// We expand pawn sets into the main Pawns map before processing
	if r.Pawns == nil {
		r.Pawns = make(map[string]RawPawnConfig)
	}

	for setName, setCfg := range r.PawnSets {
		if setCfg.Count <= 0 {
			continue
		}

		// Determine the naming pattern
		formatStr := "%s-%d"
		// Dynamic Width Calculation
		// We calculate how many digits are needed for the largest index (Count - 1)
		maxIndex := max(setCfg.Count-1, 0)
		width := len(strconv.Itoa(maxIndex))

		// Create a format string with fixed width (e.g., "%s-%02d")
		formatStr = fmt.Sprintf("%%s-%%0%dd", width)

		for i := 0; i < setCfg.Count; i++ {
			generatedName := fmt.Sprintf(formatStr, setName, i)

			if _, exists := r.Pawns[generatedName]; exists {
				return nil, fmt.Errorf("pawn set generation collision: %s is already defined manually", generatedName)
			}

			pawnCfg := setCfg.RawPawnConfig
			pawnCfg.Port = 0 // Force auto-assignment

			r.Pawns[generatedName] = pawnCfg
		}
	}

	// --- Primary virtual node ---
	// When primary=true, auto-generate a pawn entry named after the
	// hostname. This becomes a full virtual node that can run DaemonSets
	// (e.g. constellation-agent) without requiring a k3s agent on the host.
	if r.Global.Primary {
		hostName, _ := os.Hostname()
		if hostName != "" {
			if _, exists := r.Pawns[hostName]; exists {
				return nil, fmt.Errorf("primary pawn name %q conflicts with an existing pawn", hostName)
			}
			r.Pawns[hostName] = RawPawnConfig{
				CPU:    r.Global.DefaultCPU,
				Memory: r.Global.DefaultMemory,
				NodeIP: r.Global.NodeIP,
				// Port 0 → auto-assign; uses PerigeosPort (base port) for the primary
				Port: r.Global.PerigeosPort,
			}
		}
	}

	usedPorts := make(map[int]bool)
	nextPort := perigeosConfig.Global.PerigeosPort + 1

	// --- PASS 1: Reserve all manually specified ports ---
	for _, pawn := range r.Pawns {
		if pawn.Port != 0 {
			if usedPorts[pawn.Port] {
				// We found a duplicate in the config file itself.
				return nil, fmt.Errorf("port collision in config: port %d is specified more than once", pawn.Port)
			}

			usedPorts[pawn.Port] = true
		}
	}

	sortedNames := slices.Sorted(maps.Keys(r.Pawns))

	for _, name := range sortedNames {
		currentPawn := r.Pawns[name]

		// TODO: Use FSM or iterator
		assignedPort := currentPawn.Port
		if assignedPort == 0 {
			// --- Find the next available port ---
			// This loop skips over any reserved ports.
			for usedPorts[nextPort] {
				nextPort++
			}

			assignedPort = nextPort
			usedPorts[assignedPort] = true // Mark the new port as used for the next iteration.
			nextPort++                     // Move the counter for the next search
		}

		cpu, err := parseCPU(currentPawn.CPU, defaultCPU)
		if err != nil {
			return nil, fmt.Errorf("failed to parse cpu for pawn %s: %w", name, err)
		}

		var mem resource.Quantity
		switch currentPawn.Memory {
		case "":
			mem = defaultMemory

		default:
			mem, err = resource.ParseQuantity(currentPawn.Memory)
			if err != nil {
				return &PerigeosConfig{}, fmt.Errorf("failed to parse memory pawn %s: %w", name, err)
			}
		}

		spec := make([]string, 0)

		for key, val := range currentPawn.Taints {
			// Correct format: "key=value:Effect"
			spec = append(spec, key+"="+val)
		}

		pawnTaints, _, err := taints.ParseTaints(spec)
		if err != nil {
			return &PerigeosConfig{}, fmt.Errorf("failed to parse taints for pawn %s: %w", name, err)
		}

		// Parse IO Limits (optional - zero value means no limit)
		var ioRead, ioWrite resource.Quantity
		if currentPawn.IOReadBandwidthMax != "" {
			ioRead, err = resource.ParseQuantity(currentPawn.IOReadBandwidthMax)
			if err != nil {
				return nil, fmt.Errorf("failed to parse io_read_bandwidth_max for pawn %s: %w", name, err)
			}
		}
		if currentPawn.IOWriteBandwidthMax != "" {
			ioWrite, err = resource.ParseQuantity(currentPawn.IOWriteBandwidthMax)
			if err != nil {
				return nil, fmt.Errorf("failed to parse io_write_bandwidth_max for pawn %s: %w", name, err)
			}
		}

		// Detect whether this pawn is the auto-generated primary.
		hostName, _ := os.Hostname()
		isPrimary := r.Global.Primary && name == hostName

		pawnConfig := PawnConfig{
			Name:                name,
			Port:                assignedPort,
			NodeIP:              currentPawn.NodeIP,
			BaseDir:             baseDir,
			IsPrimary:           isPrimary,
			Labels:              currentPawn.Labels,
			Taints:              pawnTaints,
			CPU:                 cpu,
			Memory:              mem,
			CPUWeight:           deriveCPUWeight(cpu, currentPawn.CPUWeight),
			IOReadBandwidthMax:  ioRead,
			IOWriteBandwidthMax: ioWrite,
			CreateConcurrency:   currentPawn.CreateConcurrency,
		}

		perigeosConfig.Pawns = append(perigeosConfig.Pawns, pawnConfig)
	}

	return &perigeosConfig, nil
}

// constellationSocketPath returns the path to the constellation-agent unix socket.
// With Option B (one agent per host) the socket is at the default Cilium path.
func constellationSocketPath() string {
	return "/var/run/cilium/cilium.sock"
}

// buildCNIConfig converts a raw global CNI config into the processed form.
//
// Auto-detection: if no [global.cni] block is present but a constellation-agent
// socket exists at /var/run/cilium/cilium.sock, CNI is enabled automatically.
// Explicit [global.cni] always wins.
//
// Returns nil when no block is present AND no agent socket is found -
// callers fall back to per-pawn built-in veth networking.
func buildCNIConfig(raw *RawCNIConfig) *CNIConfig {
	if raw == nil {
		if _, err := os.Stat(constellationSocketPath()); err != nil {
			return nil
		}
		raw = &RawCNIConfig{}
	}

	binDir := raw.BinDir
	if binDir == "" {
		binDir = "/opt/cni/bin"
	}

	confDir := raw.ConfDir
	if confDir == "" {
		confDir = "/etc/cni/net.d/constellation"
	}

	return &CNIConfig{
		BinDir:  binDir,
		ConfDir: confDir,
		Debug:   raw.Debug,
	}
}
