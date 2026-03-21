package control

// StatusResponse from GET /v1/status
type StatusResponse struct {
	Hostname    string `json:"hostname"`
	UptimeSecs  int64  `json:"uptime_secs"`
	PawnCount   int    `json:"pawn_count"`
	PodCount    int    `json:"pod_count"`
	Version     string `json:"version"`
	GoVersion   string `json:"go_version"`
	Arch        string `json:"arch"`
	OS          string `json:"os"`
	Kernel      string `json:"kernel"`
	MemTotalMiB int64  `json:"mem_total_mib"`
	MemUsedMiB  int64  `json:"mem_used_mib"`
	CPUCores    int    `json:"cpu_cores"`
	LoadAvg     string `json:"load_avg"`

	// Extended stats
	Machines       int   `json:"machines"`          // machinectl registered machines
	DiskDirs       int   `json:"disk_dirs"`         // pod directories on disk
	SystemdUnits   int   `json:"systemd_units"`     // perigeos pod service units
	PerigeosRSSMiB int64 `json:"perigeos_rss_mib"`  // perigeos process RSS
	LxcVeths       int   `json:"lxc_veths"`         // lxc* veth interfaces
	NetnsCount     int   `json:"netns_count"`       // /var/run/netns entries
}

// PawnInfo in GET /v1/pawns
type PawnInfo struct {
	Name       string `json:"name"`
	IsPrimary  bool   `json:"is_primary"`
	Port       int    `json:"port"`
	NodeIP     string `json:"node_ip"`
	PodCount   int    `json:"pod_count"`
	CPUUsageMs int64  `json:"cpu_usage_ms"`
	MemoryMiB  int64  `json:"memory_mib"`
}

type PawnsResponse struct {
	Pawns []PawnInfo `json:"pawns"`
}

// TopResponse from GET /v1/top
type TopResponse struct {
	TimestampNs int64         `json:"timestamp_ns"`
	LoadAvg     string        `json:"load_avg"`
	MemUsedMiB  int64         `json:"mem_used_mib"`
	MemTotalMiB int64         `json:"mem_total_mib"`
	Pawns       []PawnTopInfo `json:"pawns"`
}

// PawnTopInfo holds per-pawn cgroup stats from a single sample.
type PawnTopInfo struct {
	Name          string `json:"name"`
	IsPrimary     bool   `json:"is_primary"`
	PodCount      int    `json:"pod_count"`
	CPUUsageNs    uint64 `json:"cpu_usage_ns"`    // cumulative usage_usec * 1000
	MemoryBytes   uint64 `json:"memory_bytes"`
	MemoryWSBytes uint64 `json:"memory_ws_bytes"` // working set
}

// PodInfo in GET /v1/pods
type PodInfo struct {
	Name       string `json:"name"`
	Namespace  string `json:"namespace"`
	UID        string `json:"uid"`
	PawnName   string `json:"pawn_name"`
	PodIP      string `json:"pod_ip"`
	Phase      string `json:"phase"`
	Containers int    `json:"containers"`
}

type PodsResponse struct {
	Pods []PodInfo `json:"pods"`
}

// DoctorResponse from GET /v1/doctor
type DoctorResponse struct {
	Healthy bool             `json:"healthy"`
	Pawns   []PawnDiagnosis  `json:"pawns"`
	Summary DoctorSummary    `json:"summary"`
}

// PawnDiagnosis is the per-pawn state comparison.
type PawnDiagnosis struct {
	Name string `json:"name"`

	// Counts from each source of truth.
	GambitPods   int `json:"gambit_pods"`
	SystemdUnits int `json:"systemd_units"`
	DiskDirs     int `json:"disk_dirs"`

	// Discrepancies — UIDs that exist in one source but not another.
	GhostPods       []DoctorEntry `json:"ghost_pods,omitempty"`        // in gambit, not in systemd
	OrphanMachines  []DoctorEntry `json:"orphan_machines,omitempty"`   // in systemd, not in gambit
	StaleDirs       []string      `json:"stale_dirs,omitempty"`        // on disk, not in gambit
	MissingDirs     []DoctorEntry `json:"missing_dirs,omitempty"`      // in gambit, not on disk
}

// DoctorEntry identifies a pod in a discrepancy report.
type DoctorEntry struct {
	UID  string `json:"uid"`
	Name string `json:"name,omitempty"`
}

// DoctorSummary aggregates counts across all pawns.
type DoctorSummary struct {
	TotalGambit    int `json:"total_gambit"`
	TotalSystemd   int `json:"total_systemd"`
	TotalDisk      int `json:"total_disk"`
	TotalGhosts    int `json:"total_ghosts"`
	TotalOrphans   int `json:"total_orphans"`
	TotalStaleDirs int `json:"total_stale_dirs"`
	LxcVeths       int `json:"lxc_veths"`
	NetnsCount     int `json:"netns_count"`
}

// VersionResponse from GET /v1/version
type VersionResponse struct {
	Version   string `json:"version"`
	GoVersion string `json:"go_version"`
	Arch      string `json:"arch"`
	OS        string `json:"os"`
	GitCommit string `json:"git_commit"`
}
