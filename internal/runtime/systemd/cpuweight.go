package systemd

const (
	minCPUShares = int64(2)
	maxCPUShares = int64(262144)
)

// milliCPUToCPUWeight converts Kubernetes millicores to systemd CPUWeight.
// Conversion follows Kubernetes CPU shares semantics:
//
//	shares = milliCPU * 1024 / 1000, clamped to [2, 262144]
//	weight = 1 + ((shares - 2) * 9999) / 262142
func milliCPUToCPUWeight(milliCPU int64) uint64 {
	if milliCPU <= 0 {
		return 0
	}

	shares := min(max(milliCPU*1024/1000, minCPUShares), maxCPUShares)

	return uint64(1 + ((shares-minCPUShares)*9999)/(maxCPUShares-minCPUShares))
}
