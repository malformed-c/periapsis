package cgroup

import (
	"strconv"
	"strings"

	"github.com/containerd/cgroups/v3/cgroup2"
	"github.com/coreos/go-systemd/v22/dbus"
	dbusv5 "github.com/godbus/dbus/v5"
)

// MilliCPUToCPUMax converts Kubernetes millicores into a cgroup2 CPUMax value
// using the kernel default 100ms period. milliCPU <= 0 yields "max" (no limit).
func MilliCPUToCPUMax(milliCPU int64) cgroup2.CPUMax {
	if milliCPU <= 0 {
		return cgroup2.NewCPUMax(nil, nil)
	}
	quota := milliCPU * 100
	return cgroup2.NewCPUMax(&quota, nil)
}

// BuildSystemdProperties translates a cgroup2.Resources struct into the
// equivalent set of systemd transient-unit D-Bus properties.
//
// Supported today: CPU.Weight, CPU.Max, Memory.Max/High/Low/Min/Swap, Pids.Max.
// Fields outside this set are silently ignored — add them here as pod-label
// driven cgroup knobs grow.
func BuildSystemdProperties(res *cgroup2.Resources) []dbus.Property {
	if res == nil {
		return nil
	}
	var props []dbus.Property

	if res.CPU != nil {
		if res.CPU.Weight != nil && *res.CPU.Weight > 0 {
			props = append(props, dbus.Property{
				Name:  "CPUWeight",
				Value: dbusv5.MakeVariant(*res.CPU.Weight),
			})
		}
		if res.CPU.Max != "" {
			if quotaUS, ok := cpuMaxToQuotaPerSecUS(res.CPU.Max); ok {
				props = append(props, dbus.Property{
					Name:  "CPUQuotaPerSecUSec",
					Value: dbusv5.MakeVariant(quotaUS),
				})
			}
		}
	}

	if res.Memory != nil {
		if v := res.Memory.Max; v != nil && *v > 0 {
			props = append(props, dbus.Property{
				Name: "MemoryMax", Value: dbusv5.MakeVariant(uint64(*v)),
			})
		}
		if v := res.Memory.High; v != nil && *v > 0 {
			props = append(props, dbus.Property{
				Name: "MemoryHigh", Value: dbusv5.MakeVariant(uint64(*v)),
			})
		}
		if v := res.Memory.Low; v != nil && *v > 0 {
			props = append(props, dbus.Property{
				Name: "MemoryLow", Value: dbusv5.MakeVariant(uint64(*v)),
			})
		}
		if v := res.Memory.Min; v != nil && *v > 0 {
			props = append(props, dbus.Property{
				Name: "MemoryMin", Value: dbusv5.MakeVariant(uint64(*v)),
			})
		}
		if v := res.Memory.Swap; v != nil && *v > 0 {
			props = append(props, dbus.Property{
				Name: "MemorySwapMax", Value: dbusv5.MakeVariant(uint64(*v)),
			})
		}
	}

	if res.Pids != nil && res.Pids.Max > 0 {
		props = append(props, dbus.Property{
			Name: "TasksMax", Value: dbusv5.MakeVariant(uint64(res.Pids.Max)),
		})
	}

	return props
}

// cpuMaxToQuotaPerSecUS converts a cgroup2 "cpu.max" value ("quota period" in
// microseconds) into systemd's CPUQuotaPerSecUSec (microseconds of CPU time
// allowed per wall-clock second). Returns false for "max" / unparseable input.
func cpuMaxToQuotaPerSecUS(m cgroup2.CPUMax) (uint64, bool) {
	parts := strings.Fields(string(m))
	if len(parts) == 0 {
		return 0, false
	}
	if strings.EqualFold(parts[0], "max") {
		return 0, false
	}
	quota, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || quota <= 0 {
		return 0, false
	}
	period := int64(100000)
	if len(parts) == 2 {
		p, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || p <= 0 {
			return 0, false
		}
		period = p
	}
	return uint64(quota * 1_000_000 / period), true
}
