package stats

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const cgroupRoot = "/sys/fs/cgroup"

// cgroupPath returns the cgroup v2 path for a pawn slice.
func cgroupPawnSlice(pawnName string) string {
	return filepath.Join(cgroupRoot, fmt.Sprintf("perigeos-%s.slice", pawnName))
}

// cgroupServicePath returns the cgroup v2 path for a specific container service.
func cgroupServicePath(pawnName, podUID, containerName string) string {
	unit := fmt.Sprintf("perigeos-%s-pod-%s-%s.service", pawnName, podUID, containerName)
	return filepath.Join(cgroupPawnSlice(pawnName), unit)
}

// readCPUStat reads cpu.stat from a cgroup directory and returns usage_usec.
func readCPUStat(cgDir string) (usageUsec uint64, err error) {
	f, err := os.Open(filepath.Join(cgDir, "cpu.stat"))
	if err != nil {
		return 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "usage_usec ") {
			parts := strings.Fields(line)
			if len(parts) == 2 {
				usageUsec, err = strconv.ParseUint(parts[1], 10, 64)
				return
			}
		}
	}
	return 0, fmt.Errorf("usage_usec not found in %s/cpu.stat", cgDir)
}

// readMemoryCurrent reads memory.current from a cgroup directory.
func readMemoryCurrent(cgDir string) (uint64, error) {
	data, err := os.ReadFile(filepath.Join(cgDir, "memory.current"))
	if err != nil {
		return 0, err
	}
	val, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse memory.current in %s: %w", cgDir, err)
	}
	return val, nil
}

// readMemoryInactive reads memory.stat's inactive_file, used to compute working set.
// working_set = memory.current - inactive_file
func readMemoryInactive(cgDir string) (uint64, error) {
	f, err := os.Open(filepath.Join(cgDir, "memory.stat"))
	if err != nil {
		return 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "inactive_file ") {
			parts := strings.Fields(line)
			if len(parts) == 2 {
				return strconv.ParseUint(parts[1], 10, 64)
			}
		}
	}
	return 0, nil // not fatal - working set == usage if missing
}

// ReadContainerCPU reads CPU stats for a container cgroup.
// Returns (usageCoreNanoSeconds, error).
func ReadContainerCPU(pawnName, podUID, containerName string) (uint64, error) {
	cgDir := cgroupServicePath(pawnName, podUID, containerName)
	usageUsec, err := readCPUStat(cgDir)
	if err != nil {
		return 0, err
	}
	return usageUsec * 1000, nil // µs -> ns
}

// ReadContainerMemory reads memory stats for a container cgroup.
// Returns (usageBytes, workingSetBytes, error).
func ReadContainerMemory(pawnName, podUID, containerName string) (usage, workingSet uint64, err error) {
	cgDir := cgroupServicePath(pawnName, podUID, containerName)
	usage, err = readMemoryCurrent(cgDir)
	if err != nil {
		return
	}
	inactive, _ := readMemoryInactive(cgDir)
	workingSet = usage
	if inactive < workingSet {
		workingSet -= inactive
	}
	return
}

// ReadSliceCPU reads CPU stats for the entire pawn slice.
func ReadSliceCPU(pawnName string) (uint64, error) {
	cgDir := cgroupPawnSlice(pawnName)
	usageUsec, err := readCPUStat(cgDir)
	if err != nil {
		return 0, err
	}
	return usageUsec * 1000, nil
}

// ReadSliceMemory reads memory stats for the entire pawn slice.
func ReadSliceMemory(pawnName string) (usage, workingSet uint64, err error) {
	cgDir := cgroupPawnSlice(pawnName)
	usage, err = readMemoryCurrent(cgDir)
	if err != nil {
		return
	}
	inactive, _ := readMemoryInactive(cgDir)
	workingSet = usage
	if inactive < workingSet {
		workingSet -= inactive
	}
	return
}
