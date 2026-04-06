package stats

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadCPUStat(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "periapsis-stats-cpu-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cpuStatContent := "usage_usec 123456\nuser_usec 100000\nsystem_usec 23456\n"
	err = os.WriteFile(filepath.Join(tempDir, "cpu.stat"), []byte(cpuStatContent), 0644)
	require.NoError(t, err)

	usage, err := readCPUStat(tempDir)
	assert.NoError(t, err)
	assert.Equal(t, uint64(123456), usage)
}

func TestReadMemoryCurrent(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "periapsis-stats-mem-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	err = os.WriteFile(filepath.Join(tempDir, "memory.current"), []byte("1048576\n"), 0644)
	require.NoError(t, err)

	usage, err := readMemoryCurrent(tempDir)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1048576), usage)
}

func TestReadMemoryInactive(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "periapsis-stats-mem-stat-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	memStatContent := "anon 512000\nfile 512000\nkernel 24576\ninactive_file 256000\n"
	err = os.WriteFile(filepath.Join(tempDir, "memory.stat"), []byte(memStatContent), 0644)
	require.NoError(t, err)

	inactive, err := readMemoryInactive(tempDir)
	assert.NoError(t, err)
	assert.Equal(t, uint64(256000), inactive)
}
