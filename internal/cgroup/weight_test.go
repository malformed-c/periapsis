package cgroup

import "testing"

func TestMilliCPUToCPUWeight(t *testing.T) {
	tests := []struct {
		name     string
		milliCPU int64
		want     uint64
	}{
		{name: "zero", milliCPU: 0, want: 0},
		{name: "tiny clamps to min", milliCPU: 1, want: 1},
		{name: "half core", milliCPU: 500, want: 20},
		{name: "one core", milliCPU: 1000, want: 39},
		{name: "huge clamps to max", milliCPU: 1000000, want: 10000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MilliCPUToCPUWeight(tt.milliCPU); got != tt.want {
				t.Fatalf("MilliCPUToCPUWeight(%d) = %d, want %d", tt.milliCPU, got, tt.want)
			}
		})
	}
}
