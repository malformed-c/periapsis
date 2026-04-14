BINS = userns-shim-amd64 userns-shim-arm64

all: $(BINS)

userns-shim-amd64: main.c
	x86_64-linux-gnu-gcc -static -Wall -Wextra -fanalyzer -O3 -o $@ $<
	strip $@
	upx --best --lzma $@

userns-shim-arm64: main.c
	aarch64-linux-gnu-gcc -static -Wall -Wextra -fanalyzer -O3 -o $@ $<
	aarch64-linux-gnu-strip $@
	upx --best --lzma $@

clean:
	rm -f $(BINS)

.PHONY: all clean
