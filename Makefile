PERIGEOS    := perigeos
APSIS       := apsis
INSTALL_DIR := /usr/local/bin
CONFIG_DIR  := /etc/apsis/perigeos
SERVICE_SRC := deploy/perigeos.service
SERVICE_DST := /etc/systemd/system/perigeos.service

# Malformed systemd-nspawn
LIB_DIR     := /usr/local/lib
LD_CONF_DST := /etc/ld.so.conf.d/apsis.conf
NSPAWN_URL  := https://github.com/malformed-c/systemd/releases/download/v260.1-3-apsis/systemd-nspawn
LIB_URL     := https://github.com/malformed-c/systemd/releases/download/v260.1-3-apsis/libsystemd-shared-261.so
NSPAWN_BIN  := systemd-nspawn
NSPAWN_LIB  := libsystemd-shared-261.so

VERSION      := $(shell git describe --tags --always --dirty="-dev")
DATE         := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
VERSION_FLAGS := -ldflags='-s -w -X "main.buildVersion=$(VERSION)" -X "main.buildTime=$(DATE)"'
GCFLAGS      := -gcflags="-l=4"

.PHONY: all build build-perigeos build-apsis test clean install uninstall fetch-deps

all: build fetch-deps

build: build-perigeos build-apsis

build-perigeos:
	go build -trimpath $(GCFLAGS) $(VERSION_FLAGS) -o $(PERIGEOS) ./cmd/perigeos

build-apsis:
	go build -trimpath $(GCFLAGS) $(VERSION_FLAGS) -o $(APSIS) ./cmd/apsis

test:
	go test ./...

fetch-deps:
	@echo "Downloading Malformed systemd-nspawn dependencies..."
	curl -L $(NSPAWN_URL) -o $(NSPAWN_BIN)
	curl -L $(LIB_URL) -o $(NSPAWN_LIB)
	@chmod +x $(NSPAWN_BIN)

clean:
	rm -f $(PERIGEOS) $(APSIS) $(NSPAWN_BIN) $(NSPAWN_LIB)

# This target assumes it is being run as root (e.g., via sudo make install)
install:
	@echo "Installing binaries and dependencies..."
	systemctl stop perigeos 2>/dev/null || true
	install -m 0755 $(PERIGEOS) $(INSTALL_DIR)/$(PERIGEOS)
	install -m 0755 $(APSIS) $(INSTALL_DIR)/$(APSIS)
	install -m 0755 $(NSPAWN_BIN) $(INSTALL_DIR)/$(NSPAWN_BIN)
	install -m 0644 $(NSPAWN_LIB) $(LIB_DIR)/$(NSPAWN_LIB)

	# Add /usr/local/lib to the system linker path
	echo "$(LIB_DIR)" > $(LD_CONF_DST)

	mkdir -p $(CONFIG_DIR)
	install -m 0644 $(SERVICE_SRC) $(SERVICE_DST)

	ldconfig
	systemctl daemon-reload
	systemctl start perigeos

	@echo "Installation complete."
	@if [ ! -f "$(CONFIG_DIR)/perigeos.toml" ]; then \
		echo "No config at $(CONFIG_DIR)/perigeos.toml - copy one from configs/"; \
	fi

uninstall:
	systemctl stop perigeos 2>/dev/null || true
	systemctl disable perigeos 2>/dev/null || true
	rm -f $(INSTALL_DIR)/$(PERIGEOS) $(INSTALL_DIR)/$(APSIS) $(INSTALL_DIR)/$(NSPAWN_BIN)
	rm -f $(LIB_DIR)/$(NSPAWN_LIB)
	rm -f $(LD_CONF_DST)
	rm -f $(SERVICE_DST)
	ldconfig
	systemctl daemon-reload
	@echo "Uninstalled binaries and service. Config left in $(CONFIG_DIR)."
