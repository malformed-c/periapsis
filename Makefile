BINARY     := perigeos
INSTALL_DIR := /usr/local/bin
CONFIG_DIR  := /etc/apsis/perigeos
SERVICE_SRC := deploy/perigeos.service
SERVICE_DST := /etc/systemd/system/perigeos.service

VERSION      := $(shell git describe --tags --always --dirty="-dev")
DATE         := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
VERSION_FLAGS := -ldflags='-X "main.buildVersion=$(VERSION)" -X "main.buildTime=$(DATE)"'
GCFLAGS      := -gcflags="-l=4"

.PHONY: all build test clean install uninstall

all: build

build:
	go build -trimpath $(GCFLAGS) $(VERSION_FLAGS) -o $(BINARY) ./cmd/perigeos

test:
	go test ./...

clean:
	rm -f $(BINARY)

install: build
	@if [ "$$(id -u)" -ne 0 ]; then echo "Must run as root" >&2; exit 1; fi
	systemctl stop perigeos 2>/dev/null || true
	install -m 0755 $(BINARY) $(INSTALL_DIR)/$(BINARY)
	mkdir -p $(CONFIG_DIR)
	install -m 0644 $(SERVICE_SRC) $(SERVICE_DST)
	systemctl daemon-reload
	systemctl start perigeos
	@echo "Installed $(INSTALL_DIR)/$(BINARY) and $(SERVICE_DST)"
	@if [ ! -f "$(CONFIG_DIR)/perigeos.toml" ]; then \
		echo "No config at $(CONFIG_DIR)/perigeos.toml — copy one from configs/"; \
	fi

uninstall:
	@if [ "$$(id -u)" -ne 0 ]; then echo "Must run as root" >&2; exit 1; fi
	systemctl stop perigeos 2>/dev/null || true
	systemctl disable perigeos 2>/dev/null || true
	rm -f $(INSTALL_DIR)/$(BINARY)
	rm -f $(SERVICE_DST)
	systemctl daemon-reload
	@echo "Uninstalled perigeos binary and service"
	@echo "Config left in $(CONFIG_DIR) — remove manually if desired"
