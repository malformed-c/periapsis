PERIGEOS    := perigeos
APSIS       := apsis
INSTALL_DIR := /usr/local/bin
CONFIG_DIR  := /etc/apsis/perigeos
SERVICE_SRC := deploy/perigeos.service
SERVICE_DST := /etc/systemd/system/perigeos.service

VERSION      := $(shell git describe --tags --always --dirty="-dev")
DATE         := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
VERSION_FLAGS := -ldflags='-s -w -X "main.buildVersion=$(VERSION)" -X "main.buildTime=$(DATE)"'
GCFLAGS      := -gcflags="-l=4"

.PHONY: all build build-perigeos build-apsis test clean install uninstall

all: build

build: build-perigeos build-apsis

build-perigeos:
	go build -trimpath $(GCFLAGS) $(VERSION_FLAGS) -o $(PERIGEOS) ./cmd/perigeos

build-apsis:
	go build -trimpath $(GCFLAGS) $(VERSION_FLAGS) -o $(APSIS) ./cmd/apsis

test:
	go test ./...

clean:
	rm -f $(PERIGEOS) $(APSIS)

install: build
	sudo systemctl stop perigeos 2>/dev/null || true
	sudo install -m 0755 $(PERIGEOS) $(INSTALL_DIR)/$(PERIGEOS)
	sudo install -m 0755 $(APSIS) $(INSTALL_DIR)/$(APSIS)
	sudo mkdir -p $(CONFIG_DIR)
	sudo install -m 0644 $(SERVICE_SRC) $(SERVICE_DST)
	sudo systemctl daemon-reload
	sudo systemctl start perigeos
	@echo "Installed $(INSTALL_DIR)/$(PERIGEOS) and $(INSTALL_DIR)/$(APSIS)"
	@if [ ! -f "$(CONFIG_DIR)/perigeos.toml" ]; then \
		echo "No config at $(CONFIG_DIR)/perigeos.toml - copy one from configs/"; \
	fi

uninstall:
	sudo systemctl stop perigeos 2>/dev/null || true
	sudo systemctl disable perigeos 2>/dev/null || true
	sudo rm -f $(INSTALL_DIR)/$(PERIGEOS) $(INSTALL_DIR)/$(APSIS)
	sudo rm -f $(SERVICE_DST)
	sudo systemctl daemon-reload
	@echo "Uninstalled perigeos and apsis binaries and service"
	@echo "Config left in $(CONFIG_DIR) - remove manually if desired"
