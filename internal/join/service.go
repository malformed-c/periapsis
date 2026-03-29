package join

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"text/template"
)

const unitTemplate = `[Unit]
Description=Perigeos Virtual Kubelet
Documentation=https://github.com/malformed-c/periapsis
After=network-online.target
Wants=network-online.target
Requires=dbus.service

[Service]
Type=simple
ExecStart=/usr/local/bin/perigeos \
    --perigeosconfig {{.ConfigDir}}/perigeos.toml \
    --kubeconfig {{.ConfigDir}}/kubeconfig.yaml \
    --base-dir {{.BaseDir}} \
    --control-socket /run/apsis/perigeos.sock

Restart=on-failure
RestartSec=5s

StateDirectory=apsis
RuntimeDirectory=apsis

StandardOutput=journal
StandardError=journal
SyslogIdentifier=perigeos

ProtectSystem=no
ProtectHome=no
NoNewPrivileges=no
LimitNOFILE=1048576
LimitNPROC=infinity
LimitMEMLOCK=infinity
TasksMax=infinity

KillMode=control-group
TimeoutStopSec=90s

[Install]
WantedBy=multi-user.target
`

const unitPath = "/etc/systemd/system/perigeos.service"

// installService writes the systemd unit file and enables it.
func installService(opts *Options, logger *slog.Logger) error {
	tmpl, err := template.New("unit").Parse(unitTemplate)
	if err != nil {
		return fmt.Errorf("parse unit template: %w", err)
	}

	f, err := os.Create(unitPath)
	if err != nil {
		return fmt.Errorf("create unit file %s: %w", unitPath, err)
	}
	defer f.Close()

	data := struct {
		ConfigDir string
		BaseDir   string
	}{
		ConfigDir: opts.ConfigDir,
		BaseDir:   opts.BaseDir,
	}

	if err := tmpl.Execute(f, data); err != nil {
		return fmt.Errorf("render unit template: %w", err)
	}
	logger.Info("Systemd unit written", "path", unitPath)

	if err := systemctlRun("daemon-reload"); err != nil {
		return fmt.Errorf("daemon-reload: %w", err)
	}

	if err := systemctlRun("enable", "perigeos.service"); err != nil {
		return fmt.Errorf("enable perigeos.service: %w", err)
	}
	logger.Info("perigeos.service enabled")

	return nil
}

// startService starts (or restarts) the perigeos service.
func startService(logger *slog.Logger) error {
	// Use restart so re-joining an already-joined node picks up new config.
	if err := systemctlRun("restart", "perigeos.service"); err != nil {
		return fmt.Errorf("restart perigeos.service: %w", err)
	}
	logger.Info("perigeos.service started")
	return nil
}

func systemctlRun(args ...string) error {
	cmd := exec.Command("systemctl", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
