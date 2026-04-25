// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-only

package image

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// extractLayer unpacks a tar stream into dst.
// Whiteout files (.wh.*) are kept on disk - overlayfs handles them at runtime.
func extractLayer(dst string, tr *tar.Reader) error {
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Security: ZipSlip protection.
		// Clean the name first so "./" becomes "" and "../foo" becomes "../foo".
		// Entries that resolve to exactly dst (e.g. "./" root dir) are harmless - skip them.
		clean := filepath.Clean(header.Name)
		if filepath.IsAbs(clean) {
			return fmt.Errorf("security violation: absolute path %s", header.Name)
		}

		target := filepath.Join(dst, clean)
		dstClean := filepath.Clean(dst)
		if target != dstClean && !strings.HasPrefix(target, dstClean+string(os.PathSeparator)) {
			return fmt.Errorf("security violation: invalid path %s", header.Name)
		}
		if target == dstClean {
			// Root dir entry - nothing to create, dst already exists.
			continue
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, header.FileInfo().Mode()); err != nil {
				return err
			}

		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, header.FileInfo().Mode())
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return err
			}
			f.Close()

		case tar.TypeSymlink:
			os.MkdirAll(filepath.Dir(target), 0755)
			os.Remove(target)
			if err := os.Symlink(header.Linkname, target); err != nil {
				return err
			}

		case tar.TypeLink:
			linkTarget := filepath.Join(dst, header.Linkname)
			os.MkdirAll(filepath.Dir(target), 0755)
			os.Remove(target)
			if err := os.Link(linkTarget, target); err != nil {
				return err
			}
		}
	}

	return nil
}
