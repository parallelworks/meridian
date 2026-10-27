//go:build !linux && !darwin && !freebsd

package local

import "io/fs"

// identity is unknown where the driver doesn't run, and nothing is kept.
func identity(fs.FileInfo) (fileIdentity, bool) { return fileIdentity{}, false }
