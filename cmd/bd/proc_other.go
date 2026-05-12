//go:build !windows

package main

import "os/exec"

// hideWindow is a no-op on non-Windows platforms. The console-window flash
// it suppresses on Windows doesn't exist on Linux/macOS.
func hideWindow(cmd *exec.Cmd) {}
