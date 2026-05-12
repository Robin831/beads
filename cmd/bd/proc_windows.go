//go:build windows

package main

import (
	"os/exec"
	"syscall"
)

// createNoWindow tells CreateProcess not to attach a console at all. Stronger
// than HideWindow alone: a hidden window can still flicker for a frame at
// process creation if the child briefly attaches before the WM_SHOW filter
// hides it; CREATE_NO_WINDOW avoids the attachment entirely.
const createNoWindow = 0x08000000

// hideWindow sets the CreateProcess flags on cmd so its child process does
// not spawn a visible console window. Used for short-lived subprocess
// invocations from bd that fire many times per minute (`dolt push/pull` from
// the auto-sync hooks, the dolt-sync scheduled task, etc.) — without this,
// each invocation produces a brief console-window flash on the user's
// desktop.
//
// Safe to call multiple times — flag-OR is idempotent.
func hideWindow(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.HideWindow = true
	cmd.SysProcAttr.CreationFlags |= createNoWindow
}
