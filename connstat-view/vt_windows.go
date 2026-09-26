//go:build windows

package main

import "golang.org/x/sys/windows"

// enableVT turns on virtual terminal processing so that ANSI clear-screen
// sequences work in the legacy console host as well.
func enableVT() {
	out, err := windows.GetStdHandle(windows.STD_OUTPUT_HANDLE)
	if err != nil {
		return
	}
	var mode uint32
	if windows.GetConsoleMode(out, &mode) != nil {
		return
	}
	_ = windows.SetConsoleMode(out, mode|windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING)
}
