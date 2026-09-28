//go:build !windows

package main

// enableVT is a no-op off Windows: POSIX terminals already interpret ANSI
// sequences, whereas the Windows console host needs
// ENABLE_VIRTUAL_TERMINAL_PROCESSING switched on explicitly (vt_windows.go).
func enableVT() {}
