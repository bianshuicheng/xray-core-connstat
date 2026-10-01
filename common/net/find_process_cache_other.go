//go:build !windows

package net

// FindProcessCached answers the same question as FindProcess. Only Windows enumerates
// an expensive connected-socket table, so the other platforms keep the direct lookup.
func FindProcessCached(network, srcIP string, srcPort uint16, destIP string, destPort uint16) (PID int, Name string, AbsolutePath string, err error) {
	return FindProcess(network, srcIP, srcPort, destIP, destPort)
}
