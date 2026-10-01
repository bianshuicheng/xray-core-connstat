//go:build !windows

package tun

import "net/netip"

// egressIsTun is a no-op off Windows: those platforms bind by name and the OS refuses a
// route through a device the process owns, so there is nothing to second-guess here.
func egressIsTun(dest netip.Addr, tunIndex int) bool {
	return false
}
