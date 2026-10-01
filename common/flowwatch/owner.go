package flowwatch

import (
	"path/filepath"

	xnet "github.com/xtls/xray-core/common/net"
)

// lookupOwner resolves which application owns the local socket of a flow. It goes
// through the shared, briefly cached socket view that routing's process matcher uses
// too, so a connection storm costs one table enumeration per cache window instead of
// one per connection.
func lookupOwner(network string, source xnet.Address, sourcePort uint16, destination xnet.Address, destinationPort uint16) (ownerInfo, bool) {
	if network != "tcp" && network != "udp" {
		return ownerInfo{err: "unsupported network"}, true
	}
	if sourcePort == 0 || !source.Family().IsIP() {
		return ownerInfo{err: "no socket address"}, true
	}

	var destIP string
	var destPort uint16
	if destination != nil && destination.Family().IsIP() && destinationPort > 0 {
		destIP = destination.String()
		destPort = destinationPort
	}

	pid, name, path, err := xnet.FindProcessCached(network, source.String(), sourcePort, destIP, destPort)
	if err != nil || pid == 0 || name == "" {
		return ownerInfo{}, false
	}
	return ownerInfo{name: name, exe: filepath.ToSlash(path), pid: pid}, true
}
