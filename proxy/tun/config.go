package tun

import (
	"context"
	"net"
	"sync"

	"github.com/xtls/xray-core/common/errors"
)

type InterfaceUpdater struct {
	sync.Mutex

	tunIndex  int
	fixedName string
	iface     *net.Interface
}

var updater *InterfaceUpdater

func (updater *InterfaceUpdater) Get() *net.Interface {
	updater.Lock()
	defer updater.Unlock()

	return updater.iface
}

func (updater *InterfaceUpdater) Update() {
	updater.Lock()
	defer updater.Unlock()

	got, err := findOutboundInterface(updater.tunIndex, updater.fixedName)
	if err != nil {
		errors.LogWarning(context.Background(), "[tun] failed to update interface, outbounds will be refused: ", err)
		updater.iface = nil
		return
	}

	if got == nil {
		errors.LogWarning(context.Background(), "[tun] failed to update interface > got == nil")
		updater.iface = nil
		return
	}

	// Sending our own dials out of the TUN feeds them straight back into the stack that
	// dialled them, which then treats them as a new client connection and dials again.
	if got.Index == updater.tunIndex {
		errors.LogWarning(context.Background(), "[tun] outbound interface would be the TUN itself, refusing: ", got.Name, " ", got.Index)
		updater.iface = nil
		return
	}
	if updater.tunIndex <= 0 {
		// Without a usable TUN index the check above cannot say anything; keep dialling
		// and let the per connection fuse catch a loop instead.
		errors.LogWarning(context.Background(), "[tun] cannot tell which interface is the TUN, index ", updater.tunIndex)
	}

	if updater.iface != nil && updater.iface.Index == got.Index && updater.iface.Name == got.Name {
		return
	}

	updater.iface = got
	errors.LogInfo(context.Background(), "[tun] update interface ", got.Name, " ", got.Index)
}
