package tun

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

type InterfaceUpdater struct {
	sync.Mutex

	tunIndex  int
	fixedName string
	iface     *net.Interface
	changedAt int64 // unix seconds of the last nil<->interface transition; reset signal for the admission breaker
}

var updater *InterfaceUpdater

// IfaceChangedAt reports when the outbound interface state last changed (appeared,
// vanished or switched). The admission breaker reads it to open the gate the moment the
// uplink returns, instead of waiting out its full hold.
func (updater *InterfaceUpdater) ChangedAt() int64 {
	updater.Lock()
	defer updater.Unlock()
	return updater.changedAt
}

func (updater *InterfaceUpdater) Get() *net.Interface {
	updater.Lock()
	defer updater.Unlock()

	return updater.iface
}

func (updater *InterfaceUpdater) Update() {
	updater.Lock()
	defer updater.Unlock()

	changed := func() {
		updater.changedAt = time.Now().Unix()
	}

	got, err := findOutboundInterface(updater.tunIndex, updater.fixedName)
	if err != nil {
		errors.LogWarning(context.Background(), "[tun] failed to update interface, outbounds will be refused: ", err)
		if updater.iface != nil {
			changed()
		}
		updater.iface = nil
		return
	}

	if got == nil {
		if updater.iface != nil {
			changed()
		}
		updater.iface = nil
		return
	}

	// Sending our own dials out of the TUN feeds them straight back into the stack that
	// dialled them, which then treats them as a new client connection and dials again.
	if got.Index == updater.tunIndex {
		errors.LogWarning(context.Background(), "[tun] outbound interface would be the TUN itself, refusing: ", got.Name, " ", got.Index)
		if updater.iface != nil {
			changed()
		}
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
	changed()
	errors.LogInfo(context.Background(), "[tun] update interface ", got.Name, " ", got.Index)
}
