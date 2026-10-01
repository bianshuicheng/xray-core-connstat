package net

import (
	"testing"
	"time"
)

func TestNoteSelfOutboundRefusesTunSourcedDial(t *testing.T) {
	RegisterTunAddress(ParseAddress("172.18.0.1"))

	before := SelfRefusedCount()
	if err := NoteSelfOutbound("tcp", ParseAddress("172.18.0.1"), 51000); err != ErrOutboundThroughTun {
		t.Fatalf("a dial leaving through our own TUN must be refused, got %v", err)
	}
	if SelfRefusedCount() != before+1 {
		t.Fatalf("a refusal must be counted: %d -> %d", before, SelfRefusedCount())
	}
	// The same port behind a real interface is an ordinary dial.
	if err := NoteSelfOutbound("tcp", ParseAddress("192.168.2.88"), 51000); err != nil {
		t.Fatalf("legit dial was refused: %v", err)
	}
	if SelfRefusedCount() != before+1 {
		t.Fatal("an admitted dial must not move the refusal counter")
	}
}

func TestIsSelfOutboundMatchesOnlyWhatWasDialed(t *testing.T) {
	port := uint16(45123)
	if err := NoteSelfOutbound("tcp", ParseAddress("203.0.113.7"), port); err != nil {
		t.Fatal(err)
	}
	if !IsSelfOutbound("tcp", ParseAddress("203.0.113.7"), port) {
		t.Fatal("the dial we just recorded must be recognised on arrival")
	}
	if IsSelfOutbound("udp", ParseAddress("203.0.113.7"), port) {
		t.Fatal("transport is part of the identity")
	}
	if IsSelfOutbound("tcp", ParseAddress("203.0.113.7"), port+1) {
		t.Fatal("a port we never dialled is somebody else's connection")
	}
	if IsSelfOutbound("tcp", ParseAddress("203.0.113.8"), port) {
		t.Fatal("address is part of the identity")
	}
}

func TestIsSelfOutboundNeedsAPort(t *testing.T) {
	if IsSelfOutbound("tcp", ParseAddress("203.0.113.9"), 0) {
		t.Fatal("without a source port nothing can be attributed")
	}
}

func TestSelfOutboundEntriesExpire(t *testing.T) {
	port := uint16(46000)
	if err := NoteSelfOutbound("tcp", ParseAddress("203.0.113.10"), port); err != nil {
		t.Fatal(err)
	}

	key := selfOutboundKey{network: "tcp", address: "203.0.113.10", port: port}
	selfOutboundTable.mu.Lock()
	selfOutboundTable.outbounds[key] = time.Now().Add(-selfOutboundTTL - time.Second)
	selfOutboundTable.mu.Unlock()

	if IsSelfOutbound("tcp", ParseAddress("203.0.113.10"), port) {
		t.Fatal("a stale record must not outlive the connection it describes")
	}
	selfOutboundTable.mu.Lock()
	_, found := selfOutboundTable.outbounds[key]
	selfOutboundTable.mu.Unlock()
	if found {
		t.Fatal("expired record should be dropped on read")
	}
}
