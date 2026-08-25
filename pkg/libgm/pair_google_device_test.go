package libgm

import (
	"errors"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

func TestGaiaDiscoveryRequiresExplicitSelectionAcrossAttempt(t *testing.T) {
	response := &gmproto.SignInGaiaResponse{DeviceData: &gmproto.SignInGaiaResponse_DeviceData{
		UnknownItems2: []*gmproto.RPCGaiaData_UnknownContainer_Item2_Item1{
			{DestOrSourceUUID: "11111111-1111-1111-1111-111111111111", UnknownInt4: 1, UnknownBigInt7: 7},
			{DestOrSourceUUID: "22222222-2222-2222-2222-222222222222", UnknownInt4: 1, UnknownBigInt7: 8},
		},
		UnknownItems3: []*gmproto.RPCGaiaData_UnknownContainer_Item4{
			{DestOrSourceUUID: "11111111-1111-1111-1111-111111111111", UnknownTimestampMicroseconds: time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC).UnixMicro()},
			{DestOrSourceUUID: "22222222-2222-2222-2222-222222222222", UnknownTimestampMicroseconds: time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC).UnixMicro()},
		},
	}}
	discovery, devices, err := newGaiaDeviceDiscovery(response)
	if err != nil {
		t.Fatalf("newGaiaDeviceDiscovery: %v", err)
	}
	if len(devices) != 2 || devices[0].ID == "" || devices[0].Label == "" || devices[1].ID == "" || devices[1].Label == "" {
		t.Fatalf("safe choices = %#v", devices)
	}
	if _, err := discovery.selectDevice(""); !errors.Is(err, ErrDeviceSelectionRequired) {
		t.Fatalf("empty selection error = %v", err)
	}
	if _, err := discovery.selectDevice("unknown"); !errors.Is(err, ErrUnknownGaiaDevice) {
		t.Fatalf("unknown selection error = %v", err)
	}
	other, _, err := newGaiaDeviceDiscovery(&gmproto.SignInGaiaResponse{DeviceData: &gmproto.SignInGaiaResponse_DeviceData{
		UnknownItems2: []*gmproto.RPCGaiaData_UnknownContainer_Item2_Item1{{DestOrSourceUUID: "33333333-3333-3333-3333-333333333333", UnknownInt4: 1}},
	}})
	if err != nil {
		t.Fatalf("second discovery: %v", err)
	}
	if _, err := other.selectDevice(devices[0].ID); !errors.Is(err, ErrUnknownGaiaDevice) {
		t.Fatalf("cross-attempt selection error = %v", err)
	}
}
