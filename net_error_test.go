//go:build with_purego

package cronet

import (
	"errors"
	"testing"

	E "github.com/sagernet/sing/common/exceptions"
)

// A copy loop logs an error that is not a close as ERROR, so a core shutting
// down printed one for every open naive connection.
func TestALocalAbortReadsAsAClose(t *testing.T) {
	for _, code := range []NetError{NetErrorAborted, NetErrorNetworkChanged, NetErrorConnectionReset} {
		if !E.IsClosedOrCanceled(code) {
			t.Errorf("%s does not read as closed", code.Name())
		}
	}
	for _, code := range []NetError{NetErrorConnectionRefused, NetErrorHTTP2ServerRefusedStream} {
		if E.IsClosedOrCanceled(code) {
			t.Errorf("%s reads as closed", code.Name())
		}
	}
	// Still not the one a ranking caller excuses.
	if errors.Is(NetErrorAborted, ErrClosedLocally) {
		t.Error("ERR_ABORTED matches ErrClosedLocally")
	}
}
