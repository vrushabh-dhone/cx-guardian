package live

import (
	"sync"
	"testing"
	"time"
)

// TestWithHeartbeat verifies the fix for intermittent "Live stream disconnected" on the
// cascade signal: a slow blocking call (e.g. the Loki victims query) must not leave the SSE
// connection silent — withHeartbeat should emit periodically while fn runs, stop once fn
// returns, and never emit concurrently with the caller's subsequent emit() calls.
func TestWithHeartbeat(t *testing.T) {
	var mu sync.Mutex
	var events []string
	emit := func(event string, data map[string]any) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, event)
	}

	start := time.Now()
	withHeartbeat(emit, 20*time.Millisecond, func() {
		time.Sleep(90 * time.Millisecond) // simulate a slow external call
	})
	elapsed := time.Since(start)

	mu.Lock()
	beforeCount := len(events)
	allHeartbeats := true
	for _, e := range events {
		if e != "heartbeat" {
			allHeartbeats = false
		}
	}
	mu.Unlock()

	if elapsed < 90*time.Millisecond {
		t.Fatalf("withHeartbeat returned before fn finished: elapsed=%v", elapsed)
	}
	if beforeCount == 0 {
		t.Fatal("expected at least one heartbeat during a 90ms call with a 20ms interval, got none")
	}
	if !allHeartbeats {
		t.Fatalf("expected only \"heartbeat\" events before withHeartbeat returned, got %v", events)
	}

	// Emit once more right after withHeartbeat returns — this must never race with the
	// ticker goroutine (which the -race flag would catch if the shutdown sync were wrong).
	// Lock is released above, so this is safe: it's a fresh, independent acquisition.
	emit("log", map[string]any{"msg": "done"})

	mu.Lock()
	defer mu.Unlock()
	if len(events) != beforeCount+1 || events[len(events)-1] != "log" {
		t.Fatalf("expected exactly one more event (\"log\") recorded cleanly after return, got %v", events)
	}
}

// TestWithHeartbeatFastCall verifies a fast fn (faster than the interval) gets zero
// heartbeats — the normal, common-case path (all current Simulation/Live scenarios finish
// in well under a second) must be unaffected.
func TestWithHeartbeatFastCall(t *testing.T) {
	var mu sync.Mutex
	var count int
	emit := func(event string, data map[string]any) {
		mu.Lock()
		defer mu.Unlock()
		count++
	}

	withHeartbeat(emit, 1*time.Second, func() {
		time.Sleep(5 * time.Millisecond)
	})

	mu.Lock()
	defer mu.Unlock()
	if count != 0 {
		t.Fatalf("expected 0 heartbeats for a fast call, got %d", count)
	}
}
