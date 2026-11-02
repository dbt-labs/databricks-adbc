// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package databricks

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	// leaseTTL is how long a lease is valid without being renewed. The holder
	// renews it while it works, so a crashed process frees the lease within
	// this time.
	leaseTTL = 30 * time.Second
	// leaseWaitTimeout is how long a process waits for another one's lease.
	leaseWaitTimeout = 90 * time.Second

	leaseGrace   = 20 * time.Millisecond
	leasePollMin = 100 * time.Millisecond
	leasePollMax = 500 * time.Millisecond
)

// fileLease is an exclusive, expiring lock shared between processes through
// one file. It lets a single process open the browser while the others wait
// for the token it caches. The file holds the owner's ID and the expiry.
//
// A lease is taken by writing the file atomically (temp file and rename) and
// reading it back after a short grace period: if another process wrote at the
// same time, only one ID survives and the other keeps waiting.
type fileLease struct {
	path string
	id   string
}

func newLeaseID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

// readLease returns the owner and expiry in the lease file. A missing or
// corrupt file reads as no lease.
func readLease(path string) (id string, expiry time.Time) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", time.Time{}
	}
	parts := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(parts) != 2 {
		return "", time.Time{}
	}
	ms, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || parts[0] == "" {
		return "", time.Time{}
	}
	return parts[0], time.UnixMilli(ms)
}

func writeLease(path, id string, expiry time.Time) error {
	f, err := os.CreateTemp(filepath.Dir(path), "*.lease")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp) //nolint:errcheck
	if _, err := fmt.Fprintf(f, "%s\n%d\n", id, expiry.UnixMilli()); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// acquireLease waits up to timeout (or until ctx ends) for the lease at path.
func acquireLease(ctx context.Context, path string, ttl, timeout time.Duration) (*fileLease, error) {
	id, err := newLeaseID()
	if err != nil {
		return nil, fmt.Errorf("lease: %w", err)
	}
	deadline := time.Now().Add(timeout)
	for {
		owner, expiry := readLease(path)
		if owner == "" || !time.Now().Before(expiry) {
			if err := writeLease(path, id, time.Now().Add(ttl+leaseGrace)); err == nil {
				if err := sleepCtx(ctx, leaseGrace); err != nil {
					return nil, err
				}
				if owner, _ := readLease(path); owner == id {
					return &fileLease{path: path, id: id}, nil
				}
			}
		}
		if !time.Now().Before(deadline) {
			return nil, fmt.Errorf("timed out after %s waiting for another login to finish (lease %s)", timeout, path)
		}
		if err := sleepCtx(ctx, jitter(leasePollMin, leasePollMax)); err != nil {
			return nil, err
		}
	}
}

// renew extends the lease. It fails if another process has taken it over.
func (l *fileLease) renew(ttl time.Duration) error {
	if owner, _ := readLease(l.path); owner != l.id {
		return fmt.Errorf("lease %s is no longer held", l.path)
	}
	return writeLease(l.path, l.id, time.Now().Add(ttl))
}

// release frees the lease if it is still ours.
func (l *fileLease) release() {
	if owner, _ := readLease(l.path); owner == l.id {
		_ = os.Remove(l.path)
	}
}

// keepAlive renews the lease until the returned function is called.
func (l *fileLease) keepAlive(ttl time.Duration) (stop func()) {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		t := time.NewTicker(ttl / 3)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				_ = l.renew(ttl)
			}
		}
	}()
	return func() { close(done); <-finished }
}

func jitter(lo, hi time.Duration) time.Duration {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(hi-lo)+1))
	if err != nil {
		return lo
	}
	return lo + time.Duration(n.Int64())
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
