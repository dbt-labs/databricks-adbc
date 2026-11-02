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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"golang.org/x/oauth2"
)

const (
	tokenCacheFileSuffix = "databricks_oauth_token_cache.json"
	tokenLeaseFileName   = "oauth_flow.lease"
)

// tokenCacheEntry is the on-disk form of a token. It matches the file the
// databricks-sql-go fork writes, so both read each other's cache.
type tokenCacheEntry struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	TokenType    string    `json:"token_type"`
	Expiry       time.Time `json:"expiry"`
}

// tokenCache keeps one browser-login token per workspace (and per custom
// client ID) in a directory, and a lease file that serialises logins across
// processes. Files are private to the user (0600, directory 0700).
type tokenCache struct {
	dir string
}

// defaultTokenCacheDir is ~/.cache/databricks/oauth (XDG_CACHE_HOME when set,
// %LOCALAPPDATA% on Windows). It returns "" when no home is known.
func defaultTokenCacheDir() string {
	if runtime.GOOS == "windows" {
		if dir := os.Getenv("LOCALAPPDATA"); dir != "" {
			return filepath.Join(dir, "databricks", "oauth")
		}
		return ""
	}
	if dir := os.Getenv("XDG_CACHE_HOME"); dir != "" {
		return filepath.Join(dir, "databricks", "oauth")
	}
	if home := os.Getenv("HOME"); home != "" {
		return filepath.Join(home, ".cache", "databricks", "oauth")
	}
	return ""
}

func newTokenCache(dir string) (*tokenCache, error) {
	if dir == "" {
		return nil, errors.New("no token cache directory")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &tokenCache{dir: dir}, nil
}

// path is the cache file of one workspace. The built-in client keeps the plain
// host name; a custom client ID gets its own file, because a refresh token only
// works with the client that obtained it.
func (c *tokenCache) path(host, customClientID string) string {
	name := host
	if customClientID != "" {
		sum := sha256.Sum256([]byte(customClientID))
		name += "_" + hex.EncodeToString(sum[:4])
	}
	return filepath.Join(c.dir, name+"_"+tokenCacheFileSuffix)
}

func (c *tokenCache) leasePath() string { return filepath.Join(c.dir, tokenLeaseFileName) }

// read returns the cached token, or nil when there is none or it is unreadable.
// The access token may be expired: the refresh token is still useful.
func (c *tokenCache) read(host, customClientID string) *oauth2.Token {
	data, err := os.ReadFile(c.path(host, customClientID))
	if err != nil {
		return nil
	}
	var e tokenCacheEntry
	if json.Unmarshal(data, &e) != nil || (e.AccessToken == "" && e.RefreshToken == "") {
		return nil
	}
	return &oauth2.Token{AccessToken: e.AccessToken, RefreshToken: e.RefreshToken, TokenType: e.TokenType, Expiry: e.Expiry}
}

// write stores the token atomically, so a reader never sees half a file.
func (c *tokenCache) write(host, customClientID string, t *oauth2.Token) error {
	data, err := json.MarshalIndent(tokenCacheEntry{
		AccessToken: t.AccessToken, RefreshToken: t.RefreshToken, TokenType: t.TokenType, Expiry: t.Expiry,
	}, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(c.dir, "*.tmp") // created 0600
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp) //nolint:errcheck
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, c.path(host, customClientID))
}

func (c *tokenCache) acquireLease(ctx context.Context) (*fileLease, error) {
	l, err := acquireLease(ctx, c.leasePath(), leaseTTL, leaseWaitTimeout)
	if err != nil {
		return nil, fmt.Errorf("token cache: %w", err)
	}
	return l, nil
}
