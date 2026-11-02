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
	"crypto/x509"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
)

// optionDef reads and writes one database option. set validates the value and
// stores it only when it is valid; get returns the stored value as a string.
type optionDef struct {
	set func(d *databaseImpl, value string) error
	get func(d *databaseImpl) string
}

// stringOption is an option that stores any string as given.
func stringOption(field func(d *databaseImpl) *string) optionDef {
	return optionDef{
		set: func(d *databaseImpl, value string) error { *field(d) = value; return nil },
		get: func(d *databaseImpl) string { return *field(d) },
	}
}

// intOption is an option holding an integer, where "" leaves the setting alone
// and a value that is not positive reads back as "".
func intOption(label string, field func(d *databaseImpl) *int) optionDef {
	return optionDef{
		set: func(d *databaseImpl, value string) error {
			if value == "" {
				return nil
			}
			n, err := strconv.Atoi(value)
			if err != nil {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("invalid %s: %v", label, err),
				}
			}
			*field(d) = n
			return nil
		},
		get: func(d *databaseImpl) string {
			if n := *field(d); n > 0 {
				return strconv.Itoa(n)
			}
			return ""
		},
	}
}

// databaseOptions holds every database option that has a fixed key. Options
// with a key prefix (session parameters and query tags) are handled by
// SetOption and GetOption.
var databaseOptions = map[string]optionDef{
	adbc.OptionKeyURI: {
		set: func(d *databaseImpl, value string) error {
			// Strip the databricks:// scheme since databricks-sql-go expects raw DSN format
			after, ok := strings.CutPrefix(value, "databricks://")
			if !ok {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("invalid URI scheme: expected 'databricks://', got '%s'", value),
				}
			}
			d.conn.uri = after
			return nil
		},
		get: func(d *databaseImpl) string { return d.conn.uri },
	},
	OptionServerHostname: stringOption(func(d *databaseImpl) *string { return &d.conn.serverHostname }),
	OptionHTTPPath:       stringOption(func(d *databaseImpl) *string { return &d.conn.httpPath }),
	OptionAccessToken:    stringOption(func(d *databaseImpl) *string { return &d.auth.accessToken }),
	OptionAuthType: {
		set: func(d *databaseImpl, value string) error {
			if _, ok := loginMethods[value]; !ok && value != "" {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("[db] unknown auth type '%s'", value),
				}
			}
			d.auth.authType = value
			return nil
		},
		get: func(d *databaseImpl) string { return d.auth.authType },
	},
	OptionUserAgent: stringOption(func(d *databaseImpl) *string { return &d.conn.userAgent }),
	OptionPort: {
		set: func(d *databaseImpl, value string) error {
			port, err := strconv.Atoi(value)
			if err != nil || port < 1 || port > 65535 {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  "invalid port number",
				}
			}
			d.conn.port = port
			return nil
		},
		get: func(d *databaseImpl) string { return strconv.Itoa(d.conn.port) },
	},
	OptionCatalog: stringOption(func(d *databaseImpl) *string { return &d.conn.catalog }),
	OptionSchema:  stringOption(func(d *databaseImpl) *string { return &d.conn.schema }),
	OptionQueryTimeout: {
		set: func(d *databaseImpl, value string) error {
			if value == "" {
				return nil
			}
			timeout, err := time.ParseDuration(value)
			if err != nil {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("invalid query timeout: %v", err),
				}
			}
			d.query.queryTimeout = timeout
			return nil
		},
		get: func(d *databaseImpl) string {
			if d.query.queryTimeout > 0 {
				return d.query.queryTimeout.String()
			}
			return ""
		},
	},
	OptionMaxRows:             intOption("max rows", func(d *databaseImpl) *int { return &d.query.maxRows }),
	OptionQueryRetryCount:     intOption("query retry count", func(d *databaseImpl) *int { return &d.query.queryRetryCount }),
	OptionDownloadThreadCount: intOption("download thread count", func(d *databaseImpl) *int { return &d.query.downloadThreadCount }),
	OptionSSLMode: {
		set: func(d *databaseImpl, value string) error {
			if value == "" {
				d.tls.sslMode = value
				d.tls.sslInsecure = false
				return nil
			}
			lowerValue := strings.ToLower(value)
			switch lowerValue {
			case "insecure":
				d.tls.sslMode = lowerValue
				d.tls.sslInsecure = true
			case "require":
				d.tls.sslMode = lowerValue
				d.tls.sslInsecure = false
			default:
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("invalid SSL mode: %s (supported: 'require', 'insecure')", value),
				}
			}
			return nil
		},
		get: func(d *databaseImpl) string { return d.tls.sslMode },
	},
	OptionSSLRootCert: {
		set: func(d *databaseImpl, value string) error {
			if value == "" {
				d.tls.sslRootCert = value
				d.tls.sslCertPool = nil
				return nil
			}
			// Validate that the certificate file exists and can be read.
			// Then, store valid cert.
			caCert, err := os.ReadFile(value)
			if err != nil {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("failed to read SSL root certificate: %v", err),
				}
			}

			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  "failed to parse SSL root certificate",
				}
			}

			d.tls.sslRootCert = value
			d.tls.sslCertPool = caCertPool
			return nil
		},
		get: func(d *databaseImpl) string { return d.tls.sslRootCert },
	},
	OptionOAuthClientID:        stringOption(func(d *databaseImpl) *string { return &d.auth.oauthClientID }),
	OptionOAuthClientSecret:    stringOption(func(d *databaseImpl) *string { return &d.auth.oauthClientSecret }),
	OptionOAuthRefreshToken:    stringOption(func(d *databaseImpl) *string { return &d.auth.oauthRefreshToken }),
	OptionAzureClientID:        stringOption(func(d *databaseImpl) *string { return &d.auth.azureClientID }),
	OptionAzureClientSecret:    stringOption(func(d *databaseImpl) *string { return &d.auth.azureClientSecret }),
	OptionAzureTenantID:        stringOption(func(d *databaseImpl) *string { return &d.auth.azureTenantID }),
	OptionGoogleCredentials:    stringOption(func(d *databaseImpl) *string { return &d.auth.googleCredentials }),
	OptionGoogleServiceAccount: stringOption(func(d *databaseImpl) *string { return &d.auth.googleServiceAccount }),
	OptionOIDCClientID:         stringOption(func(d *databaseImpl) *string { return &d.auth.oidcClientID }),
	OptionOIDCTokenFilepath:    stringOption(func(d *databaseImpl) *string { return &d.auth.oidcTokenFilepath }),
	OptionOAuthRedirectURL: {
		set: func(d *databaseImpl, value string) error {
			if err := validateBrowserRedirectURL(value); err != nil {
				return err
			}
			d.auth.oauthRedirectURL = value
			return nil
		},
		get: func(d *databaseImpl) string { return d.auth.oauthRedirectURL },
	},
	OptionOAuthScopes: {
		set: func(d *databaseImpl, value string) error {
			scopes, err := parseScopes(value)
			if err != nil {
				return err
			}
			d.auth.oauthScopes = scopes
			return nil
		},
		get: func(d *databaseImpl) string { return strings.Join(d.auth.oauthScopes, " ") },
	},
	OptionExternalBrowserTimeout: {
		set: func(d *databaseImpl, value string) error {
			if value == "" {
				d.auth.oauthU2MTimeout = 0
				return nil
			}
			timeout, err := time.ParseDuration(value)
			if err != nil || timeout <= 0 {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("[db] invalid external browser timeout '%s': must be a positive duration such as '2m'", value),
				}
			}
			d.auth.oauthU2MTimeout = timeout
			return nil
		},
		get: func(d *databaseImpl) string {
			if d.auth.oauthU2MTimeout > 0 {
				return d.auth.oauthU2MTimeout.String()
			}
			return ""
		},
	},
}
