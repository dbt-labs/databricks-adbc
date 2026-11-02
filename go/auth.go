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
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/databricks/databricks-sdk-go/common/environment"
	sdkconfig "github.com/databricks/databricks-sdk-go/config"
	dbsql "github.com/databricks/databricks-sql-go"
)

// authField is an option a login method needs, read from the database.
type authField struct {
	option string
	label  string
	value  func(d *databaseImpl) string
}

// loginMethod is one way to authenticate: the options it requires and how it
// configures the SQL driver. Adding a method means adding one entry to
// loginMethods, keyed by its auth_type value.
type loginMethod struct {
	required []authField
	build    func(d *databaseImpl) dbsql.ConnOption
}

var loginMethods = map[string]loginMethod{
	OptionValueAuthTypePAT: {
		required: []authField{
			{OptionAccessToken, "access token", func(d *databaseImpl) string { return d.accessToken }},
		},
		build: func(d *databaseImpl) dbsql.ConnOption {
			return dbsql.WithAccessToken(d.accessToken)
		},
	},
	OptionValueAuthTypeOAuthM2M: {
		required: []authField{
			{OptionOAuthClientID, "client ID", func(d *databaseImpl) string { return d.oauthClientID }},
			{OptionOAuthClientSecret, "client secret", func(d *databaseImpl) string { return d.oauthClientSecret }},
		},
		build: func(d *databaseImpl) dbsql.ConnOption {
			return dbsql.WithClientCredentials(d.oauthClientID, d.oauthClientSecret)
		},
	},
	// OIDC token federation, spelled as in dbt-databricks. The ID token is
	// exchanged for a Databricks token by the Databricks SDK.
	OptionValueAuthTypeEnvOIDC: {
		required: []authField{oidcClientIDField},
		build: func(d *databaseImpl) dbsql.ConnOption {
			return dbsql.WithAuthenticator(d.oidcConfig(OptionValueAuthTypeEnvOIDC))
		},
	},
	OptionValueAuthTypeFileOIDC: {
		required: []authField{oidcClientIDField},
		build: func(d *databaseImpl) dbsql.ConnOption {
			return dbsql.WithAuthenticator(d.oidcConfig(OptionValueAuthTypeFileOIDC))
		},
	},
	// Microsoft Entra ID service principal on Azure Databricks. The SDK sends
	// the Entra token and the management token on each request.
	OptionValueAuthTypeAzureClientSecret: {
		required: []authField{
			{OptionAzureClientID, "azure client ID", func(d *databaseImpl) string { return d.azureClientID }},
			{OptionAzureClientSecret, "azure client secret", func(d *databaseImpl) string { return d.azureClientSecret }},
			{OptionAzureTenantID, "azure tenant ID", func(d *databaseImpl) string { return d.azureTenantID }},
		},
		build: func(d *databaseImpl) dbsql.ConnOption { return dbsql.WithAuthenticator(d.azureClientSecretConfig()) },
	},
	// Azure managed identity. The client ID is optional and selects a
	// user-assigned identity.
	OptionValueAuthTypeAzureMSI: {
		build: func(d *databaseImpl) dbsql.ConnOption { return dbsql.WithAuthenticator(d.azureMSIConfig()) },
	},
	// Azure CLI: uses the current `az login` session.
	OptionValueAuthTypeAzureCLI: {
		build: func(d *databaseImpl) dbsql.ConnOption { return dbsql.WithAuthenticator(d.azureCLIConfig()) },
	},
	// Google service account key, as JSON or the path of a JSON file.
	OptionValueAuthTypeGoogleCredentials: {
		required: []authField{
			{OptionGoogleCredentials, "google credentials", func(d *databaseImpl) string { return d.googleCredentials }},
		},
		build: func(d *databaseImpl) dbsql.ConnOption { return dbsql.WithAuthenticator(d.googleCredentialsConfig()) },
	},
	// Google ID token of a service account, using the application default
	// credentials of the environment.
	OptionValueAuthTypeGoogleID: {
		required: []authField{
			{OptionGoogleServiceAccount, "google service account", func(d *databaseImpl) string { return d.googleServiceAccount }},
		},
		build: func(d *databaseImpl) dbsql.ConnOption { return dbsql.WithAuthenticator(d.googleIDConfig()) },
	},
}

var oidcClientIDField = authField{OptionOIDCClientID, "client ID", func(d *databaseImpl) string { return d.oidcClientID }}

// sdkConfig returns an SDK config for the workspace, to be used as the SQL
// client's authenticator. The config resolves itself on the first request, so
// a bad value surfaces when the connection is opened. Only environment
// variables are read, never ~/.databrickscfg, so a profile on disk cannot
// change the login method.
func (d *databaseImpl) sdkConfig() *sdkconfig.Config {
	host := d.serverHostname
	if !strings.HasPrefix(host, "https://") && !strings.HasPrefix(host, "http://") {
		host = "https://" + host
	}
	return &sdkconfig.Config{
		Host:    host,
		Loaders: []sdkconfig.Loader{sdkconfig.ConfigAttributes},
	}
}

// azureClientSecretConfig builds the SDK credential for an Entra ID service
// principal. The auth type implies Azure, so the cloud is not guessed from the
// host name. The SDK only activates this credential when the tenant is set.
func (d *databaseImpl) azureClientSecretConfig() *sdkconfig.Config {
	cfg := d.sdkConfig()
	cfg.Cloud = environment.CloudAzure
	cfg.AuthType = OptionValueAuthTypeAzureClientSecret
	cfg.AzureClientID = d.azureClientID
	cfg.AzureClientSecret = d.azureClientSecret
	cfg.AzureTenantID = d.azureTenantID
	cfg.Credentials = sdkconfig.AzureClientSecretCredentials{}
	return cfg
}

// azureMSIConfig builds the SDK credential for an Azure managed identity.
func (d *databaseImpl) azureMSIConfig() *sdkconfig.Config {
	cfg := d.sdkConfig()
	cfg.Cloud = environment.CloudAzure
	cfg.AuthType = OptionValueAuthTypeAzureMSI
	cfg.AzureUseMSI = true
	cfg.AzureClientID = d.azureClientID
	cfg.Credentials = sdkconfig.AzureMsiCredentials{}
	return cfg
}

// azureCLIConfig builds the SDK credential that uses the Azure CLI session.
func (d *databaseImpl) azureCLIConfig() *sdkconfig.Config {
	cfg := d.sdkConfig()
	cfg.Cloud = environment.CloudAzure
	cfg.AuthType = OptionValueAuthTypeAzureCLI
	cfg.Credentials = sdkconfig.AzureCliCredentials{}
	return cfg
}

// googleCredentialsConfig builds the SDK credential for a service account key.
func (d *databaseImpl) googleCredentialsConfig() *sdkconfig.Config {
	cfg := d.sdkConfig()
	cfg.Cloud = environment.CloudGCP
	cfg.AuthType = OptionValueAuthTypeGoogleCredentials
	cfg.GoogleCredentials = d.googleCredentials
	cfg.Credentials = sdkconfig.GoogleCredentials{}
	return cfg
}

// googleIDConfig builds the SDK credential for a service account ID token.
func (d *databaseImpl) googleIDConfig() *sdkconfig.Config {
	cfg := d.sdkConfig()
	cfg.Cloud = environment.CloudGCP
	cfg.AuthType = OptionValueAuthTypeGoogleID
	cfg.GoogleServiceAccount = d.googleServiceAccount
	cfg.Credentials = sdkconfig.GoogleDefaultCredentials{}
	return cfg
}

// oidcConfig builds the SDK credential that reads the ID token from an
// environment variable (env-oidc) or a file (file-oidc).
func (d *databaseImpl) oidcConfig(name string) *sdkconfig.Config {
	cfg := d.sdkConfig()
	cfg.ClientID = d.oidcClientID
	cfg.AuthType = name
	if name == OptionValueAuthTypeEnvOIDC {
		cfg.Credentials = sdkconfig.EnvOIDCCredentials{}
	} else {
		cfg.Credentials = sdkconfig.FileOIDCCredentials{}
		// When unset, the SDK falls back to DATABRICKS_OIDC_TOKEN_FILEPATH.
		cfg.OIDCTokenFilepath = d.oidcTokenFilepath
	}
	return cfg
}

// resolveLoginMethod picks the login method for the configured options and
// checks that everything it requires is set. It returns the method's auth_type
// name along with the method.
//
// When databricks.auth_type is set, it names the method. When it is not, the
// method is inferred from the options that are set, as it was before
// auth_type existed: an access token means "pat", a client ID or secret means
// "oauth-m2m".
func (d *databaseImpl) resolveLoginMethod() (string, loginMethod, error) {
	name := d.authType
	if name == "" {
		hasOAuth := d.oauthClientID != "" || d.oauthClientSecret != ""
		switch {
		case d.accessToken == "" && !hasOAuth:
			return "", loginMethod{}, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  "[db] access token or OAuth config is required",
			}
		case d.accessToken != "" && hasOAuth:
			return "", loginMethod{}, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  "[db] cannot specify both access token and OAuth config",
			}
		case d.accessToken != "":
			name = OptionValueAuthTypePAT
		default:
			name = OptionValueAuthTypeOAuthM2M
		}
	}

	method, ok := loginMethods[name]
	if !ok {
		return "", loginMethod{}, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[db] unknown auth type '%s'", name),
		}
	}
	for _, f := range method.required {
		if f.value(d) == "" {
			return "", loginMethod{}, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[db] %s is required when using auth type '%s'. Set this via '%s'.", f.label, name, f.option),
			}
		}
	}
	return name, method, nil
}
