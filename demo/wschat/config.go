package wschat

import (
	"emperror.dev/errors"
	"github.com/BurntSushi/toml"
	"github.com/je4/wsbrokerclient/demo/config"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
)

// WSChatConfig defines configuration parameters for the wschat demo application.
//
// Fields:
//   - Name: Unique client identifier (e.g. "test01") used for wsbrokerclient identity and authorization.
//   - WSBroker: Target broker endpoint URL (e.g. "https://localhost:8765" or "wss://...").
//   - Channels: Initial list of channels to subscribe to on startup (e.g. ["all", "intern"]).
//   - ClientTLS: mTLS configuration via certloader (MiniVault, File, Env, Dev, or ACME).
//   - WebTLS: TLS configuration for the local HTTPS web interface server.
type WSChatConfig struct {
	Name      string        `toml:"name"`
	WSBroker  string        `toml:"wsbroker"`
	Channels  []string      `toml:"channels"`
	ClientTLS loader.Config `toml:"clienttls"`
	WebTLS    loader.Config `toml:"webtls"`
}

// LoadConfig loads the WSChatConfig from the specified TOML file path, or falls back
// to the embedded default configuration (demo/config/wschat.toml) if path is empty.
func LoadConfig(path string) (*WSChatConfig, error) {
	conf := &WSChatConfig{}
	if path == "" {
		if _, err := toml.Decode(config.WSChatConfigTOML, conf); err != nil {
			return nil, errors.Wrap(err, "cannot decode embedded toml")
		}
		return conf, nil
	}
	if _, err := toml.DecodeFile(path, conf); err != nil {
		return nil, errors.Wrapf(err, "cannot decode toml file '%s'", path)
	}
	return conf, nil
}
