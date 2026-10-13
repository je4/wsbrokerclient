package wschat

import (
	"emperror.dev/errors"
	"github.com/BurntSushi/toml"
	"github.com/je4/wsbrokerclient/demo/config"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
)

type WSChatConfig struct {
	Name      string        `toml:"name"`
	WSBroker  string        `toml:"wsbroker"`
	Channels  []string      `toml:"channels"`
	ClientTLS loader.Config `toml:"clienttls"`
	WebTLS    loader.Config `toml:"webtls"`
}

// LoadConfig loads the WSChatConfig from path, or from the embedded default wschat.toml if path is empty.
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
