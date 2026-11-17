package config

import (
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// ConfigVersion is the version of the configuration schema
const ConfigVersion = "1.0"

// ControllerVersion is the version of the backup controller application. It is
// written into every backup's metadata and logged at startup.
//
// It is stamped at build time and never edited by hand: the release workflow
// passes the release tag (without its leading "v") to the Docker build, which
// sets it with -ldflags "-X .../internal/config.ControllerVersion=<version>";
// `make build` does the same from `git describe`. A plain `go build` or
// `go test` leaves the default, so "dev" in a backup's metadata means a binary
// that did not come from a release.
//
// It must stay a string variable with a constant initialiser, or -X silently
// stops applying.
var ControllerVersion = "dev"

// GetDefaultComponents returns the default list of components for a new Camunda instance
func GetDefaultComponents() []map[string]interface{} {
	return []map[string]interface{}{
		{
			"name":    types.ComponentZeebe,
			"enabled": true,
		},
		{
			"name":    types.ComponentOperate,
			"enabled": true,
		},
		{
			"name":    types.ComponentTasklist,
			"enabled": true,
		},
		{
			"name":    types.ComponentOptimize,
			"enabled": false,
		},
		{
			"name":    types.ComponentElasticsearch,
			"enabled": true,
		},
	}
}

// ValidateComponent validates a component configuration
func ValidateComponent(component map[string]interface{}) error {
	name, ok := component["name"].(string)
	if !ok {
		return utils.ErrInvalidComponent
	}

	// Check if component name is valid
	valid := false
	for _, validComponent := range types.ValidComponents {
		if name == validComponent {
			valid = true
			break
		}
	}
	if !valid {
		return utils.ErrInvalidComponent
	}

	return nil
}
