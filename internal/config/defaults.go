package config

import (
        "github.com/aitasadduq/camunda-backup-dr/internal/utils"
        "github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

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