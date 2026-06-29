package adapters

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/Cpotenzone/sentinel-v3/models"
)

// Adapter is the interface for all data source adapters.
type Adapter interface {
	// Query fetches records from a data source matching the given needles.
	Query(ctx context.Context, config models.DataSourceConfig, needles []string, identifiers *models.ResolvedIdentifiers) (*models.AdapterResult, error)
	// Platform returns the adapter's platform identifier.
	Platform() string
}

// Registry maps platform names to adapter instances.
var Registry = map[string]Adapter{}

// SharedHTTPClient is a connection-pooled HTTP client for all adapters.
var SharedHTTPClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     90 * time.Second,
	},
}

// Register adds an adapter to the registry.
func Register(a Adapter) {
	Registry[a.Platform()] = a
}

// Get returns the adapter for a platform, or an error if not found.
func Get(platform string) (Adapter, error) {
	a, ok := Registry[platform]
	if !ok {
		return nil, fmt.Errorf("no adapter registered for platform: %s", platform)
	}
	return a, nil
}

func init() {
	Register(&SocrataAdapter{})
	Register(&ArcGISAdapter{})
	Register(&Three11Adapter{})
}
