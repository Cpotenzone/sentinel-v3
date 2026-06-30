package catalog

import (
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/Cpotenzone/sentinel-v3/models"
)

//go:embed catalog.json
var catalogFS embed.FS

// Catalog holds the full jurisdiction data source catalog.
type Catalog struct {
	Cities    map[string]CityEntry `json:"cities"`
	Statewide map[string]CityEntry `json:"statewide"`
}

// CityEntry holds sources for a jurisdiction.
type CityEntry struct {
	Sources []SourceEntry `json:"sources"`
}

// SourceEntry is a top-level source group in the catalog.
type SourceEntry struct {
	Name       string         `json:"name"`
	SourceType string         `json:"source_type"`
	Platform   string         `json:"platform"`
	Host       string         `json:"host"`
	Datasets   []DatasetEntry `json:"datasets"`
	PerPage    int            `json:"per_page"`
	MaxPages   int            `json:"max_pages"`
}

// DatasetEntry is an individual dataset within a source.
type DatasetEntry struct {
	ID         string `json:"id"`
	JoinColumn string `json:"join_column"`
	JoinKey    string `json:"join_key"`
	Category   string `json:"category"` // Explicit category per dataset (optional, falls back to source_type)
	Label      string `json:"label"`
	LayerID    *int   `json:"layer_id"` // For ArcGIS layers
}

// ToolCategories maps each tool to the categories it should query.
// nil means query ALL categories.
var ToolCategories = map[string][]string{
	"sentinel_research":      nil,
	"sentinel_permits":       {"building_permits", "certificate_of_occupancy", "facades"},
	"sentinel_environmental": {"environmental", "fire_inspections", "health_inspections", "environmental_compliance"},
	"sentinel_financial":     {"tax_assessment", "property_records"},
	"sentinel_ownership":     {"property_records"},
	"sentinel_311":           {"complaints", "open_data"},
}

var loaded *Catalog

// Load reads and parses the embedded catalog.json.
func Load() *Catalog {
	if loaded != nil {
		return loaded
	}
	data, err := catalogFS.ReadFile("catalog.json")
	if err != nil {
		log.Printf("WARNING: catalog.json not found, using empty catalog: %v", err)
		loaded = &Catalog{
			Cities:    make(map[string]CityEntry),
			Statewide: make(map[string]CityEntry),
		}
		return loaded
	}
	var cat Catalog
	if err := json.Unmarshal(data, &cat); err != nil {
		log.Fatalf("Failed to parse catalog.json: %v", err)
	}
	loaded = &cat
	return loaded
}

// MetroFallback maps suburb city keys to the nearest covered metro's sources.
// NOTE: Only use for cities within the SAME jurisdiction's open data portal.
// Jersey City is NOT on NYC's portal — it uses NJ statewide data.
var MetroFallback = map[string]string{
	// NYC Boroughs (all on data.cityofnewyork.us)
	"brooklyn-ny":      "new-york-ny",
	"queens-ny":        "new-york-ny",
	"bronx-ny":         "new-york-ny",
	"staten-island-ny": "new-york-ny",
	"yonkers-ny":       "new-york-ny",
}

// ResolveSources returns all DataSourceConfigs for a given city/state.
func (c *Catalog) ResolveSources(city, stateCode string) []models.DataSourceConfig {
	key := slugify(city + " " + stateCode)
	var configs []models.DataSourceConfig

	// City-level sources
	if entry, ok := c.Cities[key]; ok {
		configs = append(configs, parseSources(entry.Sources)...)
	} else {
		// Metro fallback: check if this suburb maps to a covered metro
		if metroKey, ok := MetroFallback[key]; ok {
			if entry, ok := c.Cities[metroKey]; ok {
				configs = append(configs, parseSources(entry.Sources)...)
			}
		}
	}

	// Statewide sources
	stateKey := strings.ToUpper(stateCode)
	if entry, ok := c.Statewide[stateKey]; ok {
		configs = append(configs, parseSources(entry.Sources)...)
	}

	return configs
}

// PlanQuery filters sources based on the tool being called.
func PlanQuery(tool string, allSources []models.DataSourceConfig) []models.DataSourceConfig {
	cats := ToolCategories[tool]
	if cats == nil {
		return allSources // sentinel_research gets everything
	}
	catSet := make(map[string]bool, len(cats))
	for _, c := range cats {
		catSet[c] = true
	}
	var filtered []models.DataSourceConfig
	for _, s := range allSources {
		if catSet[s.Category] {
			filtered = append(filtered, s)
		}
	}
	return filtered
}

func parseSources(sources []SourceEntry) []models.DataSourceConfig {
	var configs []models.DataSourceConfig
	for _, src := range sources {
		platform := strings.ToLower(src.Platform)
		if platform == "" {
			continue
		}
		for _, ds := range src.Datasets {
			// Skip datasets with no ID (empty entries)
			if ds.ID == "" && ds.LayerID == nil {
				continue
			}
			category := ds.Category
			if category == "" {
				category = src.SourceType // Fall back to source-level type
			}
			label := ds.Label
			if label == "" {
				label = src.Name
				if ds.ID != "" {
					label += " (" + ds.ID + ")"
				}
			}
			joinCol := ds.JoinColumn
			if joinCol == "" {
				joinCol = "address" // last resort default
			}
			perPage := src.PerPage
			if perPage == 0 {
				perPage = 1000
			}
			maxPages := src.MaxPages
			if maxPages == 0 {
				maxPages = 10
			}
			datasetID := ds.ID
			if datasetID == "" && ds.LayerID != nil {
				datasetID = fmt.Sprintf("%d", *ds.LayerID)
			}
			configs = append(configs, models.DataSourceConfig{
				Platform:   platform,
				Host:       src.Host,
				DatasetID:  datasetID,
				JoinColumn: joinCol,
				JoinKey:    ds.JoinKey,
				Category:   category,
				Label:      label,
				PerPage:    perPage,
				MaxPages:   maxPages,
			})
		}
	}
	return configs
}

func slugify(text string) string {
	text = strings.ToLower(strings.TrimSpace(text))
	text = strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' {
			return r
		}
		if r == ' ' {
			return '-'
		}
		return -1
	}, text)
	// Collapse multiple hyphens
	for strings.Contains(text, "--") {
		text = strings.ReplaceAll(text, "--", "-")
	}
	return strings.Trim(text, "-")
}
