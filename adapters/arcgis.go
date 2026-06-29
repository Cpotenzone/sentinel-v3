package adapters

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Cpotenzone/sentinel-v3/models"
)

// ArcGISAdapter queries ArcGIS FeatureServer REST APIs.
type ArcGISAdapter struct{}

func (a *ArcGISAdapter) Platform() string { return "arcgis" }

func (a *ArcGISAdapter) Query(ctx context.Context, config models.DataSourceConfig, needles []string, identifiers *models.ResolvedIdentifiers) (*models.AdapterResult, error) {
	start := time.Now()
	result := &models.AdapterResult{Source: config}

	host := config.Host
	if !strings.HasPrefix(host, "http") {
		host = "https://" + host
	}

	// Build ArcGIS query URL
	endpoint := host
	if config.DatasetID != "" {
		endpoint = fmt.Sprintf("%s/%s/query", host, config.DatasetID)
	}

	// Build WHERE clause for ArcGIS
	col := sanitizeColumn(config.JoinColumn)
	var whereParts []string
	for _, needle := range needles[:min(3, len(needles))] {
		safe := strings.ReplaceAll(needle, "'", "''")
		whereParts = append(whereParts, fmt.Sprintf("UPPER(%s) LIKE UPPER('%%%s%%')", col, safe))
	}
	where := strings.Join(whereParts, " OR ")

	perPage := config.PerPage
	if perPage == 0 {
		perPage = 1000
	}

	params := url.Values{
		"where":             {where},
		"outFields":         {"*"},
		"f":                 {"json"},
		"resultOffset":      {"0"},
		"resultRecordCount": {fmt.Sprintf("%d", perPage)},
		"orderByFields":     {"OBJECTID"},
	}

	reqURL := endpoint + "?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		result.Error = err
		result.ErrorMsg = err.Error()
		result.Duration = time.Since(start)
		return result, nil
	}

	resp, err := SharedHTTPClient.Do(req)
	if err != nil {
		result.Error = err
		result.ErrorMsg = fmt.Sprintf("HTTP error: %v", err)
		result.Duration = time.Since(start)
		return result, nil
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		result.Error = fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body[:min(200, len(body))]))
		result.ErrorMsg = result.Error.Error()
		result.Duration = time.Since(start)
		return result, nil
	}

	var arcResponse struct {
		Features []struct {
			Attributes map[string]interface{} `json:"attributes"`
		} `json:"features"`
	}
	if err := json.Unmarshal(body, &arcResponse); err != nil {
		result.Error = fmt.Errorf("JSON parse error: %v", err)
		result.ErrorMsg = result.Error.Error()
		result.Duration = time.Since(start)
		return result, nil
	}

	for _, feat := range arcResponse.Features {
		recordID := ""
		if v, ok := feat.Attributes["OBJECTID"]; ok {
			recordID = fmt.Sprintf("%v", v)
		}
		result.Records = append(result.Records, models.EvidenceRecord{
			Source:          fmt.Sprintf("ArcGIS %s", config.Host),
			SourceRecordID:  recordID,
			Category:        config.Category,
			CitationURL:     endpoint,
			FetchedAt:       time.Now(),
			ExtractedFields: feat.Attributes,
			Confidence:      0.80,
			DatasetID:       config.DatasetID,
		})
	}

	result.TotalCount = len(result.Records)
	result.Duration = time.Since(start)
	return result, nil
}
