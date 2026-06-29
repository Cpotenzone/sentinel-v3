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

// Three11Adapter queries 311 service request APIs (NYC, Chicago, LA, etc.).
type Three11Adapter struct{}

func (a *Three11Adapter) Platform() string { return "three11" }

// Known 311 endpoints by city key.
var three11Sources = map[string]string{
	"new-york-ny": "https://data.cityofnewyork.us/resource/erm2-nwe9.json",
	"chicago-il":  "https://data.cityofchicago.org/resource/v6vf-nfxy.json",
	"los-angeles-ca": "https://data.lacity.org/resource/pvft-t9no.json",
}

func (a *Three11Adapter) Query(ctx context.Context, config models.DataSourceConfig, needles []string, identifiers *models.ResolvedIdentifiers) (*models.AdapterResult, error) {
	start := time.Now()
	result := &models.AdapterResult{Source: config}

	// Determine endpoint from config or city key
	endpoint := ""
	if config.Host != "" && config.DatasetID != "" {
		host := config.Host
		if !strings.HasPrefix(host, "http") {
			host = "https://" + host
		}
		endpoint = fmt.Sprintf("%s/resource/%s.json", host, config.DatasetID)
	}

	if endpoint == "" {
		result.ErrorMsg = "No 311 endpoint configured for this jurisdiction"
		result.Duration = time.Since(start)
		return result, nil
	}

	// Build query — 311 uses incident_address typically
	col := config.JoinColumn
	if col == "" {
		col = "incident_address"
	}

	var whereParts []string
	for _, needle := range needles[:min(3, len(needles))] {
		safe := strings.ReplaceAll(needle, "'", "''")
		whereParts = append(whereParts, fmt.Sprintf("UPPER(%s) LIKE UPPER('%%%s%%')", col, safe))
	}
	where := strings.Join(whereParts, " OR ")

	params := url.Values{
		"$where": {where},
		"$limit": {"200"},
		"$order": {":id"},
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
		result.ErrorMsg = fmt.Sprintf("311 HTTP error: %v", err)
		result.Duration = time.Since(start)
		return result, nil
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		result.Error = fmt.Errorf("311 HTTP %d: %s", resp.StatusCode, string(body[:min(200, len(body))]))
		result.ErrorMsg = result.Error.Error()
		result.Duration = time.Since(start)
		return result, nil
	}

	var rows []map[string]interface{}
	if err := json.Unmarshal(body, &rows); err != nil {
		result.Error = fmt.Errorf("311 JSON parse: %v", err)
		result.ErrorMsg = result.Error.Error()
		result.Duration = time.Since(start)
		return result, nil
	}

	for _, row := range rows {
		recordID := ""
		if v, ok := row["unique_key"]; ok {
			recordID = fmt.Sprintf("%v", v)
		} else if v, ok := row[":id"]; ok {
			recordID = fmt.Sprintf("%v", v)
		}
		result.Records = append(result.Records, models.EvidenceRecord{
			Source:          fmt.Sprintf("311 %s", config.Host),
			SourceRecordID:  recordID,
			Category:        "complaints",
			CitationURL:     endpoint,
			FetchedAt:       time.Now(),
			ExtractedFields: cleanFields(row),
			Confidence:      0.75,
			DatasetID:       config.DatasetID,
		})
	}

	result.TotalCount = len(result.Records)
	result.Duration = time.Since(start)
	return result, nil
}

// Resolve311Endpoint returns the 311 API endpoint for a city key.
func Resolve311Endpoint(cityKey string) string {
	return three11Sources[cityKey]
}
