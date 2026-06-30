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

// OpenDataSoftAdapter queries OpenDataSoft (ODS) portals like data.jerseycitynj.gov.
// ODS uses a different API than Socrata: /api/explore/v2.1/catalog/datasets/{id}/records
type OpenDataSoftAdapter struct{}

func (a *OpenDataSoftAdapter) Platform() string { return "opendatasoft" }

func (a *OpenDataSoftAdapter) Query(ctx context.Context, config models.DataSourceConfig, needles []string, identifiers *models.ResolvedIdentifiers) (*models.AdapterResult, error) {
	start := time.Now()
	result := &models.AdapterResult{Source: config}

	host := config.Host
	if !strings.HasPrefix(host, "http") {
		host = "https://" + host
	}

	// ODS API endpoint
	endpoint := fmt.Sprintf("%s/api/explore/v2.1/catalog/datasets/%s/records", host, config.DatasetID)

	// Build ODS where clause (uses different syntax than SoQL)
	col := config.JoinColumn
	var whereParts []string
	for _, needle := range needles[:min(3, len(needles))] {
		safe := strings.ReplaceAll(needle, "'", "\\'")
		// ODS uses: search(field, "value") or field like "value"
		whereParts = append(whereParts, fmt.Sprintf("search(%s, \"%s\")", col, safe))
	}
	where := strings.Join(whereParts, " OR ")

	perPage := config.PerPage
	if perPage == 0 {
		perPage = 100
	}
	maxPages := config.MaxPages
	if maxPages == 0 {
		maxPages = 5
	}

	var allRecords []models.EvidenceRecord
	for page := 0; page < maxPages; page++ {
		params := url.Values{
			"where": {where},
			"limit": {fmt.Sprintf("%d", perPage)},
			"offset": {fmt.Sprintf("%d", page*perPage)},
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
			result.Records = allRecords
			return result, nil
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != 200 {
			if page == 0 {
				result.Error = fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body[:min(200, len(body))]))
				result.ErrorMsg = result.Error.Error()
				result.Duration = time.Since(start)
				return result, nil
			}
			break
		}

		// ODS response format: {"total_count": N, "results": [{...}, ...]}
		var odsResp struct {
			TotalCount int                      `json:"total_count"`
			Results    []map[string]interface{} `json:"results"`
		}
		if err := json.Unmarshal(body, &odsResp); err != nil {
			if page == 0 {
				result.Error = fmt.Errorf("JSON parse: %v", err)
				result.ErrorMsg = result.Error.Error()
			}
			break
		}

		if len(odsResp.Results) == 0 {
			break
		}

		for _, row := range odsResp.Results {
			recordID := ""
			if v, ok := row["recordid"]; ok {
				recordID = fmt.Sprintf("%v", v)
			}
			allRecords = append(allRecords, models.EvidenceRecord{
				Source:          fmt.Sprintf("ODS %s %s", config.Host, config.DatasetID),
				SourceRecordID:  recordID,
				Category:        config.Category,
				CitationURL:     fmt.Sprintf("%s/explore/dataset/%s/", host, config.DatasetID),
				FetchedAt:       time.Now(),
				ExtractedFields: cleanFields(row),
				Confidence:      0.80,
				DatasetID:       config.DatasetID,
			})
		}

		if len(odsResp.Results) < perPage {
			break
		}
	}

	result.Records = allRecords
	result.TotalCount = len(allRecords)
	result.Duration = time.Since(start)
	return result, nil
}

func init() {
	Register(&OpenDataSoftAdapter{})
}
