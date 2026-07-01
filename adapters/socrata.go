package adapters

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Cpotenzone/sentinel-v3/models"
)

// SocrataAdapter queries Socrata open data portals via SODA API (SoQL).
type SocrataAdapter struct{}

func (a *SocrataAdapter) Platform() string { return "socrata" }

func (a *SocrataAdapter) Query(ctx context.Context, config models.DataSourceConfig, needles []string, identifiers *models.ResolvedIdentifiers) (*models.AdapterResult, error) {
	start := time.Now()
	result := &models.AdapterResult{Source: config}

	host := config.Host
	if !strings.HasPrefix(host, "http") {
		host = "https://" + host
	}
	endpoint := fmt.Sprintf("%s/resource/%s.json", host, config.DatasetID)

	// Determine effective needles based on join key
	// CRITICAL: When BIN/BBL identifiers are available and the dataset supports them,
	// use exact ID match instead of street name LIKE (which pulls entire corridors)
	effectiveNeedles := needles
	useExactMatch := false
	effectiveColumn := config.JoinColumn

	if identifiers != nil {
		jk := strings.ToLower(config.JoinKey)
		jc := strings.ToLower(config.JoinColumn)

		switch {
		// BIN-based joins: exact match on BIN column
		case identifiers.BIN != "" && (jk == "bin" || jc == "bin" || jc == "bin__" || jc == "bin_number"):
			effectiveNeedles = []string{identifiers.BIN}
			useExactMatch = true
			// Use the actual column that holds BIN
			if jc == "bin" || jc == "bin__" || jc == "bin_number" {
				effectiveColumn = config.JoinColumn
			}

		// BBL-based joins: exact match on BBL column
		case identifiers.BBL != "" && (jk == "bbl" || jk == "block" || jc == "bbl" || jc == "bble" || jc == "block"):
			effectiveNeedles = []string{identifiers.BBL}
			useExactMatch = true

		// APN-based joins
		case identifiers.APN != "" && (jk == "apn" || jc == "apn" || jc == "ain"):
			effectiveNeedles = []string{identifiers.APN}
			useExactMatch = true

		// Dataset has join_key=bin but column is "street_name" — 
		// This means the dataset CAN be queried by BIN on a different column.
		// Look for common BIN/BBL column names in the dataset.
		case identifiers.BIN != "" && jk == "bin":
			effectiveNeedles = []string{identifiers.BIN}
			effectiveColumn = "bin"  // Override: query the bin column directly
			useExactMatch = true

		case identifiers.BBL != "" && jk == "bbl":
			effectiveNeedles = []string{identifiers.BBL}
			effectiveColumn = "bbl"  // Override: query the bbl column directly
			useExactMatch = true
		}
	}

	// Build SoQL WHERE clause
	var where string
	if useExactMatch {
		// Exact match for BIN/BBL — no LIKE, no UPPER
		col := sanitizeColumn(effectiveColumn)
		var clauses []string
		for _, needle := range effectiveNeedles {
			safe := strings.ReplaceAll(needle, "'", "''")
			clauses = append(clauses, fmt.Sprintf("`%s` = '%s'", col, safe))
		}
		where = strings.Join(clauses, " OR ")
	} else {
		where = buildWhereClause(config.JoinColumn, effectiveNeedles)
	}

	// Append extra_params as additional WHERE filters (e.g., muniname='JERSEY CITY')
	if config.Extra != nil {
		for k, v := range config.Extra {
			safe := strings.ReplaceAll(v, "'", "''")
			where = fmt.Sprintf("(%s) AND `%s` = '%s'", where, sanitizeColumn(k), safe)
		}
	}

	// Headers
	headers := map[string]string{}
	if token := os.Getenv("NYC_SODA_APP_TOKEN"); token != "" {
		headers["X-App-Token"] = token
	}

	// Paginated fetch
	perPage := config.PerPage
	if perPage == 0 {
		perPage = 1000
	}
	maxPages := config.MaxPages
	if maxPages == 0 {
		maxPages = 10
	}

	var allRecords []models.EvidenceRecord
	for page := 0; page < maxPages; page++ {
		offset := page * perPage
		params := url.Values{
			"$where":  {where},
			"$limit":  {fmt.Sprintf("%d", perPage)},
			"$offset": {fmt.Sprintf("%d", offset)},
			"$order":  {":id DESC"},
		}

		reqURL := endpoint + "?" + params.Encode()
		req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
		if err != nil {
			result.Error = err
			result.ErrorMsg = err.Error()
			result.Duration = time.Since(start)
			return result, nil // Return result with error, don't fail the whole pipeline
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}

		resp, err := SharedHTTPClient.Do(req)
		if err != nil {
			result.Error = err
			result.ErrorMsg = fmt.Sprintf("HTTP error on page %d: %v", page, err)
			result.Duration = time.Since(start)
			// Return what we have so far (partial)
			result.Records = allRecords
			return result, nil
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != 200 {
			if page == 0 {
				result.Error = fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body[:min(200, len(body))]))
				result.ErrorMsg = result.Error.Error()
				result.Duration = time.Since(start)
				return result, nil
			}
			break // Got some data, stop pagination
		}

		var rows []map[string]interface{}
		if err := json.Unmarshal(body, &rows); err != nil {
			if page == 0 {
				result.Error = fmt.Errorf("JSON parse error: %v", err)
				result.ErrorMsg = result.Error.Error()
			}
			break
		}

		if len(rows) == 0 {
			break
		}

		for _, row := range rows {
			recordID := extractRecordID(row)
			allRecords = append(allRecords, models.EvidenceRecord{
				Source:          fmt.Sprintf("Socrata %s %s", config.Host, config.DatasetID),
				SourceRecordID:  recordID,
				Category:        config.Category,
				CitationURL:     buildCitationURL(config, recordID),
				FetchedAt:       time.Now(),
				ExtractedFields: cleanFields(row),
				Confidence:      0.85,
				DatasetID:       config.DatasetID,
			})
		}

		if len(rows) < perPage {
			break // Last page
		}
	}

	result.Records = allRecords
	result.TotalCount = len(allRecords)
	result.Duration = time.Since(start)
	return result, nil
}

func buildWhereClause(joinColumn string, needles []string) string {
	col := sanitizeColumn(joinColumn)
	var clauses []string
	for _, needle := range needles {
		safe := strings.ReplaceAll(needle, "'", "''")
		// Use exact match for numeric IDs (BIN/BBL)
		if isNumericID(needle) && isIDColumn(col) {
			clauses = append(clauses, fmt.Sprintf("`%s` = '%s'", col, safe))
		} else {
			clauses = append(clauses, fmt.Sprintf("UPPER(`%s`) LIKE UPPER('%%%s%%')", col, safe))
		}
	}
	return strings.Join(clauses, " OR ")
}

func sanitizeColumn(col string) string {
	// Remove dangerous characters from column names
	clean := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == ' ' {
			return r
		}
		return -1
	}, col)
	return strings.TrimSpace(clean)
}

func isNumericID(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0
}

func isIDColumn(col string) bool {
	lower := strings.ToLower(col)
	idCols := []string{"bin", "bin__", "bbl", "bble", "apn", "ain", "bin_number"}
	for _, id := range idCols {
		if lower == id {
			return true
		}
	}
	return false
}

func extractRecordID(row map[string]interface{}) string {
	for _, key := range []string{":id", "id", "OBJECTID", "objectid"} {
		if v, ok := row[key]; ok {
			return fmt.Sprintf("%v", v)
		}
	}
	return ""
}

func buildCitationURL(config models.DataSourceConfig, recordID string) string {
	host := config.Host
	if !strings.HasPrefix(host, "http") {
		host = "https://" + host
	}
	if recordID != "" {
		return fmt.Sprintf("%s/resource/%s/%s", host, config.DatasetID, recordID)
	}
	return fmt.Sprintf("%s/resource/%s", host, config.DatasetID)
}

func cleanFields(row map[string]interface{}) map[string]interface{} {
	fields := make(map[string]interface{}, len(row))
	for k, v := range row {
		// Skip internal/noisy fields
		if strings.HasPrefix(k, ":") || k == "the_geom" || k == "geocoded_column" || k == "location" {
			continue
		}
		if v != nil && v != "" {
			fields[k] = v
		}
	}
	return fields
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
