package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Cpotenzone/sentinel-v3/adapters"
	"github.com/Cpotenzone/sentinel-v3/cache"
	"github.com/Cpotenzone/sentinel-v3/catalog"
	"github.com/Cpotenzone/sentinel-v3/models"
	"github.com/Cpotenzone/sentinel-v3/retry"
	"golang.org/x/sync/semaphore"
)

// Pipeline orchestrates the full data extraction flow.
type Pipeline struct {
	catalog    *catalog.Catalog
	cache      *cache.Cache
	retryQueue *retry.Queue
	sem        *semaphore.Weighted
}

// New creates a new Pipeline.
func New(cat *catalog.Catalog, c *cache.Cache, rq *retry.Queue) *Pipeline {
	return &Pipeline{
		catalog:    cat,
		cache:      c,
		retryQueue: rq,
		sem:        semaphore.NewWeighted(10), // Max 10 concurrent adapter calls
	}
}

// Execute runs the full pipeline for a tool call.
func (p *Pipeline) Execute(ctx context.Context, tool, address string) (*models.StructuredReport, error) {
	start := time.Now()

	// Step 1: Geocode (cached)
	geo, err := p.resolveAddress(ctx, address)
	if err != nil {
		return nil, fmt.Errorf("geocode failed: %w", err)
	}

	// Step 2: Resolve identifiers (BIN/BBL, cached)
	identifiers := p.resolveIdentifiers(ctx, geo)

	// Step 3: Get all sources for this jurisdiction
	allSources := p.catalog.ResolveSources(geo.City, geo.StateCode)
	if len(allSources) == 0 {
		return &models.StructuredReport{
			Success:      true,
			Address:      geo.FormattedAddress,
			Geocoded:     geo,
			ExecutiveSummary: fmt.Sprintf("No data sources cataloged for %s, %s", geo.City, geo.StateCode),
			CompleteTier: "no_coverage",
			GeneratedAt:  time.Now(),
		}, nil
	}

	// Step 4: Query planner — filter sources by tool
	sources := catalog.PlanQuery(tool, allSources)
	log.Printf("[%s] %s: %d sources (filtered from %d)", tool, address, len(sources), len(allSources))

	// Step 5: Build needles
	needles := expandNeedles(geo.FullStreet, address)

	// Step 6: Dispatch adapters in parallel (with partial result handling)
	results, failedSources := p.dispatch(ctx, sources, needles, identifiers)

	// Step 7: Deduplicate
	deduped := deduplicate(results)

	// Step 8: Schedule retries for failed sources
	retryScheduled := false
	if len(failedSources) > 0 {
		for _, fs := range failedSources {
			p.retryQueue.Enqueue(models.RetryJob{
				ID:          fmt.Sprintf("%s-%s-%s", tool, address, fs.DatasetID),
				Address:     address,
				Tool:        tool,
				Source:      findSourceConfig(sources, fs.DatasetID),
				Needles:     needles,
				Identifiers: identifiers,
				MaxAttempts: 5,
				NextRetry:   time.Now().Add(2 * time.Minute),
				CreatedAt:   time.Now(),
				LastError:   fs.Error,
			})
		}
		retryScheduled = true
	}

	// Step 9: Build structured output
	report := buildReport(geo, deduped, failedSources, tool, retryScheduled)
	report.GeneratedAt = time.Now()

	elapsed := time.Since(start)
	log.Printf("[%s] %s: completed in %.1fs — %d records, %d failed sources",
		tool, address, elapsed.Seconds(), report.TotalRecords, len(failedSources))

	return report, nil
}

// ExecuteRetry re-runs a single failed source query.
func (p *Pipeline) ExecuteRetry(ctx context.Context, job models.RetryJob) (*models.AdapterResult, error) {
	adapter, err := adapters.Get(job.Source.Platform)
	if err != nil {
		return nil, err
	}
	return adapter.Query(ctx, job.Source, job.Needles, job.Identifiers)
}

// --- Internal methods ---

func (p *Pipeline) resolveAddress(ctx context.Context, address string) (*models.GeocodedAddress, error) {
	cacheKey := "geo:" + address
	if cached, ok := p.cache.Get(cacheKey); ok {
		return cached.(*models.GeocodedAddress), nil
	}

	apiKey := os.Getenv("GOOGLE_MAPS_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("GOOGLE_MAPS_API_KEY not set")
	}

	params := url.Values{"address": {address}, "key": {apiKey}}
	reqURL := "https://maps.googleapis.com/maps/api/geocode/json?" + params.Encode()

	req, _ := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	resp, err := adapters.SharedHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("geocode HTTP: %w", err)
	}
	defer resp.Body.Close()

	var geoResp struct {
		Results []struct {
			FormattedAddress string `json:"formatted_address"`
			Geometry         struct {
				Location struct {
					Lat float64 `json:"lat"`
					Lng float64 `json:"lng"`
				} `json:"location"`
			} `json:"geometry"`
			AddressComponents []struct {
				LongName  string   `json:"long_name"`
				ShortName string   `json:"short_name"`
				Types     []string `json:"types"`
			} `json:"address_components"`
		} `json:"results"`
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&geoResp); err != nil {
		return nil, fmt.Errorf("geocode parse: %w", err)
	}
	if geoResp.Status != "OK" || len(geoResp.Results) == 0 {
		return nil, fmt.Errorf("geocode failed: status=%s", geoResp.Status)
	}

	r := geoResp.Results[0]
	geo := &models.GeocodedAddress{
		FormattedAddress: r.FormattedAddress,
		Lat:              r.Geometry.Location.Lat,
		Lng:              r.Geometry.Location.Lng,
	}

	// Extract components
	for _, comp := range r.AddressComponents {
		for _, t := range comp.Types {
			switch t {
			case "locality":
				geo.City = comp.LongName
			case "administrative_area_level_2":
				geo.County = comp.LongName
			case "administrative_area_level_1":
				geo.State = comp.LongName
				geo.StateCode = comp.ShortName
			case "postal_code":
				geo.ZipCode = comp.LongName
			case "route":
				geo.FullStreet = comp.LongName
			}
		}
	}

	// Derive full street from formatted address if not found
	if geo.FullStreet == "" {
		parts := strings.Split(r.FormattedAddress, ",")
		if len(parts) > 0 {
			geo.FullStreet = strings.TrimSpace(parts[0])
		}
	}

	p.cache.Set(cacheKey, geo)
	return geo, nil
}

func (p *Pipeline) resolveIdentifiers(ctx context.Context, geo *models.GeocodedAddress) *models.ResolvedIdentifiers {
	// NYC: resolve BIN/BBL via DOB permits
	if geo.City == "New York" || geo.City == "New York City" {
		return p.resolveNYCIdentifiers(ctx, geo)
	}

	// Jersey City: resolve block/lot via spatial parcel query
	if geo.City == "Jersey City" && geo.StateCode == "NJ" {
		return p.resolveJCIdentifiers(ctx, geo)
	}

	return nil
}

func (p *Pipeline) resolveJCIdentifiers(ctx context.Context, geo *models.GeocodedAddress) *models.ResolvedIdentifiers {
	cacheKey := "ids:" + geo.FormattedAddress
	if cached, ok := p.cache.Get(cacheKey); ok {
		return cached.(*models.ResolvedIdentifiers)
	}

	ids := &models.ResolvedIdentifiers{}

	// Use JC parcels dataset with spatial query (within_distance of geocoded point)
	odsWhere := fmt.Sprintf("within_distance(geo_point_2d,geom'POINT(%f %f)',200m)", geo.Lng, geo.Lat)
	params := url.Values{
		"limit": {"1"},
		"where": {odsWhere},
	}
	endpoint := "https://data.jerseycitynj.gov/api/explore/v2.1/catalog/datasets/jersey-city-parcels/records?" + params.Encode()

	req, _ := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	resp, err := adapters.SharedHTTPClient.Do(req)
	if err != nil {
		return ids
	}
	defer resp.Body.Close()

	var odsResp struct {
		TotalCount int `json:"total_count"`
		Results    []struct {
			Block string `json:"block"`
			Lot   string `json:"lot"`
		} `json:"results"`
	}
	json.NewDecoder(resp.Body).Decode(&odsResp)

	if len(odsResp.Results) > 0 {
		ids.BBL = odsResp.Results[0].Block // Reuse BBL field for block
		ids.BIN = odsResp.Results[0].Lot   // Reuse BIN field for lot
		log.Printf("Resolved JC identifiers for %s: Block=%s Lot=%s", geo.FormattedAddress, ids.BBL, ids.BIN)
		p.cache.Set(cacheKey, ids)
	}

	return ids
}

func (p *Pipeline) resolveNYCIdentifiers(ctx context.Context, geo *models.GeocodedAddress) *models.ResolvedIdentifiers {
	cacheKey := "ids:" + geo.FormattedAddress
	if cached, ok := p.cache.Get(cacheKey); ok {
		return cached.(*models.ResolvedIdentifiers)
	}

	ids := &models.ResolvedIdentifiers{}

	// Extract house number from formatted address (e.g., "110 Central Park S" → "110")
	street := geo.FullStreet
	if street == "" {
		return ids
	}

	// Parse house number from the street address
	houseNum := ""
	parts := strings.Fields(street)
	if len(parts) > 0 && isNumericID(parts[0]) {
		houseNum = parts[0]
	} else {
		// Try from formatted address
		fmtParts := strings.Fields(geo.FormattedAddress)
		if len(fmtParts) > 0 && isNumericID(fmtParts[0]) {
			houseNum = fmtParts[0]
		}
	}

	// Query DOB permit issuance for BIN/BBL — this is the authoritative source
	// PLUTO doesn't always have the address in a matchable format
	var endpoint string
	if houseNum != "" {
		// Use DOB permits (ipu4-2q9a) which has house__ and street_name columns
		streetName := ""
		if len(parts) > 1 {
			streetName = strings.ToUpper(strings.Join(parts[1:], " "))
		}
		// Normalize: South→SOUTH, etc (DOB stores full words uppercase)
		streetName = strings.ReplaceAll(streetName, " S ", " SOUTH ")
		if strings.HasSuffix(streetName, " S") {
			streetName = strings.TrimSuffix(streetName, " S") + " SOUTH"
		}
		streetName = strings.ReplaceAll(streetName, " N ", " NORTH ")
		streetName = strings.ReplaceAll(streetName, " E ", " EAST ")
		streetName = strings.ReplaceAll(streetName, " W ", " WEST ")
		
		endpoint = fmt.Sprintf(
			"https://data.cityofnewyork.us/resource/ipu4-2q9a.json?$where=house__='%s' AND street_name='%s'&$limit=1&$select=bin__,bbl",
			url.QueryEscape(houseNum),
			url.QueryEscape(streetName),
		)
	} else {
		// Fallback to street LIKE (less precise)
		endpoint = fmt.Sprintf(
			"https://data.cityofnewyork.us/resource/ipu4-2q9a.json?$where=UPPER(street_name) LIKE UPPER('%%%s%%')&$limit=1&$select=bin__,bbl",
			url.QueryEscape(street),
		)
	}

	req, _ := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	resp, err := adapters.SharedHTTPClient.Do(req)
	if err != nil {
		return ids
	}
	defer resp.Body.Close()

	var rows []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&rows)
	if len(rows) > 0 {
		if bbl, ok := rows[0]["bbl"]; ok {
			ids.BBL = fmt.Sprintf("%v", bbl)
		}
		// DOB permits uses "bin__" not "bin"
		if bin, ok := rows[0]["bin__"]; ok {
			ids.BIN = fmt.Sprintf("%v", bin)
		} else if bin, ok := rows[0]["bin"]; ok {
			ids.BIN = fmt.Sprintf("%v", bin)
		}
	}

	if ids.BIN != "" || ids.BBL != "" {
		log.Printf("Resolved identifiers for %s: BIN=%s BBL=%s", geo.FormattedAddress, ids.BIN, ids.BBL)
		p.cache.Set(cacheKey, ids)
	}
	return ids
}

func (p *Pipeline) dispatch(ctx context.Context, sources []models.DataSourceConfig, needles []string, identifiers *models.ResolvedIdentifiers) ([]models.EvidenceRecord, []models.FailedSource) {
	var mu sync.Mutex
	var allRecords []models.EvidenceRecord
	var failed []models.FailedSource

	var wg sync.WaitGroup
	for _, src := range sources {
		wg.Add(1)
		go func(s models.DataSourceConfig) {
			defer wg.Done()

			// Acquire semaphore
			if err := p.sem.Acquire(ctx, 1); err != nil {
				return
			}
			defer p.sem.Release(1)

			// Check cache first
			cacheKey := adapterCacheKey(s, needles)
			if cached, ok := p.cache.Get(cacheKey); ok {
				mu.Lock()
				allRecords = append(allRecords, cached.([]models.EvidenceRecord)...)
				mu.Unlock()
				return
			}

			// Get adapter
			adapter, err := adapters.Get(s.Platform)
			if err != nil {
				mu.Lock()
				failed = append(failed, models.FailedSource{
					Source:    s.Label,
					DatasetID: s.DatasetID,
					Error:     err.Error(),
					LastTry:   time.Now(),
					Category:  s.Category,
				})
				mu.Unlock()
				return
			}

			// Execute with retry (up to 2 immediate retries for transient errors)
			var result *models.AdapterResult
			for attempt := 0; attempt < 3; attempt++ {
				result, _ = adapter.Query(ctx, s, needles, identifiers)
				if result != nil && result.Error == nil {
					break
				}
				if attempt < 2 {
					time.Sleep(time.Duration(attempt+1) * 2 * time.Second)
				}
			}

			mu.Lock()
			defer mu.Unlock()

			if result == nil || (result.Error != nil && len(result.Records) == 0) {
				errMsg := "unknown error"
				if result != nil && result.ErrorMsg != "" {
					errMsg = result.ErrorMsg
				}
				failed = append(failed, models.FailedSource{
					Source:    s.Label,
					DatasetID: s.DatasetID,
					Error:     errMsg,
					Retries:   3,
					LastTry:   time.Now(),
					Category:  s.Category,
				})
				return
			}

			// Cache successful results
			if result.Error == nil {
				p.cache.Set(cacheKey, result.Records)
			}
			allRecords = append(allRecords, result.Records...)
		}(src)
	}

	wg.Wait()
	return allRecords, failed
}

func adapterCacheKey(config models.DataSourceConfig, needles []string) string {
	data := fmt.Sprintf("%s:%s:%s", config.Platform, config.DatasetID, strings.Join(needles, "|"))
	hash := sha256.Sum256([]byte(data))
	return fmt.Sprintf("adapter:%x", hash[:8])
}

func expandNeedles(fullStreet, rawAddress string) []string {
	var needles []string

	// First needle: the full street address with house number (most specific)
	if fullStreet != "" {
		needles = append(needles, fullStreet)
	}

	// Add raw address street portion (may have house number)
	raw := strings.Split(rawAddress, ",")[0]
	raw = strings.TrimSpace(raw)
	if raw != "" && raw != fullStreet {
		needles = append(needles, raw)
	}

	// Add abbreviated version
	abbreviated := strings.ReplaceAll(fullStreet, "South", "S")
	abbreviated = strings.ReplaceAll(abbreviated, "North", "N")
	abbreviated = strings.ReplaceAll(abbreviated, "East", "E")
	abbreviated = strings.ReplaceAll(abbreviated, "West", "W")
	abbreviated = strings.ReplaceAll(abbreviated, "Avenue", "Ave")
	abbreviated = strings.ReplaceAll(abbreviated, "Street", "St")
	abbreviated = strings.ReplaceAll(abbreviated, "Boulevard", "Blvd")
	if abbreviated != fullStreet && abbreviated != "" {
		needles = append(needles, abbreviated)
	}

	// DO NOT add just the street name without house number —
	// that causes corridor-wide matching (the scoping bug)
	return needles
}

func deduplicate(records []models.EvidenceRecord) []models.EvidenceRecord {
	seen := make(map[string]bool)
	var unique []models.EvidenceRecord
	for _, r := range records {
		key := r.Source + ":" + r.SourceRecordID
		if r.SourceRecordID == "" {
			// No record ID — use hash of extracted fields
			data, _ := json.Marshal(r.ExtractedFields)
			key = r.Source + ":" + fmt.Sprintf("%x", sha256.Sum256(data))[:16]
		}
		if !seen[key] {
			seen[key] = true
			unique = append(unique, r)
		}
	}
	return unique
}

func findSourceConfig(sources []models.DataSourceConfig, datasetID string) models.DataSourceConfig {
	for _, s := range sources {
		if s.DatasetID == datasetID {
			return s
		}
	}
	return models.DataSourceConfig{}
}

func buildReport(geo *models.GeocodedAddress, records []models.EvidenceRecord, failed []models.FailedSource, tool string, retryScheduled bool) *models.StructuredReport {
	// Group by category
	groups := make(map[string][]models.EvidenceRecord)
	for _, r := range records {
		groups[r.Category] = append(groups[r.Category], r)
	}

	var sections []models.ReportSection
	var summaryParts []string
	sourcesSet := make(map[string]bool)

	for cat, recs := range groups {
		section := models.ReportSection{
			Title:       strings.ReplaceAll(strings.Title(strings.ReplaceAll(cat, "_", " ")), "_", " "),
			Category:    cat,
			Records:     recs,
			RecordCount: len(recs),
		}
		// Build citations
		citSet := make(map[string]bool)
		for _, r := range recs {
			if r.CitationURL != "" && !citSet[r.CitationURL] {
				section.Citations = append(section.Citations, r.CitationURL)
				citSet[r.CitationURL] = true
			}
			sourcesSet[r.Source] = true
		}
		sections = append(sections, section)
		summaryParts = append(summaryParts, fmt.Sprintf("- %s: %d records", section.Title, len(recs)))
	}

	// Build sources list
	var sources []string
	for s := range sourcesSet {
		sources = append(sources, s)
	}

	// Determine completeness tier
	tier := "complete"
	if len(failed) > 0 && len(records) > 0 {
		tier = "partial"
	} else if len(failed) > 0 && len(records) == 0 {
		tier = "degraded"
	}

	summary := fmt.Sprintf("SENTINEL %s for %s:\n%s\n\nTotal records: %d\nFailed sources: %d",
		tool, geo.FormattedAddress, strings.Join(summaryParts, "\n"), len(records), len(failed))

	return &models.StructuredReport{
		Success:          true,
		Address:          geo.FormattedAddress,
		Geocoded:         geo,
		ExecutiveSummary: summary,
		Sections:         sections,
		SourcesConsulted: sources,
		FailedSources:    failed,
		TotalRecords:     len(records),
		Confidence:       calculateConfidence(len(records), len(failed)),
		CompleteTier:     tier,
		RetryScheduled:   retryScheduled,
	}
}

func calculateConfidence(records, failures int) float64 {
	total := records + failures
	if total == 0 {
		return 0
	}
	return float64(records) / float64(total)
}

func isNumericID(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0
}
