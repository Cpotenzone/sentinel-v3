package models

import "time"

// EvidenceRecord represents a single fact extracted from a public data source.
// Every field is directly sourced — no AI generation.
type EvidenceRecord struct {
	Source          string                 `json:"source"`           // e.g. "Socrata data.cityofnewyork.us ic3t-wcy2"
	SourceRecordID  string                 `json:"source_record_id"` // Unique ID within the source dataset
	Category        string                 `json:"category"`         // e.g. "building_permits", "violations"
	CitationURL     string                 `json:"citation_url"`     // Direct link to the source record
	FetchedAt       time.Time              `json:"fetched_at"`       // When this record was retrieved
	ExtractedFields map[string]interface{} `json:"extracted_fields"` // All fields from the source record
	Confidence      float64                `json:"confidence"`       // Data quality confidence (0-1)
	DatasetID       string                 `json:"dataset_id"`       // Source dataset identifier
}

// DataSourceConfig defines a single queryable dataset in the catalog.
type DataSourceConfig struct {
	Platform   string            `json:"platform"`    // "socrata", "arcgis", "three11"
	Host       string            `json:"host"`        // e.g. "data.cityofnewyork.us"
	DatasetID  string            `json:"dataset_id"`  // e.g. "ic3t-wcy2"
	JoinColumn string            `json:"join_column"` // Column to query against (address, BIN, BBL)
	JoinKey    string            `json:"join_key"`    // Type of join: "address", "bin", "bbl", "apn"
	Category   string            `json:"category"`    // e.g. "building_permits"
	Label      string            `json:"label"`       // Human-readable name
	PerPage    int               `json:"per_page"`    // Records per page (default 1000)
	MaxPages   int               `json:"max_pages"`   // Max pages to fetch (default 10)
	Extra      map[string]string `json:"extra_params"`
}

// StructuredReport is the final output from a sentinel tool call.
type StructuredReport struct {
	Success          bool              `json:"success"`
	Address          string            `json:"address"`
	Geocoded         *GeocodedAddress  `json:"geocoded,omitempty"`
	ExecutiveSummary string            `json:"executive_summary"`
	Sections         []ReportSection   `json:"sections"`
	SourcesConsulted []string          `json:"sources_consulted"`
	FailedSources    []FailedSource    `json:"failed_sources,omitempty"`
	TotalRecords     int               `json:"total_records"`
	Confidence       float64           `json:"confidence"`
	CompleteTier     string            `json:"completeness_tier"` // "complete", "partial", "degraded"
	GeneratedAt      time.Time         `json:"generated_at"`
	RetryScheduled   bool              `json:"retry_scheduled,omitempty"`
	Warnings         []string          `json:"warnings,omitempty"`
}

// ReportSection groups evidence by category.
type ReportSection struct {
	Title         string           `json:"title"`
	Category      string           `json:"category"`
	Records       []EvidenceRecord `json:"records"`
	RecordCount   int              `json:"record_count"`
	DateRange     string           `json:"date_range,omitempty"`
	KeyFindings   []string         `json:"key_findings,omitempty"`
	Citations     []string         `json:"citations,omitempty"`
}

// FailedSource records a data source that could not be queried.
type FailedSource struct {
	Source    string    `json:"source"`
	DatasetID string   `json:"dataset_id"`
	Error     string   `json:"error"`
	Retries   int      `json:"retries"`
	LastTry   time.Time `json:"last_try"`
	Category  string   `json:"category"`
}

// GeocodedAddress holds structured geocode results.
type GeocodedAddress struct {
	FormattedAddress string  `json:"formatted_address"`
	FullStreet       string  `json:"full_street"`
	City             string  `json:"city"`
	County           string  `json:"county"`
	State            string  `json:"state"`
	StateCode        string  `json:"state_code"`
	ZipCode          string  `json:"zip_code"`
	Lat              float64 `json:"lat"`
	Lng              float64 `json:"lng"`
}

// ResolvedIdentifiers holds BIN/BBL/APN identifiers for a property.
type ResolvedIdentifiers struct {
	BIN string `json:"bin,omitempty"`
	BBL string `json:"bbl,omitempty"`
	APN string `json:"apn,omitempty"`
}

// RetryJob represents a failed source query that should be retried.
type RetryJob struct {
	ID          string           `json:"id"`
	Address     string           `json:"address"`
	Tool        string           `json:"tool"`
	Source      DataSourceConfig `json:"source"`
	Needles     []string         `json:"needles"`
	Identifiers *ResolvedIdentifiers `json:"identifiers,omitempty"`
	Attempts    int              `json:"attempts"`
	MaxAttempts int              `json:"max_attempts"`
	NextRetry   time.Time        `json:"next_retry"`
	CreatedAt   time.Time        `json:"created_at"`
	LastError   string           `json:"last_error"`
}

// AdapterResult is returned by each adapter query.
type AdapterResult struct {
	Records    []EvidenceRecord `json:"records"`
	Source     DataSourceConfig `json:"source"`
	Error      error            `json:"-"`
	ErrorMsg   string           `json:"error,omitempty"`
	Duration   time.Duration    `json:"duration"`
	TotalCount int              `json:"total_count"` // Expected count from API (if available)
}
