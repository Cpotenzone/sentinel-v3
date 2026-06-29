package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/Cpotenzone/sentinel-v3/pipeline"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// Register adds all 6 sentinel tools to the MCP server.
func Register(s *server.MCPServer, pipe *pipeline.Pipeline) {
	tools := []struct {
		name        string
		description string
	}{
		{"sentinel_research", "Full building research — queries ALL public record categories (permits, violations, environmental, ownership, financial, inspections, facades, certificates of occupancy). Returns comprehensive cited evidence."},
		{"sentinel_permits", "Building permits and certificates of occupancy only. Faster than full research."},
		{"sentinel_environmental", "Environmental records, fire inspections, and health inspections only."},
		{"sentinel_financial", "Tax assessments and property records only."},
		{"sentinel_ownership", "Property ownership and registration records only."},
		{"sentinel_311", "311 service request complaints for the building."},
	}

	for _, t := range tools {
		toolDef := mcp.NewTool(t.name,
			mcp.WithDescription(t.description),
			mcp.WithString("address",
				mcp.Required(),
				mcp.Description("Full street address including city and state (e.g., '110 Central Park South, New York, NY 10019')"),
			),
		)

		toolName := t.name // capture for closure
		handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := request.GetArguments()
			address, _ := args["address"].(string)
			if address == "" {
				return mcp.NewToolResultError("address is required"), nil
			}

			log.Printf("Tool called: %s for %s", toolName, address)

			report, err := pipe.Execute(ctx, toolName, address)
			if err != nil {
				log.Printf("Tool %s failed for %s: %v", toolName, address, err)
				return mcp.NewToolResultError(fmt.Sprintf("Research failed: %v", err)), nil
			}

			// Serialize report to JSON
			data, err := json.Marshal(report)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("Serialization failed: %v", err)), nil
			}

			return mcp.NewToolResultText(string(data)), nil
		}

		s.AddTool(toolDef, handler)
	}
}
