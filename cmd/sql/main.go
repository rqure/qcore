package main

import (
	"bufio"
	"context"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qstore"
	"github.com/rqure/qlib/pkg/qlog"
)

const (
	defaultPostgresAddr = "postgres://qcore:qcore@postgres:5432/qstore?sslmode=disable"
	defaultTimeout      = 30 * time.Second
)

var (
	postgresAddr string
	timeout      int
	logLevel     string
	outputFormat string
)

func init() {
	flag.StringVar(&postgresAddr, "postgres", getEnvOrDefault("Q_POSTGRES_ADDR", defaultPostgresAddr), "PostgreSQL connection string")
	flag.IntVar(&timeout, "timeout", 30, "Connection timeout in seconds")
	flag.StringVar(&logLevel, "log-level", "INFO", "Log level (TRACE, DEBUG, INFO, WARN, ERROR, PANIC)")
	flag.StringVar(&outputFormat, "format", "table", "Output format (table, json, xml)")
	flag.Parse()
}

func getEnvOrDefault(env, defaultVal string) string {
	if val := os.Getenv(env); val != "" {
		return val
	}
	return defaultVal
}

// Result represents a row of data from the query result
type Result map[string]string

// ResultSet represents the complete query results
type ResultSet struct {
	Headers []string `json:"headers" xml:"headers>header"`
	Rows    []Result `json:"rows" xml:"rows>row"`
}

// XMLResultSet is a wrapper for XML output
type XMLResultSet struct {
	XMLName xml.Name `xml:"resultset"`
	ResultSet
}

func displayResults(results ResultSet) {
	switch strings.ToLower(outputFormat) {
	case "json":
		jsonData, err := json.MarshalIndent(results, "", "  ")
		if err != nil {
			qlog.Error("Error marshaling JSON: %s", err.Error())
			return
		}
		fmt.Println(string(jsonData))

	case "xml":
		xmlData, err := xml.MarshalIndent(XMLResultSet{ResultSet: results}, "", "  ")
		if err != nil {
			qlog.Error("Error marshaling XML: %s", err.Error())
			return
		}
		fmt.Println(xml.Header + string(xmlData))

	case "table", "": // Default to table format
		if len(results.Rows) == 0 {
			fmt.Println("No results found")
			return
		}

		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader(results.Headers)

		for _, row := range results.Rows {
			rowData := make([]string, len(results.Headers))
			for i, header := range results.Headers {
				rowData[i] = row[header]
			}
			table.Append(rowData)
		}

		table.SetAutoFormatHeaders(true)
		table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
		table.SetAlignment(tablewriter.ALIGN_LEFT)
		table.SetAutoWrapText(false)
		table.Render()

	default:
		qlog.Error("Unsupported output format: %s", outputFormat)
	}
}

func main() {
	ctx := context.WithValue(context.Background(), qcontext.KeyAppName, "qsql")
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()

	// Create and connect store
	store := new(qdata.Store).Init(
		qstore.PersistOverPostgres(postgresAddr))
	store.Connect(ctx)
	defer store.Disconnect(ctx)

	// Wait for connection
	startTime := time.Now()
	for !store.IsConnected() {
		store.CheckConnection(ctx)
		if time.Since(startTime) > defaultTimeout {
			qlog.Error("Timeout waiting for database connection")
			os.Exit(1)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Get query from command line args or stdin
	query := strings.Join(flag.Args(), " ")
	if query == "" {
		scanner := bufio.NewScanner(os.Stdin)
		fmt.Print("Enter SQL query: ")
		if scanner.Scan() {
			query = scanner.Text()
		}
	}

	if query == "" {
		qlog.Error("No query provided")
		os.Exit(1)
	}

	if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "SELECT") {
		qlog.Error("Only SELECT queries are allowed")
		os.Exit(1)
	}

	// Execute query and collect results
	results := ResultSet{
		Headers: []string{},
		Rows:    []Result{},
	}
	headerMap := make(map[string]bool)

	// First pass to collect all possible headers
	store.PrepareQuery(query).ForEach(ctx, func(entity *qdata.Entity) bool {
		row := make(Result)

		// Initialize all fields to empty strings
		for _, header := range results.Headers {
			row[header] = ""
		}

		// Fill in values for fields that exist
		for _, field := range entity.Fields {
			headerMap[field.FieldType.AsString()] = true
			row[field.FieldType.AsString()] = field.Value.AsString()
		}

		results.Rows = append(results.Rows, row)
		return true
	})

	// Convert header map to slice
	for header := range headerMap {
		results.Headers = append(results.Headers, header)
	}

	// Display results in selected format
	displayResults(results)
}
