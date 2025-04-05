package main

import (
	"bufio"
	"context"
	"encoding/csv"
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

	// Output format constants
	formatTable        = "table"
	formatPlain        = "plain"
	formatUnicode      = "unicode"
	formatUnicodeLight = "unicodelight"
	formatUnicodeBold  = "unicodebold"
	formatColon        = "colon"
	formatCSV          = "csv"
	formatGithub       = "github"
	formatJSON         = "json"
	formatXML          = "xml"
)

var (
	postgresAddr string
	timeout      int
	logLevel     string
	libLogLevel  string
	outputFormat string
)

func init() {
	flag.StringVar(&postgresAddr, "postgres", getEnvOrDefault("Q_POSTGRES_ADDR", defaultPostgresAddr), "PostgreSQL connection string")
	flag.IntVar(&timeout, "timeout", 30, "Connection timeout in seconds")
	flag.StringVar(&logLevel, "log-level", "INFO", "Log level (TRACE, DEBUG, INFO, WARN, ERROR, PANIC)")
	flag.StringVar(&libLogLevel, "lib-log-level", "INFO", "Set library log level (TRACE, DEBUG, INFO, WARN, ERROR, PANIC)")
	flag.StringVar(&outputFormat, "format", "table", "Output format (table, plain, unicode, unicodelight, unicodebold, colon, csv, github, json, xml)")
	flag.Parse()
}

func getEnvOrDefault(env, defaultVal string) string {
	if val := os.Getenv(env); val != "" {
		return val
	}
	return defaultVal
}

// Result represents a row of data from the query result
type Result struct {
	Fields []Field `json:"-" xml:"field"`
	data   map[string]string
}

// Field represents a single field in the XML output
type Field struct {
	Name  string `xml:"name,attr"`
	Value string `xml:",chardata"`
}

// MarshalJSON implements custom JSON marshaling
func (r Result) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.data)
}

// ResultSet represents the complete query results
type ResultSet struct {
	Headers []string `json:"headers" xml:"headers>header"`
	Rows    []Result `json:"rows" xml:"rows>row"`
}

// XMLResultSet is a wrapper for XML output
type XMLResultSet struct {
	XMLName xml.Name `xml:"resultset"`
	Headers []string `xml:"headers>header"`
	Rows    []Result `xml:"rows>row"`
}

func displayResults(results ResultSet) {
	switch strings.ToLower(outputFormat) {
	case formatJSON:
		jsonData, err := json.MarshalIndent(results, "", "  ")
		if err != nil {
			qlog.Error("Error marshaling JSON: %s", err.Error())
			return
		}
		fmt.Println(string(jsonData))

	case formatXML:
		// Convert ResultSet to XMLResultSet format
		xmlResults := XMLResultSet{
			Headers: results.Headers,
			Rows:    make([]Result, len(results.Rows)),
		}

		for i, row := range results.Rows {
			result := Result{
				Fields: make([]Field, len(results.Headers)),
				data:   row.data,
			}
			for j, header := range results.Headers {
				result.Fields[j] = Field{
					Name:  header,
					Value: row.data[header],
				}
			}
			xmlResults.Rows[i] = result
		}

		xmlData, err := xml.MarshalIndent(xmlResults, "", "  ")
		if err != nil {
			qlog.Error("Error marshaling XML: %s", err.Error())
			return
		}
		fmt.Println(xml.Header + string(xmlData))

	case formatPlain:
		if len(results.Rows) == 0 {
			fmt.Println("No results found")
			return
		}
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader(results.Headers)
		table.SetBorder(false)
		table.SetColumnSeparator(" ")
		table.SetHeaderLine(false)
		table.SetRowLine(false)
		fillTableData(table, results)
		table.Render()

	case formatUnicode:
		if len(results.Rows) == 0 {
			fmt.Println("No results found")
			return
		}
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader(results.Headers)
		table.SetBorders(tablewriter.Border{
			Left:   true,
			Right:  true,
			Top:    true,
			Bottom: true,
		})
		table.SetCenterSeparator("┳")
		table.SetColumnSeparator("┃")
		table.SetRowSeparator("━")
		fillTableData(table, results)
		table.Render()

	case formatUnicodeLight:
		if len(results.Rows) == 0 {
			fmt.Println("No results found")
			return
		}
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader(results.Headers)
		table.SetBorders(tablewriter.Border{
			Left:   true,
			Right:  true,
			Top:    true,
			Bottom: true,
		})
		table.SetCenterSeparator("┼")
		table.SetColumnSeparator("│")
		table.SetRowSeparator("─")
		fillTableData(table, results)
		table.Render()

	case formatUnicodeBold:
		if len(results.Rows) == 0 {
			fmt.Println("No results found")
			return
		}
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader(results.Headers)
		table.SetBorders(tablewriter.Border{
			Left:   true,
			Right:  true,
			Top:    true,
			Bottom: true,
		})
		table.SetCenterSeparator("╋")
		table.SetColumnSeparator("┃")
		table.SetRowSeparator("━")
		fillTableData(table, results)
		table.Render()

	case formatColon:
		if len(results.Rows) == 0 {
			fmt.Println("No results found")
			return
		}
		// Print headers
		fmt.Println(strings.Join(results.Headers, " : "))
		// Print rows
		for _, row := range results.Rows {
			rowData := make([]string, len(results.Headers))
			for i, header := range results.Headers {
				rowData[i] = row.data[header]
			}
			fmt.Println(strings.Join(rowData, " : "))
		}

	case formatCSV:
		if len(results.Rows) == 0 {
			fmt.Println("No results found")
			return
		}
		w := csv.NewWriter(os.Stdout)
		w.Write(results.Headers)
		for _, row := range results.Rows {
			rowData := make([]string, len(results.Headers))
			for i, header := range results.Headers {
				rowData[i] = row.data[header]
			}
			w.Write(rowData)
		}
		w.Flush()

	case formatGithub:
		if len(results.Rows) == 0 {
			fmt.Println("No results found")
			return
		}
		// Print headers
		fmt.Print("| ")
		fmt.Print(strings.Join(results.Headers, " | "))
		fmt.Println(" |")

		// Print separator
		separators := make([]string, len(results.Headers))
		for i := range separators {
			separators[i] = "------"
		}
		fmt.Print("| ")
		fmt.Print(strings.Join(separators, " | "))
		fmt.Println(" |")

		// Print rows
		for _, row := range results.Rows {
			rowData := make([]string, len(results.Headers))
			for i, header := range results.Headers {
				rowData[i] = row.data[header]
			}
			fmt.Print("| ")
			fmt.Print(strings.Join(rowData, " | "))
			fmt.Println(" |")
		}

	case formatTable, "": // Default to table format
		if len(results.Rows) == 0 {
			fmt.Println("No results found")
			return
		}

		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader(results.Headers)
		fillTableData(table, results)
		table.Render()

	default:
		qlog.Error("Unsupported output format: %s", outputFormat)
	}
}

// Helper function to fill table data
func fillTableData(table *tablewriter.Table, results ResultSet) {
	for _, row := range results.Rows {
		rowData := make([]string, len(results.Headers))
		for i, header := range results.Headers {
			rowData[i] = row.data[header]
		}
		table.Append(rowData)
	}
	table.SetAutoFormatHeaders(true)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetAutoWrapText(false)
}

func main() {
	// Set log levels before any other operations
	setLogLevel(logLevel, libLogLevel)

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
		row := make(map[string]string)

		// Initialize all fields to empty strings
		for _, header := range results.Headers {
			row[header] = ""
		}

		// Fill in values for fields that exist
		for _, field := range entity.Fields {
			headerMap[field.FieldType.AsString()] = true
			row[field.FieldType.AsString()] = field.Value.AsString()
		}

		results.Rows = append(results.Rows, Result{data: row})
		return true
	})

	// Convert header map to slice
	for header := range headerMap {
		results.Headers = append(results.Headers, header)
	}

	// Display results in selected format
	displayResults(results)
}

func setLogLevel(appLevel, libLevel string) {
	levelMap := map[string]qlog.Level{
		"TRACE": qlog.TRACE,
		"DEBUG": qlog.DEBUG,
		"INFO":  qlog.INFO,
		"WARN":  qlog.WARN,
		"ERROR": qlog.ERROR,
		"PANIC": qlog.PANIC,
	}

	if level, ok := levelMap[strings.ToUpper(appLevel)]; ok {
		qlog.SetLevel(level)
	} else {
		qlog.Warn("Invalid log level '%s', using INFO", appLevel)
		qlog.SetLevel(qlog.INFO)
	}

	if level, ok := levelMap[strings.ToUpper(libLevel)]; ok {
		qlog.SetLibLevel(level)
	} else {
		qlog.Warn("Invalid lib log level '%s', using INFO", libLevel)
		qlog.SetLibLevel(qlog.INFO)
	}
}
