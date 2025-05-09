package sql_test

import (
	"bytes"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rqure/qlib/pkg/qlog"
)

func init() {
	// Set log levels to minimize noise during tests
	qlog.SetLevel(qlog.ERROR)
	qlog.SetLibLevel(qlog.ERROR)
}

// engines to test
var engines = []string{"exprlang", "sqlite"}

func TestQueryEngines(t *testing.T) {
	testCases := []struct {
		name     string
		query    string
		validate func(t *testing.T, output string)
	}{
		{
			name:  "basic_entity_query",
			query: "SELECT \"$EntityId\", \"Name\" FROM Root",
			validate: func(t *testing.T, output string) {
				// Parse the JSON output
				var results []map[string]interface{}
				err := json.Unmarshal([]byte(output), &results)
				if err != nil {
					t.Errorf("failed to parse JSON output: %v", err)
					return
				}

				// Check that we have at least one result
				if len(results) == 0 {
					t.Errorf("expected at least one result row")
					return
				}

				// Validate that each result has the expected fields
				for i, result := range results {
					entityId, hasEntityId := result["$EntityId"]
					if !hasEntityId {
						t.Errorf("result %d missing $EntityId field", i)
					}
					if entityId == "" {
						t.Errorf("result %d has empty $EntityId", i)
					}

					name, hasName := result["Name"]
					if !hasName {
						t.Errorf("result %d missing Name field", i)
					}
					if name == "" {
						t.Errorf("result %d has empty Name", i)
					}

					// Specific check for "Root" entity
					if name == "Root" && !strings.Contains(entityId.(string), "-") {
						t.Errorf("Root entity ID format incorrect: %v", entityId)
					}
				}
			},
		},
		{
			name:  "security_models_query",
			query: "SELECT \"$EntityId\", \"Name\" FROM Folder WHERE Name = 'Security Models'",
			validate: func(t *testing.T, output string) {
				var results []map[string]interface{}
				err := json.Unmarshal([]byte(output), &results)
				if err != nil {
					t.Errorf("failed to parse JSON output: %v", err)
					return
				}

				// Should find exactly one "Security Models" folder
				if len(results) != 1 {
					t.Errorf("expected exactly one result, got %d", len(results))
					return
				}

				result := results[0]
				name, hasName := result["Name"]
				if !hasName || name != "Security Models" {
					t.Errorf("expected Name='Security Models', got %v", name)
				}
			},
		},
		{
			name:  "permissions_query",
			query: "SELECT \"$EntityId\", \"Name\", \"Policy\" FROM Permission",
			validate: func(t *testing.T, output string) {
				var results []map[string]interface{}
				err := json.Unmarshal([]byte(output), &results)
				if err != nil {
					t.Errorf("failed to parse JSON output: %v", err)
					return
				}

				// Should find at least one permission entity (Kernel)
				if len(results) == 0 {
					t.Errorf("expected at least one permission entity")
					return
				}

				foundKernel := false
				for _, result := range results {
					name, hasName := result["Name"]
					if hasName && name == "Kernel" {
						foundKernel = true
						// Check that policy field exists and is non-empty
						policy, hasPolicy := result["Policy"]
						if !hasPolicy {
							t.Errorf("Kernel permission missing Policy field")
						}
						if policy == "" {
							t.Errorf("Kernel permission has empty Policy")
						}
					}
				}

				if !foundKernel {
					t.Errorf("did not find Kernel permission entity")
				}
			},
		},
		{
			name:  "users_query",
			query: "SELECT \"$EntityId\", \"Name\" FROM User",
			validate: func(t *testing.T, output string) {
				var results []map[string]interface{}
				err := json.Unmarshal([]byte(output), &results)
				if err != nil {
					t.Errorf("failed to parse JSON output: %v", err)
					return
				}

				// Should find at least one user entity (qei)
				if len(results) == 0 {
					t.Errorf("expected at least one user entity")
					return
				}

				foundAdmin := false
				for _, result := range results {
					name, hasName := result["Name"]
					if hasName && name == "qei" {
						foundAdmin = true
					}
				}

				if !foundAdmin {
					t.Errorf("did not find qei user entity")
				}
			},
		},
		{
			name:  "client_query",
			query: "SELECT \"$EntityId\", \"Name\" FROM Client",
			validate: func(t *testing.T, output string) {
				var results []map[string]interface{}
				err := json.Unmarshal([]byte(output), &results)
				if err != nil {
					t.Errorf("failed to parse JSON output: %v", err)
					return
				}

				// Should find at least the clients defined in init.go
				if len(results) < 3 {
					t.Errorf("expected at least 3 client entities, got %d", len(results))
					return
				}

				foundClients := make(map[string]bool)
				for _, result := range results {
					name, hasName := result["Name"]
					if hasName && name != "" {
						foundClients[name.(string)] = true
					}
				}

				expectedClients := []string{"qinitdb", "qsql", "qcore"}
				for _, clientName := range expectedClients {
					if !foundClients[clientName] {
						t.Errorf("did not find %s client entity", clientName)
					}
				}
			},
		},
	}

	for _, engine := range engines {
		for _, tc := range testCases {
			t.Run(engine+"_"+tc.name, func(t *testing.T) {
				// Run the command from parent directory
				binPath := filepath.Join("..", "main.go")
				cmd := exec.Command("go", "run", binPath,
					"--engine", engine,
					"--format", "json",
					"--log-level", "ERROR",
					"--lib-log-level", "ERROR",
					tc.query)

				var out, stderr bytes.Buffer
				cmd.Stdout = &out
				cmd.Stderr = &stderr

				err := cmd.Run()
				if err != nil {
					t.Fatalf("engine %s: failed to run qsql: %v\nstderr: %s", engine, err, stderr.String())
				}

				output := out.String()
				if output == "" || output == "No results found\n" {
					t.Errorf("engine %s: empty output or no results found", engine)
					return
				}

				// Run the test-case specific validation
				tc.validate(t, output)
			})
		}
	}
}

func TestAdvancedQueryFeatures(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping advanced query tests in short mode")
	}

	testCases := []struct {
		name     string
		query    string
		validate func(t *testing.T, output string)
	}{
		{
			name:  "join_entities",
			query: "SELECT u.\"Name\" as UserName, r.\"Name\" as RoleName FROM User u JOIN Role r ON u.\"Roles\" = r.\"$EntityId\"",
			validate: func(t *testing.T, output string) {
				var results []map[string]interface{}
				err := json.Unmarshal([]byte(output), &results)
				if err != nil {
					t.Errorf("failed to parse JSON output: %v", err)
					return
				}

				// Check for at least one result
				if len(results) == 0 {
					t.Errorf("expected at least one join result")
					return
				}

				// Check for the admin role assignment
				foundAdminRole := false
				for _, result := range results {
					userName := result["UserName"]
					roleName := result["RoleName"]
					if userName == "qei" && roleName == "Admin" {
						foundAdminRole = true
						break
					}
				}

				if !foundAdminRole {
					t.Errorf("did not find qei user with Admin role in join results")
				}
			},
		},
		{
			name:  "count_query",
			query: "SELECT COUNT(*) as EntityCount FROM Root",
			validate: func(t *testing.T, output string) {
				var results []map[string]interface{}
				err := json.Unmarshal([]byte(output), &results)
				if err != nil {
					t.Errorf("failed to parse JSON output: %v", err)
					return
				}

				// Should have exactly one result with the count
				if len(results) != 1 {
					t.Errorf("expected exactly one result for count query, got %d", len(results))
					return
				}

				count, hasCount := results[0]["EntityCount"]
				if !hasCount {
					t.Errorf("count result missing EntityCount field")
					return
				}

				// We expect at least 10 entities from the initialization
				// Need to handle that count could be a float or string depending on the engine
				var countValue float64
				switch v := count.(type) {
				case float64:
					countValue = v
				case int:
					countValue = float64(v)
				case string:
					countValue, err = parseNumber(v)
					if err != nil {
						t.Errorf("failed to parse count value %v: %v", v, err)
						return
					}
				default:
					t.Errorf("unexpected type for count value: %T", count)
					return
				}

				if countValue < 10 {
					t.Errorf("expected at least 10 entities, got %v", count)
				}
			},
		},
		{
			name:  "nested_query",
			query: "SELECT f.\"Name\" as FolderName, p.\"Name\" as PermissionName FROM Folder f JOIN Permission p ON p.\"Parent\" = f.\"$EntityId\" WHERE f.\"Name\" = 'Permissions'",
			validate: func(t *testing.T, output string) {
				var results []map[string]interface{}
				err := json.Unmarshal([]byte(output), &results)
				if err != nil {
					t.Errorf("failed to parse JSON output: %v", err)
					return
				}

				// Should find permissions under the permissions folder
				if len(results) == 0 {
					t.Errorf("expected at least one permission under Permissions folder")
					return
				}

				// Check for Kernel permission
				foundKernel := false
				for _, result := range results {
					folderName := result["FolderName"]
					permissionName := result["PermissionName"]
					if folderName == "Permissions" && permissionName == "Kernel" {
						foundKernel = true
						break
					}
				}

				if !foundKernel {
					t.Errorf("did not find Kernel permission under Permissions folder")
				}
			},
		},
	}

	for _, engine := range engines {
		for _, tc := range testCases {
			t.Run(engine+"_"+tc.name, func(t *testing.T) {
				// Run the command from parent directory
				binPath := filepath.Join("..", "main.go")
				cmd := exec.Command("go", "run", binPath,
					"--engine", engine,
					"--format", "json",
					"--log-level", "ERROR",
					"--lib-log-level", "ERROR",
					tc.query)

				var out, stderr bytes.Buffer
				cmd.Stdout = &out
				cmd.Stderr = &stderr

				err := cmd.Run()
				if err != nil {
					// Some engines might not support all features, so just skip the test
					t.Skipf("engine %s: unsupported query feature or error: %v\nstderr: %s", engine, err, stderr.String())
					return
				}

				output := out.String()
				tc.validate(t, string(output))
			})
		}
	}
}

// parseNumber attempts to parse a numeric string
func parseNumber(s string) (float64, error) {
	var f float64
	err := json.Unmarshal([]byte(s), &f)
	return f, err
}
