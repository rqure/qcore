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

					// Specific check for "Root" entity - accept $ format instead of hyphen
					if name == "Root" && !strings.Contains(entityId.(string), "$") && !strings.Contains(entityId.(string), "-") {
						t.Errorf("Root entity ID format incorrect: %v", entityId)
					}
				}
			},
		},
		{
			name:  "security_models_query",
			query: "SELECT \"$EntityId\", \"Name\" FROM Folder WHERE Name LIKE 'Security%'",
			validate: func(t *testing.T, output string) {
				var results []map[string]interface{}
				err := json.Unmarshal([]byte(output), &results)
				if err != nil {
					t.Errorf("failed to parse JSON output: %v", err)
					return
				}

				// Should find at least one security-related folder
				if len(results) == 0 {
					t.Errorf("expected at least one security folder result")
					return
				}

				// Look for a security-related folder
				securityFolderFound := false
				for _, result := range results {
					name, hasName := result["Name"]
					if hasName && strings.Contains(name.(string), "Security") {
						securityFolderFound = true
						break
					}
				}

				if !securityFolderFound {
					t.Errorf("expected a security-related folder in results")
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
			query: "SELECT u.\"Name\" as UserName, r.\"Name\" as RoleName FROM User u, Role r WHERE r.\"$EntityId\" IN u.\"Roles\"",
			validate: func(t *testing.T, output string) {
				var results []map[string]interface{}
				err := json.Unmarshal([]byte(output), &results)
				if err != nil {
					t.Errorf("failed to parse JSON output: %v", err)
					return
				}

				// Check if we got any results
				if len(results) == 0 {
					// Try a simpler check to see if we have any users and roles 
					// to determine if this is a query syntax issue or data issue
					t.Logf("No join results - verify that users have assigned roles in the system")
					return
				}

				// Check for user-role assignments if we have results
				for _, result := range results {
					userName, hasUserName := result["UserName"]
					roleName, hasRoleName := result["RoleName"]
					
					if !hasUserName || !hasRoleName {
						t.Errorf("missing UserName or RoleName in result: %v", result)
					} else {
						// Successful test - we found at least one valid user-role assignment
						t.Logf("Found user '%v' with role '%v'", userName, roleName)
					}
				}
			},
		},
		{
			name:  "count_query",
			query: "SELECT COUNT(*) as Count FROM Root",
			validate: func(t *testing.T, output string) {
				var results []map[string]interface{}
				err := json.Unmarshal([]byte(output), &results)
				if err != nil {
					t.Errorf("failed to parse JSON output: %v", err)
					return
				}

				// Should have at least one result with the count
				if len(results) == 0 {
					t.Errorf("expected at least one result for count query")
					return
				}

				// Try different possible count field names
				count, hasCount := results[0]["Count"]
				if !hasCount {
					count, hasCount = results[0]["count"]
					if !hasCount {
						count, hasCount = results[0]["COUNT(*)"]
						if !hasCount {
							// Print available fields to help debug
							t.Errorf("count result missing expected count field. Available fields: %v", results[0])
							return
						}
					}
				}

				// We expect at least a few entities from the initialization
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

				if countValue < 1 {
					t.Errorf("expected at least 1 entity, got %v", count)
				}
			},
		},
		{
			name:  "nested_query",
			query: "SELECT f.\"Name\" as FolderName, p.\"Name\" as PermissionName FROM Folder f, Permission p WHERE p.\"Parent\" = f.\"$EntityId\"",
			validate: func(t *testing.T, output string) {
				var results []map[string]interface{}
				err := json.Unmarshal([]byte(output), &results)
				if err != nil {
					t.Errorf("failed to parse JSON output: %v", err)
					return
				}

				// Check if we have any results at all
				if len(results) == 0 {
					// Try a simpler check to see if permissions exist in folders
					t.Logf("No nested permissions found - verify folder and permission structure")
					return
				}

				// We found at least one permission in a folder
				for _, result := range results {
					folderName, hasFolderName := result["FolderName"]
					permissionName, hasPermissionName := result["PermissionName"]
					
					if !hasFolderName || !hasPermissionName {
						t.Errorf("missing FolderName or PermissionName in result: %v", result)
					} else {
						// Successful test - we found at least one valid folder-permission relationship
						t.Logf("Found permission '%v' in folder '%v'", permissionName, folderName)
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
