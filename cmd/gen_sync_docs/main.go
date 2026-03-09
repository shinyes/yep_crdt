package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
)

type messageType struct {
	Name  string
	Value string
}

type networkField struct {
	Name     string
	Type     string
	JSONName string
	Required bool
}

var messageDescriptions = map[string]string{
	"heartbeat":         "Peer liveness + HLC clock + gc_floor propagation.",
	"raw_data":          "Full-row CRDT payload replication.",
	"raw_delta":         "Partial-column CRDT payload replication.",
	"local_file_chunk":  "Chunked local-file CRDT payload transport.",
	"fetch_raw_request": "Request remote side to stream one table snapshot.",
	"fetch_raw_response": "Streamed response row for fetch_raw_request; " +
		"end of stream is marked by a sentinel key.",
	"version_digest":  "Version digest exchange for sync planning.",
	"merkle_root_req": "Request per-table Merkle root hashes for sync diff planning.",
	"merkle_root_ack": "Response carrying per-table Merkle root hashes.",
	"merkle_node_req": "Request one Merkle node hash and its direct child hashes.",
	"merkle_node_ack": "Response carrying one Merkle node hash and its direct child hashes.",
	"merkle_leaf_req": "Request row-level hashes under one Merkle leaf prefix.",
	"merkle_leaf_ack": "Response carrying row-level hashes under one Merkle leaf prefix.",
	"gc_prepare":      "Manual GC phase 1 request.",
	"gc_prepare_ack":  "Manual GC phase 1 response.",
	"gc_commit":       "Manual GC phase 2 request (confirmation only).",
	"gc_commit_ack":   "Manual GC phase 2 response.",
	"gc_execute":      "Manual GC phase 3 request (actual execution).",
	"gc_execute_ack":  "Manual GC phase 3 response.",
	"gc_abort":        "Manual GC phase 4 request (best-effort cleanup).",
	"gc_abort_ack":    "Manual GC phase 4 response.",
}

func main() {
	root, err := findRepoRoot()
	if err != nil {
		fail(err)
	}

	typesFile := filepath.Join(root, "pkg", "sync", "types.go")
	msgTypes, fields, fetchDoneKey, err := parseTypes(typesFile)
	if err != nil {
		fail(err)
	}

	docsDir := filepath.Join(root, "docs")
	if err := os.MkdirAll(docsDir, 0o755); err != nil {
		fail(fmt.Errorf("create docs dir: %w", err))
	}

	protocolDoc := renderProtocolDoc(msgTypes, fetchDoneKey)
	if err := os.WriteFile(filepath.Join(docsDir, "sync_protocol.md"), []byte(protocolDoc), 0o644); err != nil {
		fail(fmt.Errorf("write sync_protocol.md: %w", err))
	}

	messageDoc := renderMessageDoc(fields)
	if err := os.WriteFile(filepath.Join(docsDir, "network_message.md"), []byte(messageDoc), 0o644); err != nil {
		fail(fmt.Errorf("write network_message.md: %w", err))
	}
}

func findRepoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("getwd: %w", err)
	}

	dir := wd
	for {
		goMod := filepath.Join(dir, "go.mod")
		typesFile := filepath.Join(dir, "pkg", "sync", "types.go")
		if fileExists(goMod) && fileExists(typesFile) {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("cannot find repository root from %s", wd)
}

func parseTypes(path string) ([]messageType, []networkField, string, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		return nil, nil, "", fmt.Errorf("parse %s: %w", path, err)
	}

	var msgTypes []messageType
	var fields []networkField
	fetchDoneKey := ""

	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}

		switch gen.Tok {
		case token.CONST:
			for _, spec := range gen.Specs {
				valueSpec, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for idx, name := range valueSpec.Names {
					value := constStringValue(valueSpec, idx)
					if strings.HasPrefix(name.Name, "MsgType") && value != "" {
						msgTypes = append(msgTypes, messageType{
							Name:  name.Name,
							Value: value,
						})
					}
					if name.Name == "fetchRawResponseDoneKey" && value != "" {
						fetchDoneKey = value
					}
				}
			}
		case token.TYPE:
			for _, spec := range gen.Specs {
				typeSpec, ok := spec.(*ast.TypeSpec)
				if !ok || typeSpec.Name.Name != "NetworkMessage" {
					continue
				}
				structType, ok := typeSpec.Type.(*ast.StructType)
				if !ok {
					return nil, nil, "", fmt.Errorf("NetworkMessage is not a struct")
				}
				extracted, err := extractFields(fset, structType)
				if err != nil {
					return nil, nil, "", err
				}
				fields = extracted
			}
		}
	}

	if len(msgTypes) == 0 {
		return nil, nil, "", fmt.Errorf("no MsgType constants found")
	}
	if len(fields) == 0 {
		return nil, nil, "", fmt.Errorf("NetworkMessage fields not found")
	}
	if fetchDoneKey == "" {
		return nil, nil, "", fmt.Errorf("fetchRawResponseDoneKey not found")
	}

	sort.Slice(msgTypes, func(i, j int) bool {
		if msgTypes[i].Value == msgTypes[j].Value {
			return msgTypes[i].Name < msgTypes[j].Name
		}
		return msgTypes[i].Value < msgTypes[j].Value
	})

	return msgTypes, fields, fetchDoneKey, nil
}

func constStringValue(spec *ast.ValueSpec, idx int) string {
	if len(spec.Values) == 0 {
		return ""
	}
	exprIdx := idx
	if exprIdx >= len(spec.Values) {
		exprIdx = len(spec.Values) - 1
	}
	lit, ok := spec.Values[exprIdx].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return ""
	}
	return strings.Trim(lit.Value, `"`)
}

func extractFields(fset *token.FileSet, structType *ast.StructType) ([]networkField, error) {
	var out []networkField
	for _, field := range structType.Fields.List {
		if len(field.Names) == 0 {
			continue
		}

		typeText, err := exprString(fset, field.Type)
		if err != nil {
			return nil, fmt.Errorf("render field type: %w", err)
		}

		jsonName, required := parseJSONTag(field.Tag)
		for _, name := range field.Names {
			out = append(out, networkField{
				Name:     name.Name,
				Type:     typeText,
				JSONName: jsonName,
				Required: required,
			})
		}
	}
	return out, nil
}

func exprString(fset *token.FileSet, expr ast.Expr) (string, error) {
	var buf bytes.Buffer
	if err := printer.Fprint(&buf, fset, expr); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func parseJSONTag(tag *ast.BasicLit) (string, bool) {
	if tag == nil {
		return "-", false
	}

	raw := strings.Trim(tag.Value, "`")
	jsonTag := reflect.StructTag(raw).Get("json")
	if jsonTag == "" {
		return "-", false
	}

	parts := strings.Split(jsonTag, ",")
	name := parts[0]
	if name == "" {
		name = "-"
	}

	required := name != "-"
	for _, part := range parts[1:] {
		if part == "omitempty" {
			required = false
			break
		}
	}
	return name, required
}

func renderProtocolDoc(msgTypes []messageType, fetchDoneKey string) string {
	var b strings.Builder

	b.WriteString("# Sync Protocol Reference\n\n")
	b.WriteString("Generated by `go run ./cmd/gen_sync_docs`. Do not edit manually.\n\n")

	b.WriteString("## Message Types\n\n")
	b.WriteString("| Constant | Wire Value | Description |\n")
	b.WriteString("| --- | --- | --- |\n")
	for _, mt := range msgTypes {
		desc := messageDescriptions[mt.Value]
		if desc == "" {
			desc = "No description registered."
		}
		b.WriteString(fmt.Sprintf("| `%s` | `%s` | %s |\n", mt.Name, mt.Value, desc))
	}
	b.WriteString("\n")

	b.WriteString("## Protocol Flows\n\n")
	b.WriteString("1. Heartbeat: `heartbeat`\n")
	b.WriteString("2. Incremental data sync: `raw_data`, `raw_delta`, `local_file_chunk`\n")
	b.WriteString("3. Snapshot fetch RPC: `fetch_raw_request` -> stream of `fetch_raw_response`\n")
	b.WriteString(fmt.Sprintf("   End-of-stream sentinel key: `%s`\n", fetchDoneKey))
	b.WriteString("4. Version sync digest: `version_digest`\n")
	b.WriteString("5. Manual GC: `gc_prepare` -> `gc_prepare_ack` -> `gc_commit` -> `gc_commit_ack` -> `gc_execute` -> `gc_execute_ack`\n")
	b.WriteString("6. Manual GC abort path: `gc_abort` -> `gc_abort_ack`\n")

	return b.String()
}

func renderMessageDoc(fields []networkField) string {
	var b strings.Builder

	b.WriteString("# NetworkMessage Schema\n\n")
	b.WriteString("Generated by `go run ./cmd/gen_sync_docs`. Do not edit manually.\n\n")

	b.WriteString("| Go Field | JSON Field | Type | Required |\n")
	b.WriteString("| --- | --- | --- | --- |\n")
	for _, field := range fields {
		required := "no"
		if field.Required {
			required = "yes"
		}
		b.WriteString(fmt.Sprintf("| `%s` | `%s` | `%s` | %s |\n",
			field.Name, field.JSONName, field.Type, required))
	}
	b.WriteString("\n")

	b.WriteString("Notes:\n")
	b.WriteString("- `Timestamp` is the only always-present field at transport level.\n")
	b.WriteString("- `Type` determines which optional fields must be present for one specific message.\n")
	b.WriteString("- `Clock`, `GCFloor`, and `SafeTimestamp` carry HLC / GC coordination state.\n")

	return b.String()
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "gen_sync_docs:", err)
	os.Exit(1)
}
