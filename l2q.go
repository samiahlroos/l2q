package main

import (
	"bytes"
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		processLine(scanner.Bytes())
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading from stdin: %v\n", err)
	}
}

func processLine(line []byte) {
	var logEntry map[string]interface{}
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.UseNumber()

	if err := decoder.Decode(&logEntry); err == nil {
		if _, ok := logEntry["attr"]; ok {
			processLineJSON(logEntry)
			return
		}
	}
	processLineLegacy(line)
}

// -----------------------------------------------------------------------------
// Logic for Modern JSON Logs (MongoDB 4.4+)
// -----------------------------------------------------------------------------

func processLineJSON(logEntry map[string]interface{}) {
	attr, ok := logEntry["attr"].(map[string]interface{})
	if !ok { return }
	command, ok := attr["command"].(map[string]interface{})
	if !ok { return }
	ns, ok := attr["ns"].(string)
	if !ok { return }

	parts := strings.SplitN(ns, ".", 2)
	if len(parts) < 2 { return }
	database := parts[0]
	collection := parts[1]

	if _, ok := command["find"]; ok {
		handleFindJSON(database, collection, command)
	} else if _, ok := command["aggregate"]; ok {
		handleAggregateJSON(database, collection, command)
	}
}

func handleFindJSON(database, collection string, command map[string]interface{}) {
	query := fmt.Sprintf("db.getSiblingDB('%s').%s.find(\n", database, collection)
	filter := "{}"
	if f, ok := command["filter"]; ok { filter = toShellFormat(f, true, 1) }
	query += filter
	if p, ok := command["projection"]; ok { query += ",\n" + toShellFormat(p, true, 1) }
	query += "\n)"
	if s, ok := command["sort"]; ok { query += fmt.Sprintf(".sort(%s)", toShellFormat(s, false, 0)) }
	if s, ok := command["skip"]; ok { query += fmt.Sprintf(".skip(%v)", s) }
	if l, ok := command["limit"]; ok { query += fmt.Sprintf(".limit(%s)", toShellFormat(l, false, 0)) }
	fmt.Println(query + ".explain()")
	fmt.Println("---")
}

func handleAggregateJSON(database, collection string, command map[string]interface{}) {
	pipeline, ok := command["pipeline"]
	if !ok { return }
	query := fmt.Sprintf("db.getSiblingDB('%s').%s.aggregate(\n%s\n)", database, collection, toShellFormat(pipeline, true, 1))
	fmt.Println(query + ".explain()")
	fmt.Println("---")
}

func toShellFormat(data interface{}, pretty bool, level int) string {
	indent := ""; if pretty { indent = strings.Repeat("  ", level) }
	closingIndent := ""; if pretty { closingIndent = strings.Repeat("  ", level-1) }

	switch v := data.(type) {
	case json.Number:
		return v.String()
	case map[string]interface{}:
		if val, ok := v["$oid"]; ok && len(v) == 1 { return fmt.Sprintf(`ObjectId("%v")`, val) }
		if val, ok := v["$date"]; ok && len(v) == 1 { return fmt.Sprintf(`ISODate("%v")`, val) }
		if val, ok := v["$numberInt"]; ok && len(v) == 1 { return fmt.Sprintf("%v", val) }
		if val, ok := v["$numberLong"]; ok && len(v) == 1 { return fmt.Sprintf("%v", val) }
		if val, ok := v["$regularExpression"]; ok && len(v) == 1 {
			if reMap, ok := val.(map[string]interface{}); ok { return fmt.Sprintf(`/%v/%v`, reMap["pattern"], reMap["options"]) }
		}

		var parts []string; keys := make([]string, 0, len(v)); for k := range v { keys = append(keys, k) }; sort.Strings(keys)
		for _, k := range keys {
			keyPart := fmt.Sprintf(`"%s"`, k); valPart := toShellFormat(v[k], pretty, level+1)
			if pretty { parts = append(parts, fmt.Sprintf("%s%s: %s", indent, keyPart, valPart))
			} else { parts = append(parts, fmt.Sprintf("%s: %s", keyPart, valPart)) }
		}
		separator := ", "; if pretty { separator = ",\n" }
		if pretty { return fmt.Sprintf("{\n%s\n%s}", strings.Join(parts, separator), closingIndent) }
		return fmt.Sprintf("{ %s }", strings.Join(parts, separator))

	case []interface{}:
		var parts []string; for _, item := range v { parts = append(parts, toShellFormat(item, pretty, level+1)) }
		separator := ", "; if pretty { separator = ",\n" }
		if pretty { return fmt.Sprintf("[\n%s%s\n%s]", indent, strings.Join(parts, separator+indent), closingIndent) }
		return fmt.Sprintf("[%s]", strings.Join(parts, separator))
	case string: return fmt.Sprintf(`"%s"`, v)
	case nil: return "null"
	default: return fmt.Sprintf("%v", v)
	}
}

// -----------------------------------------------------------------------------
// Logic for Legacy Text Logs (Pre-MongoDB 4.4)
// -----------------------------------------------------------------------------

func processLineLegacy(line []byte) {
	logStr := string(line)
	if strings.Contains(logStr, " command: aggregate ") {
		handleLegacyAggregate(logStr)
	} else if strings.Contains(logStr, " command: find ") {
		handleLegacyFind(logStr)
	}
}

func handleLegacyAggregate(logStr string) {
	cmdStart := strings.Index(logStr, "command: aggregate ")
	if cmdStart == -1 { return }
	objStart := strings.Index(logStr[cmdStart:], "{")
	if objStart == -1 { return }
	objStart += cmdStart

	objEnd := findMatchingBrace(logStr, objStart)
	if objEnd == -1 { return }
	commandStr := logStr[objStart : objEnd+1]

	collection := extractStringValue(commandStr, "aggregate")
	database := extractStringValue(commandStr, "$db")
	if collection == "" || database == "" { return }

	pipelineStr, ok := extractObject(commandStr, "pipeline")
	if !ok { return }

	query := fmt.Sprintf("db.getSiblingDB('%s').%s.aggregate(%s)", database, collection, pipelineStr)
	fmt.Println(query + ".explain()")
	fmt.Println("---")
}

func handleLegacyFind(logStr string) {
	cmdStart := strings.Index(logStr, "command: find ")
	if cmdStart == -1 { return }
	objStart := strings.Index(logStr[cmdStart:], "{")
	if objStart == -1 { return }
	objStart += cmdStart

	objEnd := findMatchingBrace(logStr, objStart)
	if objEnd == -1 { return }
	commandStr := logStr[objStart : objEnd+1]

	collection := extractStringValue(commandStr, "find")
	database := extractStringValue(commandStr, "$db")
	if collection == "" || database == "" { return }

	filterStr, ok := extractObject(commandStr, "filter")
	if !ok { filterStr = "{}" }

	projectionStr, hasProjection := extractObject(commandStr, "projection")
	sortStr, hasSort := extractObject(commandStr, "sort")
	limitStr, hasLimit := extractNumericValue(commandStr, "limit")
	skipStr, hasSkip := extractNumericValue(commandStr, "skip")

	query := fmt.Sprintf("db.getSiblingDB('%s').%s.find(%s", database, collection, filterStr)
	if hasProjection { query += ", " + projectionStr }
	query += ")"
	if hasSort { query += fmt.Sprintf(".sort(%s)", sortStr) }
	if hasSkip { query += fmt.Sprintf(".skip(%s)", skipStr) }
	if hasLimit { query += fmt.Sprintf(".limit(%s)", limitStr) }

	fmt.Println(query + ".explain()")
	fmt.Println("---")
}

// -----------------------------------------------------------------------------
// Helper functions for parsing legacy log text
// -----------------------------------------------------------------------------

func findMatchingBrace(s string, startPos int) int {
	openChar := s[startPos]; var closeChar byte
	if openChar == '{' { closeChar = '}' } else { closeChar = ']' }

	balance := 1
	for i := startPos + 1; i < len(s); i++ {
		if s[i] == openChar { balance++ }
		if s[i] == closeChar { balance-- }
		if balance == 0 { return i }
	}
	return -1
}

func extractObject(s, key string) (string, bool) {
	keyStart := strings.Index(s, key+":")
	if keyStart == -1 { return "", false }
	objStart := strings.Index(s[keyStart:], "{")
	if objStart == -1 {
		objStart = strings.Index(s[keyStart:], "[")
		if objStart == -1 { return "", false }
	}
	objStart += keyStart
	objEnd := findMatchingBrace(s, objStart)
	if objEnd == -1 { return "", false }
	return s[objStart : objEnd+1], true
}

func extractStringValue(s, key string) string {
	re := regexp.MustCompile(regexp.QuoteMeta(key) + `: "([^"]+)"`)
	matches := re.FindStringSubmatch(s)
	if len(matches) < 2 { return "" }
	return matches[1]
}

func extractNumericValue(s, key string) (string, bool) {
	re := regexp.MustCompile(regexp.QuoteMeta(key) + `: (\d+)`)
	matches := re.FindStringSubmatch(s)
	if len(matches) < 2 { return "", false }
	return matches[1], true
}
