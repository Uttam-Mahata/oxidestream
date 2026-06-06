package scheduler

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

// ApplyDPP parses the dimension CSV file, filters rows matching filterCol == filterVal,
// extracts the joinKey values, and injects them as an `AND joinKey IN (...)` clause
// into the Map SQL query to prune mapper scan partitions dynamically.
func ApplyDPP(sql string, dimFile string, filterCol string, filterVal string, joinKey string) (string, error) {
	file, err := os.Open(dimFile)
	if err != nil {
		return sql, fmt.Errorf("failed to open dimension file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return sql, fmt.Errorf("failed to read dimension header: %w", err)
	}

	filterIdx := -1
	joinIdx := -1
	for i, col := range header {
		col = strings.TrimSpace(col)
		if strings.EqualFold(col, filterCol) {
			filterIdx = i
		}
		if strings.EqualFold(col, joinKey) {
			joinIdx = i
		}
	}

	if filterIdx == -1 || joinIdx == -1 {
		return sql, fmt.Errorf("filter column %q or join key %q not found in dimension header %v", filterCol, joinKey, header)
	}

	var matchingKeys []string
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return sql, fmt.Errorf("failed to read dimension row: %w", err)
		}

		if len(row) > filterIdx && len(row) > joinIdx {
			if strings.EqualFold(strings.TrimSpace(row[filterIdx]), filterVal) {
				val := strings.TrimSpace(row[joinIdx])
				if val != "" {
					// Quote the value for safety in SQL
					matchingKeys = append(matchingKeys, fmt.Sprintf("'%s'", val))
				}
			}
		}
	}

	log.Printf("DPP: Found %d matching keys from dimension file %s where %s = %s", len(matchingKeys), dimFile, filterCol, filterVal)

	if len(matchingKeys) == 0 {
		// No keys matched; inject a false condition to skip processing entirely
		matchingKeys = append(matchingKeys, "'-1'")
	}

	inClause := fmt.Sprintf(" AND %s IN (%s)", joinKey, strings.Join(matchingKeys, ", "))
	
	// Inject filter before the GROUP BY clause or append to WHERE
	modifiedSQL := sql
	if strings.Contains(strings.ToUpper(sql), "GROUP BY") {
		parts := strings.SplitN(sql, "GROUP BY", 2)
		if strings.Contains(strings.ToUpper(parts[0]), "WHERE") {
			modifiedSQL = fmt.Sprintf("%s%s GROUP BY%s", parts[0], inClause, parts[1])
		} else {
			modifiedSQL = fmt.Sprintf("%s WHERE 1=1%s GROUP BY%s", parts[0], inClause, parts[1])
		}
	} else {
		if strings.Contains(strings.ToUpper(sql), "WHERE") {
			modifiedSQL = fmt.Sprintf("%s%s", sql, inClause)
		} else {
			modifiedSQL = fmt.Sprintf("%s WHERE 1=1%s", sql, inClause)
		}
	}

	log.Printf("DPP: Original SQL: %s", sql)
	log.Printf("DPP: Modified SQL: %s", modifiedSQL)
	return modifiedSQL, nil
}
