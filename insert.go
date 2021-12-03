package bulk

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/daniloor/helper"
)

const PLACEHOLDER_LIMIT = 60000

// This structure acts as a "Bulk Insert", which is defined as a process or method provided
// by a database management system to load multiple rows of data into a database table.
type Bulk struct {
	initStr        string // Contains the first part of the string in the insert statment
	placeholderStr string // Contains the placeholders str, which depends of the number of columns and values to insert.
	// In the case of 2 different columns and 3 values, placeolderStr will be: (?,?),(?,?),(?,?),
	vals []interface{} // Contains all the values to insert. It's neccesary to use the Go Interface type to manipulate both
	// float64 and int values
	rows                 int    // Number of rows
	valuesPerRow         int    // Number of values per row
	placeholderStrHelper string // Contains a string helper which helps to construct the placeholderStr. In the case of
	// 2 different columns, placeholderStrHelper will be: (?,?),
}

// Init initializes the attributes members
func (b *Bulk) Init(tableName string, s ...string) {
	b.initStr = "INSERT INTO " + tableName + "(" + strings.Join(s, ", ") + ") VALUES "
	b.vals = []interface{}{}
	b.valuesPerRow = len(s)
	b.rows = 0
	b.placeholderStrHelper = "(?" + strings.Repeat(",?", b.valuesPerRow-1) + "),"
}

// Insert inserts the data into the db database. If replaceOnDuplicate is true, the insert statment
// will include a ON DUPLICATE KEY UPDATE at the end.
func (b *Bulk) Insert(db *sql.DB, replaceOnDuplicate bool) error {

	// if len(b.vals) < PLACEHOLDER_LIMIT, that means that there is no placeholder problem
	if len(b.vals) < PLACEHOLDER_LIMIT {
		// Trim the last ,
		b.placeholderStr = b.placeholderStr[0 : len(b.placeholderStr)-1]

		// Generate the strim that it's going to be used for the prepared statement
		str := b.initStr + b.placeholderStr
		if replaceOnDuplicate {
			firstIndex := strings.Index(b.initStr, "(")
			secondIndex := strings.Index(b.initStr, ")")
			columnsStr := b.initStr[firstIndex+1 : secondIndex]
			columns := strings.Split(columnsStr, ",")
			endStr := " ON DUPLICATE KEY UPDATE "
			for _, v := range columns {
				endStr += v + "=VALUES(" + v + "),"
			}
			endStr = endStr[:len(endStr)-1]
			str += endStr
		}
		// Prepare the statement
		stmt, err := db.Prepare(str)
		if err != nil {
			return err
		}
		// Format all vals at once
		_, err = stmt.Exec(b.vals...)
		if err != nil {
			return err
		}
	} else { // If we have more than PLACEHOLDER_LIMIT values to insert, we have to insert the values separatly
		// In each iteration, we will insert at least PLACEHOLDER_LIMIT values

		// "batchs" is the number of times we have to divide the data
		batchsF := float64(len(b.vals)) / float64(PLACEHOLDER_LIMIT)
		batchs := helper.RoundUp(batchsF)

		rowsPerBatch := PLACEHOLDER_LIMIT / b.valuesPerRow
		charactersPerPlaceholder := 2 * (b.valuesPerRow + 1)
		charactersPerBatch := rowsPerBatch * charactersPerPlaceholder
		for i := 0; i < batchs; i++ {
			var str string
			var vals []interface{}
			if i == batchs-1 {
				str = b.initStr + b.placeholderStr[charactersPerBatch*i:]
				vals = b.vals[rowsPerBatch*b.valuesPerRow*i:]
			} else {
				str = b.initStr + b.placeholderStr[charactersPerBatch*i:charactersPerBatch*(i+1)]
				vals = b.vals[rowsPerBatch*b.valuesPerRow*i : rowsPerBatch*b.valuesPerRow*(i+1)]
			}

			// Same process for len(b.vals) < PLACEHOLDER_LIMIT
			// Trim the last ,
			str = str[0 : len(str)-1]
			if replaceOnDuplicate {
				firstIndex := strings.Index(b.initStr, "(")
				secondIndex := strings.Index(b.initStr, ")")
				columnsStr := b.initStr[firstIndex+1 : secondIndex]
				columns := strings.Split(columnsStr, ", ")
				endStr := " ON DUPLICATE KEY UPDATE "
				for _, v := range columns {
					endStr += v + "=VALUES(" + v + "),"
				}
				endStr = endStr[:len(endStr)-1]
				str += endStr
			}
			// Prepare the statement
			stmt, err := db.Prepare(str)
			if err != nil {
				return err
			}
			// Format all vals at once
			_, err = stmt.Exec(vals...)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// PrepareValues receives the values that are going to be appended to the vals members.
// The number of values must match the valuesPerRow, otherwise, it exits with an error code.
func (b *Bulk) PrepareValues(vals ...interface{}) error {
	if len(vals) != b.valuesPerRow {
		return fmt.Errorf("ERROR: Inserted a wrong amount of values: Inserted: %v  Required: %v \n", len(vals), b.valuesPerRow)
	}
	b.placeholderStr += b.placeholderStrHelper
	b.vals = append(b.vals, vals...)
	b.rows++
	return nil
}
