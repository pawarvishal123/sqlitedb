package sqlitedb

import (
	"github.com/TIBCOSoftware/flogo-lib/core/activity"
	"github.com/TIBCOSoftware/flogo-lib/logger"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"strings"
	"fmt"
	"reflect"
)

// log is the default package logger
var flogoLogger = logger.GetLogger("activity-tibco-sqlitedb")

// SQLiteDBActivity is a stub for your Activity implementation
type SQLiteDBActivity struct {
	metadata *activity.Metadata
}

// NewActivity creates a new activity
func NewActivity(metadata *activity.Metadata) activity.Activity {
	flogoLogger.Debugf("SQLiteDB NewActivity")
	return &SQLiteDBActivity{metadata: metadata}
}

// Metadata implements activity.Activity.Metadata
func (a *SQLiteDBActivity) Metadata() *activity.Metadata {
	return a.metadata
}

// Eval implements activity.Activity.Eval
func (a *SQLiteDBActivity) Eval(context activity.Context) (done bool, err error) {
	flogoLogger.Debugf("SQLiteDB Eval")

	dbname := context.GetInput("DBName").(string)
	query := context.GetInput("Query").(string)
	
	db, err := sql.Open("sqlite3", "./"+ dbname +".db")
	if err != nil {
		fmt.Errorf("Error while opening DB file - %+v", err)
		return false, nil
	}
	defer db.Close()
	
	if params, ok := context.GetInput("Parameters").(map[string]string); ok && len(params) > 0 {
		for key, value := range params {
			query = strings.Replace(query, "?"+key, "'"+value+"'", -1)
		}
	}
	fmt.Printf("Query: %s\n", query)
	//if query is not select
	if strings.Index(query, "select") == -1 {
		result, err := db.Exec(query)
		if err != nil {
			fmt.Printf("%q: %s\n", err, query)
			return false, nil
		}

		rowCnt, err := result.RowsAffected()
 		if err != nil {
			fmt.Printf(err.Error())
			return false, nil
 		}
		context.SetOutput("Result", rowCnt)
		fmt.Printf("Result: %d\n", rowCnt)
	} else {
		rows, err := db.Query(query) 
		if err != nil {
			fmt.Printf(err.Error())
			return false, nil
 		}
		cols, err := rows.Columns()
		if err != nil {
			fmt.Printf(err.Error())
			return false, nil
 		}
		defer rows.Close()
		var result []map[string]interface{}
		for rows.Next() {
    			// Create a slice of interface{}'s to represent each column,
    			// and a second slice to contain pointers to each item in the columns slice.
    			columns := make([]interface{}, len(cols))
    			columnPointers := make([]interface{}, len(cols))
    			for i, _ := range columns {
        			columnPointers[i] = &columns[i]
    			}
    
    			// Scan the result into the column pointers...
    			if err := rows.Scan(columnPointers...); err != nil {
        			fmt.Printf(err.Error())
				return false, nil
    			}

    			// Create our map, and retrieve the value for each column from the pointers slice,
    			// storing it in the map with the name of the column as the key.
    			m := make(map[string]interface{})
    			for i, colName := range cols {
        			val := columnPointers[i].(*interface{})				
					//m[colName] = *val
					temp := *val
					//fmt.Print(temp)
					switch v := temp.(type) {
					case int64:
						fmt.Printf("Integer: %v", v)
						m[colName] = temp
					case float64:
						fmt.Printf("Float64: %v", v)
						m[colName] = temp
					case string:
						fmt.Printf("String: %v", v)
						m[colName] = temp
					case []uint8:
						fmt.Printf("%v", temp)
						str := string(temp.([]byte))
						fmt.Println(str)
						m[colName] = str
					default:
						fmt.Printf("Unknown type..")
						var r = reflect.TypeOf(v)
						fmt.Printf("Other:%v\n", r)
						m[colName] = temp						
					}
    			}
    
    			// Outputs: map[columnName:value columnName2:value2 columnName3:value3 ...] 
    			fmt.Println(m)
			result = append(result, m)
		}
		context.SetOutput("Result", result)
	}
	fmt.Println("Query execution successful..")
	return true, nil
}
