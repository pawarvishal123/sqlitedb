package sqlitedb

import (
	"github.com/TIBCOSoftware/flogo-lib/core/activity"
	"github.com/TIBCOSoftware/flogo-lib/logger"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"strings"
	"fmt"
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
	}
	defer db.Close()
	
	if params, ok := context.GetInput("Parameters").(map[string]string); ok && len(params) > 0 {
		for key, value := range params {
			query = strings.Replace(query, "?"+key, "'"+value+"'", -1)
		}
	}
	
	result, err := db.Exec(query)
	if err != nil {
		fmt.Errorf("%q: %s\n", err, query)
		return
	}

	
	context.SetOutput("Result", result)
	fmt.Debugf("Query execution successful..")
	return true, nil
}
