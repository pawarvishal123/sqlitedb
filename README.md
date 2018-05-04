# SQLite DB Activity
This activity allows you to perform create table, insert, update and delete oeprations on SQLite DB.
To build this activity on windows, gcc executable is required in PATH. MingW can be used.

## Installation
### Flogo CLI
```bash
flogo install github.com/pawarvishal123/sqlitedb
```

## Schema
Inputs and Outputs:

```json
{
  "input":[
    {
      "name": "DBName",
      "type": "string",
      "required": true
    },
    {
      "name": "Query",
      "type": "string",
      "required": true
    },
    {
      "name": "Parameters",
      "type": "params"
    }
  ],
  "output": [
    {
      "name": "Result",
      "type": "any"
    }
  ]
}
```

## Settings
| Setting     | Required | Description |
|:------------|:---------|:------------|
| DBName  | True     | The name of SQLite DB file |
| Query       | True     | SQL statement |
| Parameters     | False     | Parameters used in SQL statement (Please refer below sample input)|

## Example Inputs
```json
{
            "id": "sqlitedb_3",
            "name": "SQLite DB Operations (2)",
            "description": "Create, insert, update & delete from SQLite DB",
            "activity": {
              "ref": "github.com/pawarvishal123/sqlitedb",
              "input": {
                "DBName": "test2",
                "Query": "insert into emp(id, name) values(?id, ?name)",
                "Parameters": {
                  "id": "11",
                  "name": "Vishal"
                }
              }
            }
          }
```

## Third Party Library
SQLite API in Go - [https://github.com/mattn/go-sqlite3](https://github.com/mattn/go-sqlite3)
