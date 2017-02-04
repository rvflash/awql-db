# Awql Database

[![GoDoc](https://godoc.org/github.com/rvflash/awql-db?status.svg)](https://godoc.org/github.com/rvflash/awql-db)
[![Build Status](https://img.shields.io/travis/rvflash/awql-db.svg)](https://travis-ci.org/rvflash/awql-db)
[![Code Coverage](https://img.shields.io/codecov/c/github/rvflash/awql-db.svg)](http://codecov.io/github/rvflash/awql-db?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/rvflash/awql-db)](https://goreportcard.com/report/github.com/rvflash/awql-db)


All information about Adwords reports represented as tables or user views. 

## Example
 
```go
import db "github.com/rvflash/awql-db"

awql, _ := db.Open("v201609")
for _, table := range awql.TablesPrefixedBy("VIDEO") {
    fmt.Println(table.SourceName())
}
// Output: VIDEO_PERFORMANCE_REPORT
```