# Awql Database

[![GoDoc](https://godoc.org/github.com/rvflash/awql-db?status.svg)](https://godoc.org/github.com/rvflash/awql-db)
[![Build Status](https://img.shields.io/travis/rvflash/awql-db.svg)](https://travis-ci.org/rvflash/awql-db)
[![Code Coverage](https://img.shields.io/codecov/c/github/rvflash/awql-db.svg)](http://codecov.io/github/rvflash/awql-db?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/rvflash/awql-db)](https://goreportcard.com/report/github.com/rvflash/awql-db)


All information about Adwords reports represented as tables or user views. 

## Example
 
 ```go
db := awql_db.NewDb("v201609")
// Ignores the check of error for the test
db.Load()
for _, t := range db.TablesPrefixedBy("VIDEO") {
    fmt.Println(t.SourceName())
}
// Output: VIDEO_PERFORMANCE_REPORT
 ```