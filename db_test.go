package awql_db_test

import (
	"testing"

	"github.com/rvflash/awql-db"
)

func TestNewDb(t *testing.T) {
	db := awql_db.NewDb("v201609")
	if err := db.Load(); err != nil {
		t.Errorf("Expected no error on loading tables and views properties, received %s", err)
	}
	if _, err := db.Table("AD_PERFORMANCE_REPORT"); err != nil {
		t.Errorf("Expected a table named AD_PERFORMANCE_REPORT, received %s", err)
	}
	if tables := db.TablesPrefixedBy("CAMPAIGN"); len(tables) != 8 {
		t.Error("Expected only 8 tables prefixed by 'CAMPAIGN'")
	}
	if tables := db.TablesSuffixedBy("_REPORT"); len(tables) != 45 {
		t.Error("Expected only 45 tables suffixed by '_REPORT'")
	}
	if tables := db.TablesContains("NEGATIVE"); len(tables) != 3 {
		t.Error("Expected only 3 tables with 'NEGATIVE' in its name")
	}
	if tables := db.TablesWithColumn("TrackingUrlTemplate"); len(tables) != 13 {
		t.Error("Expected 13 tables using TrackingUrlTemplate as column")
	}
}
