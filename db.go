package awql_db

import (
	"errors"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"path/filepath"
	"strings"
)

const (
	refReportPath = "./src/%s/reports.yml"
	refViewPath   = "./src/views.yml"
)

// DataSchema represents a basic data table.
type DataSchema interface {
	AggregateColumn() string
	ColumnByName(column string) (Field, error)
	Columns() []Field
	SourceName() string
}

// DataSchemaView represents a data view table.
type DataSchemaView interface {
	DataSchema
	ConditionList() []Condition
	DuringList() []string
	GroupList() []int
	OrderList() []Ordering
	StartIndex() int
	PageSize() int
}

// DataSchema represents all data schema.
type Schema struct {
	DataTable
	DataView
}

// Database represents the database.
type Database struct {
	Version string
	fields  map[string][]DataSchema
	s       Schema
	ready   bool
}

// NewParser returns a new instance of Parser.
func NewDb(version string) *Database {
	return &Database{Version: version}
}

// AddView create a view in the database.
// Writes it to config file and adds it to current database.
// It return on error if the view can not be saved.
func (d *Database) AddView(v *View, replace bool) error {
	// Checks if the view already exists.
	if t, err := d.Table(v.Name); err == nil {
		if !replace {
			return errors.New("DatabaseError.TABLE_ALREADY_EXISTS")
		}
		if _, ok := t.(DataSchemaView); !ok {
			return errors.New("DatabaseError.TABLE_ALREADY_EXISTS")
		}
	}
	// Updates the views configuration file.
	var dv DataView
	dv.Views = append(d.s.Views, *v)
	if err := ioutil.WriteFile(refViewPath, []byte(dv.String()), 0644); err != nil {
		return err
	}
	d.s.Views = dv.Views

	return nil
}

// Load loads all dependencies of the database.
func (d *Database) Load() error {
	if d.ready {
		// Schema already loaded.
		return nil
	}
	if err := d.loadReports(); err != nil {
		return errors.New("DatabaseError.TABLES")
	}
	if err := d.loadViews(); err != nil {
		return errors.New("DatabaseError.VIEWS")
	}
	if err := d.buildColumnsIndex(); err != nil {
		return errors.New("DatabaseError.COLUMNS")
	}
	d.ready = true

	return nil
}

// Table returns the table by its name or an error if it not exists.
func (d *Database) Table(table string) (DataSchema, error) {
	// Search in all reports.
	for i, t := range d.s.Tables {
		if t.Name == table {
			return &d.s.Tables[i], nil
		}
	}
	// Search in Views
	for i, v := range d.s.Views {
		if v.Name == table {
			return &d.s.Views[i], nil
		}
	}
	return nil, errors.New("DatabaseError.UNKNOWN_TABLE")
}

// TablesPrefixedBy returns the list of tables prefixed by this pattern.
func (d *Database) TablesContains(pattern string) (tables []DataSchema) {
	// Search in all reports.
	for i, t := range d.s.Tables {
		if strings.Contains(t.Name, pattern) {
			tables = append(tables, &d.s.Tables[i])
		}
	}
	// Search in Views
	for i, v := range d.s.Views {
		if strings.Contains(v.Name, pattern) {
			tables = append(tables, &d.s.Views[i])
		}
	}
	return tables
}

// TablesPrefixedBy returns the list of tables prefixed by this pattern.
func (d *Database) TablesPrefixedBy(pattern string) (tables []DataSchema) {
	// Search in all reports.
	for i, t := range d.s.Tables {
		if strings.HasPrefix(t.Name, pattern) {
			tables = append(tables, &d.s.Tables[i])
		}
	}
	// Search in Views
	for i, v := range d.s.Views {
		if strings.HasPrefix(v.Name, pattern) {
			tables = append(tables, &d.s.Views[i])
		}
	}
	return tables
}

// TablesSuffixedBy returns the list of tables suffixed by this pattern.
func (d *Database) TablesSuffixedBy(pattern string) (tables []DataSchema) {
	// Search in all reports.
	for i, t := range d.s.Tables {
		if strings.HasSuffix(t.Name, pattern) {
			tables = append(tables, &d.s.Tables[i])
		}
	}
	// Search in Views
	for i, v := range d.s.Views {
		if strings.HasSuffix(v.Name, pattern) {
			tables = append(tables, &d.s.Views[i])
		}
	}
	return tables
}

// WithColumn returns the list of tables using this column.
func (d *Database) TablesWithColumn(column string) []DataSchema {
	return d.fields[column]
}

// buildColumnsIndex lists for each column the tables using it.
func (d *Database) buildColumnsIndex() error {
	// Returns on error if there is no table.
	if len(d.s.Tables) == 0 {
		return errors.New("DatabaseError.NO_TABLE")
	}
	// References for each column, the tables using it.
	d.fields = make(map[string][]DataSchema)
	for i, t := range d.s.Tables {
		for _, c := range t.Fields {
			d.fields[c.Name] = append(d.fields[c.Name], &d.s.Tables[i])
		}
	}
	// Also adds columns of the views.
	var name string
	for i, v := range d.s.Views {
		for _, column := range v.Fields {
			if column.Alias != "" {
				name = column.Alias
			} else {
				name = column.Name
			}
			d.fields[name] = append(d.fields[name], &d.s.Views[i])
		}
	}
	return nil
}

// loadReports loads all report table and returns it as Database or error.
func (d *Database) loadReports() error {
	ymlFile, err := d.loadFile(fmt.Sprintf(refReportPath, d.Version))
	if err != nil {
		return err
	}
	if err := yaml.Unmarshal(ymlFile, &d.s.DataTable); err != nil {
		return err
	}
	return nil
}

// loadReports loads all report table and returns it as Database or error.
func (d *Database) loadViews() error {
	ymlFile, err := d.loadFile(refViewPath)
	if err != nil {
		return err
	}
	if err := yaml.Unmarshal(ymlFile, &d.s.DataView); err != nil {
		return err
	}
	// Adds table properties to the view.
	for _, view := range d.s.Views {
		t, err := d.Table(view.TableName)
		if err != nil {
			return err
		}
		// Merges column properties of the view with the table.
		var fields []Field
		for _, c := range view.Fields {
			field, err := t.ColumnByName(c.Name)
			if err != nil {
				return nil
			}
			field.Alias = c.Alias
			fields = append(fields, field)
		}
		view.Fields = fields
	}
	return nil
}

// loadFile
func (d *Database) loadFile(path string) ([]byte, error) {
	// Gets path of reports reference.
	p, err := filepath.Abs(path)
	if err != nil {
		return []byte{}, err
	}
	// Gets reference in yaml format.
	ymlFile, err := ioutil.ReadFile(p)
	if err != nil {
		return []byte{}, err
	}
	return ymlFile, nil
}
