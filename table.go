package awql_db

import "errors"

// Field represents a column.
type Field struct {
	Name                     string
	Alias                    string   `yaml:"psnm,omitempty"`
	Kind                     string   `yaml:",omitempty"`
	IsSegment                bool     `yaml:"sgmt,omitempty"`
	SupportsZeroImpressions  bool     `yaml:"zero,omitempty"`
	ValueList                []string `yaml:"enum,omitempty,flow"`
	ColumnNamesNotCompatible []string `yaml:"notc,omitempty,flow"`
}

// DataTable represents the tables of the database.
type DataTable struct {
	Tables []Table `yaml:"reports"`
}

// Table represents a table.
type Table struct {
	Name       string
	PrimaryKey string  `yaml:"aggr,omitempty"`
	Fields     []Field `yaml:"cols"`
}

// Aggregate implements DataSchema.
func (t *Table) AggregateColumn() string {
	return t.PrimaryKey
}

// ColumnByName returns the field with this column name or an error.
// It implements DataSchema.
func (t *Table) ColumnByName(column string) (Field, error) {
	for _, c := range t.Fields {
		if c.Name == column {
			return c, nil
		}
	}
	return Field{}, errors.New("DatabaseError.UNKNOWN_COLUMN")
}

// Columns implements DataSchema.
func (t *Table) Columns() []Field {
	return t.Fields
}

// SourceName implements DataSchema.
func (t *Table) SourceName() string {
	return t.Name
}
