package awql_db

import (
	"errors"
	"fmt"
	"strings"
)

const (
	newline = "\n"
	sep     = "  "
	dsep    = sep + sep
)

// Condition represents a condition.
type Condition struct {
	ColumnName     string   `yaml:"coln"`
	Operator       string   `yaml:"oprt"`
	Value          []string `yaml:"cval"`
	IsValueLiteral bool     `yaml:"lval,omitempty"`
}

// Ordering represents an order clause.
type Ordering struct {
	ColumnPosition int  `yaml:"cpos"`
	SortDesc       bool `yaml:"desc,omitempty"`
}

// Limit represents the limit clause.
type Limit struct {
	Offset   int `yaml:"oset,omitempty"`
	RowCount int `yaml:"rcnt"`
}

// DataView represents the database's views.
type DataView struct {
	Views []View
}

// String outputs the list of views.
func (dv DataView) String() (s string) {
	s = "views:" + newline
	for _, v := range dv.Views {
		s += v.String()
	}
	return
}

// View represents a view.
type View struct {
	Name       string
	PrimaryKey string      `yaml:"aggr,omitempty"`
	Fields     []Field     `yaml:"cols"`
	TableName  string      `yaml:"rprt"`
	Where      []Condition `yaml:",omitempty"`
	During     []string    `yaml:",omitempty"`
	GroupBy    []int       `yaml:"group,omitempty"`
	OrderBy    []Ordering  `yaml:"order,omitempty"`
	Limit      Limit       `yaml:",omitempty"`
}

// Aggregate implements DataSchema.
func (v *View) AggregateColumn() string {
	return v.PrimaryKey
}

// ColumnByName returns the field with this column name or an error.
// It implements DataSchema.
func (v *View) ColumnByName(column string) (Field, error) {
	for _, c := range v.Fields {
		if c.Name == column {
			return c, nil
		} else if c.Alias != "" && c.Alias == column {
			return c, nil
		}
	}
	return Field{}, errors.New("DatabaseError.UNKNOWN_COLUMN")
}

// Columns implements DataSchema.
func (v *View) Columns() []Field {
	return v.Fields
}

// SourceName implements DataSchema.
func (v *View) SourceName() string {
	return v.Name
}

// ConditionList implements DataSchemaView.
func (v *View) ConditionList() []Condition {
	return v.Where
}

// ConditionList implements DataSchemaView.
func (v *View) DuringList() []string {
	return v.During
}

// ConditionList implements DataSchemaView.
func (v *View) GroupList() []int {
	return v.GroupBy
}

// ConditionList implements DataSchemaView.
func (v *View) OrderList() []Ordering {
	return v.OrderBy
}

// ConditionList implements DataSchemaView.
func (v *View) StartIndex() int {
	return v.Limit.Offset
}

// ConditionList implements DataSchemaView.
func (v *View) PageSize() int {
	return v.Limit.RowCount
}

// String represents a view in order to save it as plain text.
// It implements fmt.Stringer interface.
//
// Output example:
//   - name: ADGROUP_DAILY
//     rprt: ADGROUP_PERFORMANCE_REPORT
//     cols:
//       - name: AdGroupId
//         psnm: Id
//     where:
//       - coln: Impressions
//         oprt: >
//         cval: [0]
//         lval: true
//     during: [LAST_30_DAYS]
//     group: []
//     order:
//       - cpos: 1
//         desc: false
//     limit:
//       offs: 0
//       rcnt: 15
//
func (v *View) String() (s string) {
	if v.Name == "" || v.TableName == "" || len(v.Fields) == 0 {
		return
	}
	// Name of the view
	s = sep + "- name: " + v.Name + newline
	// Report name at the  source of the view
	s += dsep + "rprt: " + v.TableName + newline
	// List of columns
	s += dsep + "cols:" + newline
	for _, c := range v.Fields {
		s += dsep + sep + "- name: " + c.Name + newline
		if c.Alias != "" {
			s += dsep + dsep + "psnm: " + c.Alias + newline
		}
	}
	// Conditions
	if len(v.Where) > 0 {
		s += dsep + "where:" + newline
		for _, c := range v.Where {
			s += dsep + sep + "- coln: " + c.ColumnName + newline
			s += dsep + dsep + "oprt: " + c.Operator + newline
			s += dsep + dsep + "cval: [" + strings.Join(c.Value, ", ") + "]" + newline
			if c.IsValueLiteral {
				s += dsep + dsep + "lval: true" + newline
			}
		}
	}
	// During range date
	if len(v.During) > 0 {
		s += dsep + "during: [" + strings.Join(v.During, ", ") + "]" + newline
	}
	// Group by columns
	if len(v.GroupBy) > 0 {
		s += dsep + "group: " + intJoin(v.GroupBy, ", ") + newline
	}
	// Order by columns
	if len(v.OrderBy) > 0 {
		s += dsep + "order:" + newline
		for _, c := range v.OrderBy {
			s += dsep + sep + "- cpos: " + string(c.ColumnPosition) + newline
			if c.SortDesc {
				s += dsep + dsep + "desc: true" + newline
			}
		}
	}
	// Limit clause
	if v.Limit.RowCount > 0 {
		s += dsep + "limit:"
		s += dsep + sep + "offs:" + string(v.Limit.Offset) + newline
		s += dsep + sep + "rcnt:" + string(v.Limit.RowCount) + newline
	}
	return
}

// intJoin concatenates the integer of a to create a single string.
// The separator string sep is placed between elements in the resulting string.
func intJoin(a []int, sep string) string {
	return strings.Join(strings.Fields(fmt.Sprint(a)), sep)
}
