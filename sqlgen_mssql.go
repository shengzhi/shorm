// Copyright 2016 The shorm Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package shorm

import (
	"fmt"
	"sort"
	"strings"
)

type MSSqlGenerator struct {
	*BaseGenerator
}

func NewMSSqlGenerator() *MSSqlGenerator {
	g := MSSqlGenerator{BaseGenerator: newBaseGenerator()}
	g.wrapFunc = func(s string) string { return fmt.Sprintf("[%s]", s) }
	return &g
}

//Generates select SQL statement
func (m *MSSqlGenerator) GenSelect(table *TableMetadata, sqls sqlClauseList) (string, []interface{}) {
	buf := m.getBuf()
	defer m.putBuf(buf)
	var args []interface{}
	var colNames string
	var omitCols []string
	sqls = append(sqls, sqlClause{op: opType_table, clause: m.wrapColumn(table.Name)})
	sort.Sort(sqls)
	isPaging := false
	pagingOrder := table.IdColumn.name
	hasWhere := false
	var pagingParam []interface{}
	buf.WriteString("select ")
	for _, s := range sqls {
		switch s.op {
		case opType_rawQuery:
			return s.clause, s.params
		case opType_top:
			buf.WriteString(fmt.Sprintf("top %v ", s.params...))
		case opType_cols:
			colNames = s.clause
		case opType_omit:
			omitCols = strings.Split(strings.ToLower(s.clause), ",")
		case opType_table:
			buf.WriteString("%s")
			buf.WriteString(fmt.Sprintf(" from %v", s.clause))
		case opType_unlockTable:
			buf.WriteString(" with(nolock) ")
		case opType_id:
			if hasWhere {
				buf.WriteString(fmt.Sprintf(" and %s=?", table.IdColumn.name))
			} else {
				buf.WriteString(fmt.Sprintf(" where %s=?", table.IdColumn.name))
				hasWhere = true
			}
			args = append(args, s.params...)
		case opType_where:
			if hasWhere {
				buf.WriteString(fmt.Sprintf(" and %s", s.clause))
			} else {
				buf.WriteString(fmt.Sprintf(" where %s", s.clause))
				hasWhere = true
			}
			args = append(args, s.params...)
		case opType_and:
			buf.WriteString(fmt.Sprintf(" and %s", s.clause))
			args = append(args, s.params...)
		case opType_or:
			buf.WriteString(fmt.Sprintf(" or (%s)", s.clause))
			args = append(args, s.params...)
		case opType_in:
			if len(s.params) > 0 {
				if hasWhere {
					buf.WriteString(fmt.Sprintf(" and %s in (%s)", s.clause, m.makeInArgs(s.params)))
				} else {
					buf.WriteString(fmt.Sprintf(" where %s in (%s)", s.clause, m.makeInArgs(s.params)))
					hasWhere = true
				}
			}
		case opType_in_or:
			if len(s.params) > 0 {
				buf.WriteString(fmt.Sprintf(" or(%s in (%s))", s.clause, m.makeInArgs(s.params)))
			}
		case opType_between:
			if hasWhere {
				buf.WriteString(fmt.Sprintf(" and %s between ? and ?", s.clause))
			} else {
				buf.WriteString(fmt.Sprintf(" where %s between ? and ?", s.clause))
				hasWhere = true
			}

			args = append(args, s.params...)
		case opType_between_or:
			buf.WriteString(fmt.Sprintf(" or (%s between ? and ?)", s.clause))
			args = append(args, s.params...)
		case opType_limit:
			buf.WriteString("ROW_NUMBER() OVER (order by %s) as row,")
			isPaging = true
			pagingParam = s.params
		case opType_orderby:
			if isPaging {
				pagingOrder = s.clause
			} else {
				buf.WriteString(" order by ")
				buf.WriteString(s.clause)
			}
		default:
			break
		}
	}

	if len(colNames) <= 0 {
		cols := make([]string, 0, len(table.Columns))
		table.Columns.Foreach(func(colKey string, col *columnMetadata) {
			if col.rwType&io_type_ro == io_type_ro {
				if len(omitCols) > 0 {
					for i := range omitCols {
						if colKey == omitCols[i] {
							return
						}
					}
				}
				cols = append(cols, m.wrapColumn(col.name))
			}
		})
		colNames = strings.Join(cols, ",")
	}
	if isPaging {
		sqlStr := fmt.Sprintf("select top %[3]v * from (%[1]s) t where t.row > %[2]v",
			fmt.Sprintf(buf.String(), pagingOrder, colNames), pagingParam[0], pagingParam[1])
		return sqlStr, args
	}
	return fmt.Sprintf(buf.String(), colNames), args
}
