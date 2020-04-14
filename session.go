// Copyright 2016 The shorm Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package shorm

import (
	"log"
	"reflect"
	"strings"
)

type Session struct {
	group       *DbGroup
	clauseList  sqlClauseList
	sqlGen      SqlGenerator
	logger      *log.Logger
	engine      *Engine
	hasShardKey bool //if specified shard key
	isWrite     bool //True when insert,update,delete action, else false
	forceMaster bool //force to execute sql against master node
}

// Copy 复制session
func (s *Session) Copy() *Session {
	copy := &Session{
		group:       s.group,
		engine:      s.engine,
		logger:      s.logger,
		sqlGen:      s.sqlGen,
		hasShardKey: s.hasShardKey,
		isWrite:     false,
		forceMaster: s.forceMaster,
	}
	for _, clause := range s.clauseList {
		copy.clauseList = append(copy.clauseList, sqlClause{
			op:     clause.op,
			clause: clause.clause,
			params: clause.params,
		})
	}
	return copy
}

func (s *Session) reset() {
	s.group = nil
	s.clauseList = nil
	s.hasShardKey = false
	s.isWrite = false
	s.forceMaster = false
}

func (s *Session) ShardValue(value int64) *Session {
	var has bool
	if s.group, has = s.engine.cluster.findGroup(value); !has {
		s.group, _ = s.engine.cluster.DefaultGroup()
	} else {
		s.hasShardKey = true
	}
	return s
}

//Force to touch master db fo group
func (s *Session) ForseMaster() *Session {
	s.forceMaster = true
	return s
}

func (s *Session) Query(query string, args ...interface{}) *Session {
	subSql := sqlClause{
		op:     opType_rawQuery,
		clause: query,
	}
	subSql.params = append(subSql.params, args...)
	s.clauseList = append(s.clauseList, subSql)
	return s
}

func (s *Session) Exec(sql string, args ...interface{}) *Session {
	return s.Query(sql, args...)
}

func (s *Session) Id(id interface{}) *Session {
	s.clauseList = append(s.clauseList, sqlClause{
		op:     opType_id,
		params: []interface{}{id},
	})
	return s
}

func (s *Session) Table(tablename string) *Session {
	s.clauseList = append(s.clauseList, sqlClause{
		op:     opType_table,
		clause: tablename,
	})
	return s
}

func (s *Session) Where(clause string, args ...interface{}) *Session {
	subSql := sqlClause{
		op:     opType_where,
		clause: clause,
	}
	if len(args) > 0 {
		subSql.params = append(subSql.params, args...)
	}
	s.clauseList = append(s.clauseList, subSql)
	return s
}

func (s *Session) And(clause string, args ...interface{}) *Session {
	subSql := sqlClause{
		op:     opType_and,
		clause: clause,
	}
	if len(args) > 0 {
		subSql.params = append(subSql.params, args...)
	}
	s.clauseList = append(s.clauseList, subSql)
	return s
}

func (s *Session) Or(clause string, args ...interface{}) *Session {
	subSql := sqlClause{
		op:     opType_or,
		clause: clause,
	}
	if len(args) > 0 {
		subSql.params = append(subSql.params, args...)
	}
	s.clauseList = append(s.clauseList, subSql)
	return s
}

func (s *Session) In(colName string, args ...interface{}) *Session {
	subSql := sqlClause{
		op:     opType_in,
		clause: colName,
	}
	if len(args) > 0 {
		if len(args) > 1 {
			subSql.params = append(subSql.params, args...)
		} else {
			arg := args[0]
			val := reflect.ValueOf(arg)
			if val.Type().Kind() == reflect.Slice {
				for i := 0; i < val.Len(); i++ {
					subSql.params = append(subSql.params, val.Index(i).Interface())
				}
			} else {
				subSql.params = append(subSql.params, arg)
			}
		}

	}
	s.clauseList = append(s.clauseList, subSql)
	return s
}

func (s *Session) OrIn(colName string, args ...interface{}) *Session {
	subSql := sqlClause{
		op:     opType_in_or,
		clause: colName,
	}
	if len(args) > 0 {
		subSql.params = append(subSql.params, args...)
	}
	s.clauseList = append(s.clauseList, subSql)
	return s
}

func (s *Session) Between(colName string, args ...interface{}) *Session {
	subSql := sqlClause{
		op:     opType_between,
		clause: colName,
	}
	if len(args) > 0 {
		subSql.params = append(subSql.params, args...)
	}
	s.clauseList = append(s.clauseList, subSql)
	return s
}

func (s *Session) OrBetween(colName string, args ...interface{}) *Session {
	subSql := sqlClause{
		op:     opType_between_or,
		clause: colName,
	}
	if len(args) > 0 {
		subSql.params = append(subSql.params, args...)
	}
	s.clauseList = append(s.clauseList, subSql)
	return s
}

func (s *Session) Limit(skip, size int) *Session {
	subSql := sqlClause{
		op: opType_limit,
	}
	subSql.params = append(subSql.params, skip, size)
	s.clauseList = append(s.clauseList, subSql)
	return s
}

func (s *Session) OrderBy(orderby ...string) *Session {
	subSql := sqlClause{
		op:     opType_orderby,
		clause: strings.Join(orderby, ","),
	}
	s.clauseList = append(s.clauseList, subSql)
	return s
}

func (s *Session) Cols(cols string) *Session {
	subSql := sqlClause{
		op:     opType_cols,
		clause: cols,
	}
	s.clauseList = append(s.clauseList, subSql)
	return s
}

func (s *Session) Omit(cols string) *Session {
	subSql := sqlClause{
		op:     opType_omit,
		clause: cols,
	}
	s.clauseList = append(s.clauseList, subSql)
	return s
}

func (s *Session) UnlockTable() *Session {
	s.clauseList = append(s.clauseList, sqlClause{op: opType_unlockTable})
	return s
}
