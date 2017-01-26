// Copyright 2016 The shorm Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//Implements sql query operation

package shorm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

// Scalar 获取一个值
func (s *Session) Scalar(sql string, v interface{}, args ...interface{}) error {
	defer s.reset()
	s.group, _ = s.engine.cluster.DefaultGroup()
	node := s.group.GetNode()
	row := node.Db.QueryRow(sql, args...)
	return row.Scan(v)
}

func (s *Session) Count(model interface{}) (int64, error) {
	defer s.reset()
	table, err := getTableMeta(model)
	if err != nil {
		return 0, err
	}
	sqlStr, args := s.sqlGen.GenCount(table, s.clauseList)
	s.logger.Printf("sql:%s, args:%#v\r\n", sqlStr, args)
	if s.hasShardKey {
		return s.innerCountWithShardkey(sqlStr, args...)
	}
	if s.engine.cluster.has1DbGroup() {
		s.group, _ = s.engine.cluster.DefaultGroup()
		return s.innerCountWithShardkey(sqlStr, args...)
	}

	return s.innerCountWithoutShardkey(sqlStr, args...)
}

func (s *Session) innerCountWithShardkey(sqlStr string, args ...interface{}) (int64, error) {
	var node *DbNode
	var err error
	if s.forceMaster {
		node, err = s.group.GetMaster()
		if err != nil {
			return 0, err
		}
	} else {
		node = s.group.GetNode()
	}
	s.logger.Println("Node name:", node.Name)
	row := node.Db.QueryRow(sqlStr, args...)
	var result int64
	err = row.Scan(&result)
	return result, err
}

func (s *Session) innerCountWithoutShardkey(sqlStr string, args ...interface{}) (int64, error) {
	ch_row := make(chan int64, s.engine.cluster.RealGroups)
	for _, dg := range s.engine.cluster.Groups {
		var node *DbNode
		if s.forceMaster {
			node, _ = dg.GetMaster()
		} else {
			node = dg.GetNode()
		}
		go func() {
			s.logger.Println("execute sql query againt db:", node.Name)
			row := node.Db.QueryRow(sqlStr, args...)
			var result int64
			row.Scan(&result)
			ch_row <- result
		}()
	}
	var retResult int64
	count := 0
Loop:
	for {
		select {
		case rslt := <-ch_row:
			retResult += rslt
			count++
			if count >= s.engine.cluster.RealGroups {
				break Loop
			}
			continue Loop
		case <-time.After(time.Second * 30):
			break Loop
		}
	}
	return retResult, nil
}

//Retrieve one record
func (s *Session) Get(model interface{}) (bool, error) {
	defer s.reset()
	table, err := getTableMeta(model)
	if err != nil {
		return false, err
	}
	s.clauseList = append(s.clauseList, sqlClause{op: opType_top, params: []interface{}{1}})
	sqlStr, args := s.sqlGen.GenSelect(table, s.clauseList)
	s.logger.Printf("sql:%s, args:%#v\r\n", sqlStr, args)
	var rows *sql.Rows
	if s.hasShardKey {
		rows, err = s.innerGetWithShardKey(sqlStr, args...)
	} else {
		if s.engine.cluster.has1DbGroup() {
			s.group, _ = s.engine.cluster.DefaultGroup()
			rows, err = s.innerGetWithShardKey(sqlStr, args...)
		} else {
			rows, err = s.innerGetWithoutShardKey(sqlStr, args...)
		}
	}
	if err != nil {
		return false, nil
	}
	var valuePair valuePairList
	if valuePair, err = row2Slice(rows, table.Columns); err != nil {
		return false, err
	}
	if err = toStruct(valuePair, model); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Session) innerGetWithShardKey(sqlStr string, args ...interface{}) (*sql.Rows, error) {
	var node *DbNode
	var err error
	if s.forceMaster {
		node, err = s.group.GetMaster()
		if err != nil {
			return nil, err
		}
	} else {
		node = s.group.GetNode()
	}
	s.logger.Println("Node name:", node.Name)
	var rows *sql.Rows
	rows, err = node.Db.Query(sqlStr, args...)
	if err != nil {
		if rows != nil {
			rows.Close()
		}
		return nil, err
	}
	if !rows.Next() {
		rows.Close()
		return nil, sql.ErrNoRows
	}
	return rows, nil
}

func (s *Session) innerGetWithoutShardKey(sqlstr string, args ...interface{}) (*sql.Rows, error) {
	ch_row := make(chan *sql.Rows, s.engine.cluster.RealGroups)
	for _, dg := range s.engine.cluster.Groups {
		var node *DbNode
		if s.forceMaster {
			node, _ = dg.GetMaster()
		} else {
			node = dg.GetNode()
		}
		go func() {
			s.logger.Println("execute sql query againt db:", node.Name)
			if rows, err := node.Db.Query(sqlstr, args...); err != nil {
				if rows != nil {
					rows.Close()
				}
				ch_row <- nil
			} else {
				if rows.Next() {
					ch_row <- rows
				} else {
					rows.Close()
					ch_row <- nil
				}

			}
		}()
	}
	count := 0
Loop:
	for {
		select {
		case row := <-ch_row:
			if row != nil {
				return row, nil
			}
			count++
			if count >= s.engine.cluster.RealGroups {
				return nil, sql.ErrNoRows
			}
			continue Loop
		case <-time.After(time.Second * 30):
			return nil, fmt.Errorf("query timeout")
		}
	}
}

//Find implements querying multiple recrods according to search criteria
func (s *Session) Find(slicePtr interface{}) error {
	defer s.reset()
	slice := reflect.Indirect(reflect.ValueOf(slicePtr))
	if slice.Kind() != reflect.Slice {
		return fmt.Errorf("slicePtr must be a pointer to slice")
	}
	elementType := slice.Type().Elem()

	if elementType.Kind() == reflect.Ptr {
		elementType = elementType.Elem()
	}
	if elementType.Kind() != reflect.Struct {
		return fmt.Errorf("element type must be struct")
	}
	table, err := getTableMeta(reflect.New(elementType).Interface())
	if err != nil {
		return err
	}

	sqlstr, args := s.sqlGen.GenSelect(table, s.clauseList)
	s.logger.Printf("sql:%s, args:%#v\r\n", sqlstr, args)
	if !(strings.Contains(sqlstr, "where") || strings.Contains(sqlstr, "limit")) {
		return fmt.Errorf("'%s',table scan, DANGEROUS!", sqlstr)
	}
	var valuePair valuePairList
	if !(s.hasShardKey || s.engine.cluster.has1DbGroup()) {
		row_ch := s.innerFindWithoutShardKey(sqlstr, args...)
		for row := range row_ch {
			if row == nil {
				continue
			}
			if valueList, err2 := row2Slice(row, table.Columns); err2 != nil {
				continue
			} else {
				valuePair = append(valuePair, valueList...)
			}
		}
	} else {
		var rows *sql.Rows
		if !s.hasShardKey {
			s.group, _ = s.engine.cluster.DefaultGroup()
		}
		rows, err = s.innerGetWithShardKey(sqlstr, args...)
		if err == sql.ErrNoRows {
			return nil
		} else if err != nil {
			return err
		}
		if valuePair, err = row2Slice(rows, table.Columns); err != nil {
			return err
		}
	}

	return toStructList(valuePair, slicePtr)
}

//Exec query against all db groups, and will merge all results as final output
func (s *Session) innerFindWithoutShardKey(sqlstr string, args ...interface{}) chan *sql.Rows {
	ch_row := make(chan *sql.Rows, s.engine.cluster.RealGroups)
	wg := &sync.WaitGroup{}
	for _, dg := range s.engine.cluster.Groups {
		var node *DbNode
		if s.forceMaster {
			node, _ = dg.GetMaster()
		} else {
			node = dg.GetNode()
		}
		wg.Add(1)
		go func(dbNode *DbNode, ch chan *sql.Rows) {
			defer wg.Done()
			s.logger.Println("execute sql query againt db:", dbNode.Name)
			rows, err := node.Db.Query(sqlstr, args...)
			if err != nil {
				if rows != nil {
					rows.Close()
				}
				ch <- nil
			} else {
				if rows.Next() {
					ch <- rows
				} else {
					rows.Close()
					ch <- nil
				}
			}
		}(node, ch_row)
	}
	go func(ch chan *sql.Rows) {
		wg.Wait()
		close(ch)
	}(ch_row)

	return ch_row
}
