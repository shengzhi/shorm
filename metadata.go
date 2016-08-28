// Copyright 2016 The shorm Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//Implements that parsing, cache, get metadata action against model

package shorm

import (
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	tag_shorm   = "shorm"
	tag_ignore  = "-"
	tag_extends = "extends"
)

var meta_cache = make(map[uintptr]*TableMetadata, 0)
var lock = &sync.Mutex{}

func getTableMeta(val interface{}) (*TableMetadata, error) {
	itab := reflect.ValueOf(&val).Elem().InterfaceData()[0]
	// fmt.Println("shorm:", itab, reflect.TypeOf(val).Name())
	if v, ok := meta_cache[itab]; ok {
		// fmt.Println("cache hint!")
		return v, nil
	}
	lock.Lock()
	defer lock.Unlock()
	if v, ok := meta_cache[itab]; ok {
		return v, nil
	}
	value := reflect.ValueOf(val)
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return nil, fmt.Errorf("val must be a pointer to struct, but %s", value.Kind())
	}
	table := extractTableMetadata(value)
	meta_cache[itab] = table
	return table, nil
}

var shardingerType = reflect.TypeOf((*Shardinger)(nil)).Elem()

func extractTableMetadata(structVal reflect.Value) *TableMetadata {
	structType := structVal.Type()
	table := &TableMetadata{
		Name:         structType.Name(),
		Columns:      make(ColMetadataMap, 0),
		IsShardinger: structType.Implements(shardingerType),
	}

	extractColMetadata(table, structType)
	return table
}

func extractColMetadata(table *TableMetadata, structType reflect.Type) {
	tableNameType := reflect.TypeOf(new(TableName)).Elem()
	var field reflect.StructField
	for i := 0; i < structType.NumField(); i++ {
		field = structType.Field(i)
		if field.Type == tableNameType {
			table.Name = field.Tag.Get(tag_shorm)
			continue
		}
		tag := field.Tag.Get(tag_shorm)
		if tag == tag_ignore {
			continue
		}
		if tag == tag_extends {
			elementType := field.Type
			if field.Type.Kind() == reflect.Ptr {
				elementType = field.Type.Elem()
			}
			if elementType.Kind() == reflect.Struct {
				extractColMetadata(table, elementType)
			}
			continue
		}
		col := &columnMetadata{isNullable: true}
		col.convertFromField(field, tag)
		table.Columns.Add(strings.ToLower(col.name), col)
		if col.isKey {
			table.IdColumn = col
		}
		if col.isShardKey {
			table.ShardColumn = col
		}
	}
}

//Indicates the field is table name, similar with XMLName
type TableName string

//TableMetadata is used to save the mapping relationship between go type and db table
type TableMetadata struct {
	Name    string         //Table's name
	Columns ColMetadataMap //Table's column collection
	//IdColumn is the column as marked with 'pk' in 'shorm' tag.
	//ShardColumn is the column as marked with 'shard' in 'shorm' tag, it is used to calculate db group
	IdColumn, ShardColumn *columnMetadata
	IsShardinger          bool //Indicates if the type implements interface Shardinger
}

type ColMetadataMap map[string]*columnMetadata

func (c ColMetadataMap) Foreach(action func(string, *columnMetadata)) {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i := range keys {
		action(keys[i], c[keys[i]])
	}
}

func (c ColMetadataMap) Add(k string, v *columnMetadata) {
	if _, ok := c[k]; !ok {
		c[k] = v
	}
}

func (c ColMetadataMap) Union(other ColMetadataMap) {
	for k, v := range other {
		c.Add(k, v)
	}
}

const (
	specialType_time int8 = iota + 1
	specialType_rawbytes
)

const (
	io_type_ro = 0x01
	io_type_wo = 0x02
	io_type_rw = io_type_ro | io_type_wo
)

//data column metadata
type columnMetadata struct {
	name                                    string
	dbType                                  reflect.Type
	goType                                  reflect.Type
	fieldIndex                              []int
	isKey, isAutoId, isShardKey, isNullable bool
	rwType, specialType                     int8
}

var byteType = reflect.TypeOf(new(sql.RawBytes)).Elem()

func (c *columnMetadata) convertFromField(field reflect.StructField, tag string) {
	c.fieldIndex = field.Index
	c.goType = field.Type
	c.rwType = io_type_rw
	tagVals := strings.Split(tag, ",")
	if len(tagVals) <= 0 {
		c.name = field.Name
		return
	}
	if len(tagVals[0]) <= 0 {
		c.name = field.Name
	} else {
		c.name = tagVals[0]
	}
	for i := 1; i < len(tagVals); i++ {
		switch tagVals[i] {
		case "pk":
			c.isKey = true
		case "auto":
			c.isAutoId = true
		case "ro":
			c.rwType = io_type_ro
		case "wo":
			c.rwType = io_type_wo
		case "rw":
			c.rwType = io_type_rw
		case "shard":
			c.isShardKey = true
		case "notnull":
			c.isNullable = false
		}
	}
	switch field.Type.Kind() {
	case reflect.Slice, reflect.Struct:
		if field.Type.Name() == "Time" {
			c.dbType = reflect.TypeOf(&time.Time{})
			c.specialType = specialType_time
		} else {
			c.specialType = specialType_rawbytes
			c.dbType = byteType
		}
	case reflect.String:
		if c.isNullable {
			c.dbType = reflect.TypeOf(sql.NullString{})
		} else {
			c.dbType = c.goType
		}
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if c.isNullable {
			c.dbType = reflect.TypeOf(sql.NullInt64{})
		} else {
			c.dbType = c.goType
		}
	case reflect.Float32, reflect.Float64:
		if c.isNullable {
			c.dbType = reflect.TypeOf(sql.NullFloat64{})
		} else {
			c.dbType = c.goType
		}
	case reflect.Bool:
		if c.isNullable {
			c.dbType = reflect.TypeOf(sql.NullBool{})
		} else {
			c.dbType = c.goType
		}
	default:
		c.dbType = c.goType
	}

}

type DataType string
