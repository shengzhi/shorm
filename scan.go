// Copyright 2016 The shorm Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//Scan db type to go type

package shorm

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type valuePairList []valuePairs

type valuePairs []valuePair

type valuePair struct {
	pindex, index             []int
	value                     interface{}
	specialType               int8
	isNullable, isDBConverter bool
}

func row2Slice(rows *sql.Rows, colMap ColMetadataMap) (valuePairList, error) {
	defer rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rowCols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	values := make([]interface{}, 0, len(rowCols))
	result := make(valuePairList, 0)
	pairs := make(valuePairs, len(rowCols))

	for i := range rowCols {
		if v, ok := colMap[strings.ToLower(rowCols[i])]; ok {
			values = append(values, reflect.New(v.dbType).Interface())
			// fmt.Println(v.name, v.dbType.Name(), v.parentFieldIndex)
			pairs[i] = valuePair{
				pindex:        v.parentFieldIndex,
				index:         v.fieldIndex,
				specialType:   v.specialType,
				isNullable:    v.isNullable,
				isDBConverter: v.isDBConverter,
			}
		} else {
			values = append(values, &sql.RawBytes{})
		}
	}

	var pairRow valuePairs
	fnParseValue := func(vals []interface{}) {
		pairRow = make(valuePairs, len(pairs))
		for i, v := range vals {
			if len(pairs[i].index) <= 0 {
				continue
			}
			pairRow[i].pindex = pairs[i].pindex
			pairRow[i].index = pairs[i].index
			pairRow[i].specialType = pairs[i].specialType
			pairRow[i].isNullable = pairs[i].isNullable
			pairRow[i].isDBConverter = pairs[i].isDBConverter
			if pairs[i].specialType == specialType_rawbytes {
				rawBytes := reflect.ValueOf(v).Elem().Interface().(sql.RawBytes)
				slice := make([]byte, 0, len(rawBytes))
				pairRow[i].value = append(slice, rawBytes...)
			} else {
				pairRow[i].value = reflect.ValueOf(v).Elem().Interface()
			}
		}
		result = append(result, pairRow)
	}

	//before call this function, rows.Next() has been called
	if err = rows.Scan(values...); err != nil {
		return nil, err
	}
	fnParseValue(values)

	for rows.Next() {
		if err = rows.Scan(values...); err != nil {
			return nil, err
		}
		fnParseValue(values)
	}
	return result, nil
}

func toStruct(list valuePairList, model interface{}) error {
	value := reflect.ValueOf(model)
	return assignValueToStruct(list[0], value)
}

func toStructList(list valuePairList, slicePtr interface{}) error {
	slice := reflect.Indirect(reflect.ValueOf(slicePtr))
	if slice.Kind() != reflect.Slice {
		return fmt.Errorf("slicePtr must be pointer of slice")
	}
	elementType := slice.Type().Elem()

	isElePtrType := false
	if elementType.Kind() == reflect.Ptr {
		elementType = elementType.Elem()
		isElePtrType = true
	}
	if elementType.Kind() != reflect.Struct {
		return fmt.Errorf("element type must be struct")
	}

	var err error
	for i := range list {
		element := reflect.New(elementType)
		err = assignValueToStruct(list[i], element)
		if err != nil {
			return err
		}
		if isElePtrType {
			slice.Set(reflect.Append(slice, element))
		} else {
			slice.Set(reflect.Append(slice, reflect.Indirect(element)))
		}
	}
	return nil
}

func assignValueToStruct(pairs []valuePair, val reflect.Value) error {
	// fmt.Println(pairs)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return fmt.Errorf("val must be a struct")
	}
	for i := range pairs {
		if len(pairs[i].index) <= 0 {
			continue
		}
		if pairs[i].value == nil {
			continue
		}
		var field reflect.Value
		if pairs[i].pindex == nil || len(pairs[i].pindex) <= 0 {
			field = val.FieldByIndex(pairs[i].index)
		} else {
			field = val.FieldByIndex(pairs[i].pindex)
			field = field.FieldByIndex(pairs[i].index)
		}

		switch field.Kind() {
		case reflect.Ptr:
			if pairs[i].isDBConverter {
				newObj := reflect.New(field.Type().Elem())
				newObj.Interface().(Unmarshaler).FromDB(pairs[i].value.([]byte))
				field.Set(newObj)
			} else {
				json.Unmarshal(pairs[i].value.([]byte), field.Addr().Interface())
			}
		case reflect.Struct, reflect.Slice:
			if pairs[i].specialType == specialType_time {
				t := pairs[i].value.(*time.Time)
				if t == nil {
					break
				}
				field.Set(reflect.ValueOf(*t))
			} else {
				if pairs[i].isDBConverter {
					field.Addr().Interface().(Unmarshaler).FromDB(pairs[i].value.([]byte))
				} else {
					json.Unmarshal(pairs[i].value.([]byte), field.Addr().Interface())
				}
			}
		case reflect.String:
			if pairs[i].isNullable {
				field.SetString(pairs[i].value.(sql.NullString).String)
			} else {
				field.Set(reflect.ValueOf(pairs[i].value))
			}
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
			if pairs[i].isNullable {
				field.SetUint(uint64(pairs[i].value.(sql.NullInt64).Int64))
			} else {
				field.Set(reflect.ValueOf(pairs[i].value))
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if pairs[i].isNullable {
				field.SetInt(pairs[i].value.(sql.NullInt64).Int64)
			} else {
				field.Set(reflect.ValueOf(pairs[i].value))
			}
		case reflect.Float32, reflect.Float64:
			if pairs[i].isNullable {
				field.SetFloat(pairs[i].value.(sql.NullFloat64).Float64)
			} else {
				field.Set(reflect.ValueOf(pairs[i].value))
			}
		case reflect.Bool:
			if pairs[i].isNullable {
				field.SetBool(pairs[i].value.(sql.NullBool).Bool)
			} else {
				field.Set(reflect.ValueOf(pairs[i].value))
			}
		default:
			field.Set(reflect.ValueOf(pairs[i].value))
		}
	}
	return nil
}
