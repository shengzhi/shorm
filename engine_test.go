// Copyright 2016 The shorm Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package shorm

import (
	"os"
	"testing"

	_ "github.com/denisenkom/go-mssqldb"
)

var connstr = "server=139.196.7.213;port=3341;database=TicketSales_UAT;user id=sa;password=1Q2w3e4r;connection timeout=30;dial timeout=15;log=1;encrypt=disable"
var connstr2 = "server=192.168.0.128;database=DX_Fly;user id=sa;password=today123;connection timeout=30;dial timeout=15;log=1;encrypt=disable"

type FlyOrder struct {
	TableName  TableName `shorm:"FL_Order"`
	Id         int64     `shorm:",pk,auto"`
	SerialNo   string
	OriginCity string
	DestCity   string
}

var e *Engine

func setup() {
	cluster := &Cluster{
		TotalGroups: 5,
		RealGroups:  2,
	}
	cluster.Groups = append(cluster.Groups,
		&KeyDbGroupMapping{
			RangeFrom: 1,
			RangeTo:   3,
			Group: &DbGroup{
				Name: "Group1",
				Nodes: []*DbNode{
					{
						Name:    "G1_Master",
						ConnStr: connstr,
						Type:    NodeType_Master,
					},
					{
						Name:    "G1_Node1",
						ConnStr: connstr,
						Type:    NodeType_Slave,
					},
				},
			},
			IsDefault: true,
		})
	cluster.Groups = append(cluster.Groups,
		&KeyDbGroupMapping{
			RangeFrom: 4,
			RangeTo:   5,
			Group: &DbGroup{
				Name: "Group2",
				Nodes: []*DbNode{
					{
						Name:    "G2_Master",
						ConnStr: connstr2,
						Type:    NodeType_Master,
					},
					{
						Name:    "G2_Node1",
						ConnStr: connstr2,
						Type:    NodeType_Slave,
					},
				},
			},
		})
	e = NewEngine("mssql", cluster)
	e.Logger.Println(e.Open())
}

func shutdown() {
	e.Close()
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	shutdown()
	os.Exit(code)
}

func BenchmarkGet(b *testing.B) {
	s := e.StartSession()
	for i := 0; i < b.N; i++ {
		model := FlyOrder{}
		_, err := s.ShardValue(9).Id(5).Get(&model)
		if err != nil {
			b.Fatal(err)
		}
	}
	e.EndSession(s)
}

func BenchmarkGetWihtoutShardKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := e.StartSession()
		model := FlyOrder{}
		has, err := s.Id(10).Get(&model)
		if !has || err != nil {
			b.Fail()
		}
		e.EndSession(s)
	}
}

func BenchmarkFindWithShardKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := e.StartSession()
		var slice []*FlyOrder
		err := s.ShardValue(9).Where("id <=?", 100).Limit(10, 10).Find(&slice)
		if err != nil {
			b.Fail()
		}
		e.EndSession(s)
	}
}

func BenchmarkFindWithoutShardKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := e.StartSession()
		var slice []*FlyOrder
		err := s.Where("id <=?", 100).Limit(10, 10).Find(&slice)
		if err != nil {
			b.Fail()
		}
		e.EndSession(s)
	}
}
