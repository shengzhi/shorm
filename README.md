# shorm
shorm is a orm tool wriitted in golang, which supports databse cluster and master-slave mode
# Installation
  go get github.com/shengzhi/shorm
# Features
---
- Struct -> table mapping support
- Fluent API
- Transaction support
- Both ORM and raw sql support
- Database cluster support
- Master-slave mode support
- Struce field support, will be stored as json string

# Drivers support
---
- MsSql: github.com/denisenkom/go-mssqldb

# Change log
---


# Quick start
---
- Create engine from code

```Go
    cluster := &Cluster{TotalGroups: 5}
	cluster.Groups = append(cluster.Groups,
		&DbGroup{
			IsDefault: true,
			RangeFrom: 0,
			RangeTo:   3,
			Name:      "Group1",
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
	)
	cluster.Groups = append(cluster.Groups,
		&DbGroup{
			RangeFrom: 3,
			RangeTo:   5,
			Name:      "Group2",
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
		})
	engine := shorm.NewEngine("mssql", &cluster)
```
- if the sharding value lines in[0,3), the sql query will be exeucted against Group1
- if the sharding value lines in[3,5), the sql query will be exeucted against Group2

- Create engine from config file
```Go
	engine, err := shorm.NewEngineFromConfig("cluster_config.json")
```
- Define go struct to mapping data table
```Sql
	Create table T_User(
		UserId bigint not null,
		UserName varchar(50) not null default '',
		Age int not null default 0,
		CreatedTime datetime not null default getdate(),
	)
	Alter table T_User create cluster index PK_User(UserId asc)
```
```Go
	type User struct{
		TabName shorm.TableName `shorm:"T_User"`
		Id int64 `shorm:"UserId,pk,shard"`
		UserName string
		Age int32
		CreatedTime time.Time
	}

	func (u User) GetShardValue() int64{
		return u.Id
	}
```

- Get value via specify sharding value
```Go
	s := engine.StartSession()
	defer engine.EndSession(s)
	var u User
	s.ShardValue(1).Id(1).Get(&u)
```
it will generate sql as below:
```Sql
	select top 1 * from T_User where UserId=1
``` 
and will locate the database "G1_node1"

shorm provides three ways to locate the database
* *Specify sharding value by call session.ShardValue(value int64), with highest priority*
* *Go struct implements Shardinger interface, with higher priority*
* *Mark field with "shard" in shorm struct tag, with lowest priority*
