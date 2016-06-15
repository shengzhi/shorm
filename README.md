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

# Drivers support
---
- MsSql: github.com/denisenkom/go-mssqldb

# Change log
---


# Quick start
---
- Create engine from code

```Go
    cluster := &shorm.Cluster{}  
	engine := shorm.NewEngine("mssql", &cluster)
```

- Create engine from config file
```Go
	engine, err := shorm.NewEngineFromConfig("cluster_config.json")
```
