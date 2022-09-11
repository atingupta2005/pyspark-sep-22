# Spark and Hive Integration
## Pre-requisites:
- Docker
- 8 GB RAM

## Create & Start all services
```
docker-compose up
```

## Verify container status
- Allow Docker a few minutes to spin up all the containers
- Look for the messages in the logs once you run docker-compose up

## Connect to Hive and create tables
```
docker exec -it hive-server /bin/bash
```

```
cd ..;cd employee/
```

```
hive -f employee_table.hql
```

```
hadoop fs -put employee.csv /user/hive/warehouse/testdb.db/employee
```

## Validate
- On the hive-server, launch hive to verify the contents of the employee table.
```
hive
```

```
hive> show databases;
```

```
hive> use testdb;
```

```
hive> select * from employee;
```

### Exit from Hive
```
exit
```

### Exit from Docker container
```
exit
```

## Check if thrift server running (It should show connected)
- Press CTRL+C to come out of it
```
telnet localhost 9083
```

## Connect to Hive using Spark
- Refer the notebook - spark-hive-integration.ipynb
