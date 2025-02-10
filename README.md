# Dev Notes

## Launch a local TimeScale DB

```
docker rm timescaledb
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb-ha:pg17
```

## Trying out MongoDB
A document type database may be more suited for the type of data I am collecting. (The data has lots of one-to-many relationships).

```
docker pull mongodb/mongodb-community-server:latest

```