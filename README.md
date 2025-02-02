# Dev Notes

## Launch a local TimeScale DB

```
docker rm timescaledb
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb-ha:pg17
```