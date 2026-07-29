
### Start DB
```bash
docker compose up app-mosaic
```

### Add items/collections to the db

```bash
uv pip install pypgstac==0.9.11 "psycopg[pool]"
uv run pypgstac load collections stac/collection.json --dsn postgresql://username:password@127.0.0.1:5439/postgis --method insert_ignore
uv run pypgstac load items stac/items.json --dsn postgresql://username:password@127.0.0.1:5439/postgis --method insert_ignore

# Create Collection Search in cache
curl http://127.0.0.1:8081/collections/world/info | jq
```

### Siege
```
# 50 concurrents / repeat 10 times (500 tiles)
$ siege --file urls.txt -b -c 50 -r 10

Transactions:                 500    hits
Availability:                 100.00 %
Elapsed time:                   7.83 secs
Data transferred:               5.84 MB
Response time:                764.02 ms
Transaction rate:              63.86 trans/sec
Throughput:                     0.75 MB/sec
Concurrency:                   48.79
Successful transactions:      500
Failed transactions:            0
Longest transaction:         1110.00 ms
Shortest transaction:         400.00 ms


# 10 concurrents / repeat 100 times (1000 tiles)
$ siege --file urls.txt -b -c 10 -r 100

Transactions:                1000    hits
Availability:                 100.00 %
Elapsed time:                  20.12 secs
Data transferred:              11.46 MB
Response time:                196.11 ms
Transaction rate:              49.70 trans/sec
Throughput:                     0.57 MB/sec
Concurrency:                    9.75
Successful transactions:     1000
Failed transactions:            0
Longest transaction:          420.00 ms
Shortest transaction:          80.00 ms


# 200 concurrents / repeat 1 time (200 tiles)
$ siege --file urls.txt -b -c 200 -r 1

Transactions:                 200    hits
Availability:                 100.00 %
Elapsed time:                   3.18 secs
Data transferred:               2.16 MB
Response time:               2285.35 ms
Transaction rate:              62.89 trans/sec
Throughput:                     0.68 MB/sec
Concurrency:                  143.73
Successful transactions:      200
Failed transactions:            0
Longest transaction:         3180.00 ms
Shortest transaction:         820.00 ms
```
