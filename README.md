'2024-09-09T18:48:04Z' '2024-09-09T23:59:59Z'
Error: 400, {"error":{"code":"BadRequest","message":"Bad Request","details":[{"message":"Expected literal type token but found token 'eyJTdGFydERhdGVUaW1lIjoiMjAyNC0wOS0wOVQxODo0ODowNC4wMDAwMDAwXHUwMDJCMDA6MDAiLCJFbmREYXRlVGltZSI6IjIwMjQtMDktMDlUMjM6NTk6NTkuMDAwMDAwMFx1MDAyQjAwOjAwIiwiRmlsZU5hbWUiOiIyMDI0LTA5LTA5VDE5X3YxXzAwMS5jc3YiLCJGaWxlT2Zmc2V0IjowLCJBY3Rpdml0eSI6bnVsbCwiVXNlcklkIjpudWxsfSwyMDI0LTA5LTA5VDE4OjQ4OjA0LjAwMDAwMDArMDA6MDAsMjAyNC0wOS0wOVQyMzo1OTo1OS4wMDAwMDAwKzAwOjAwLDAsLA'.","target":"continuationToken"}]}}
Wrote 659867 bytes.
Error: 400, {"error":{"code":"BadRequest","message":"Bad Request","details":[{"message":"Expected literal type token but found token 'eyJTdGFydERhdGVUaW1lIjoiMjAyNC0wOS0wOVQxODo0ODowNC4wMDAwMDAwXHUwMDJCMDA6MDAiLCJFbmREYXRlVGltZSI6IjIwMjQtMDktMDlUMjM6NTk6NTkuMDAwMDAwMFx1MDAyQjAwOjAwIiwiRmlsZU5hbWUiOiIyMDI0LTA5LTA5VDE5X3YxXzAwMS5jc3YiLCJGaWxlT2Zmc2V0IjowLCJBY3Rpdml0eSI6bnVsbCwiVXNlcklkIjpudWxsfSwyMDI0LTA5LTA5VDE4OjQ4OjA0LjAwMDAwMDArMDA6MDAsMjAyNC0wOS0wOVQyMzo1OTo1OS4wMDAwMDAwKzAwOjAwLDAsLA'.","target":"continuationToken"}]}}
Wrote 2 bytes.


dateLoop = getDateLoop(startDateTime, endDateTime)

for (start, end) in dateLoop:
    print(start, end)
    lastResultSet, continuationToken = getResultForQuery(access_token, start=start, end=end)
    loopNumber = 1
    while lastResultSet == False:
        lastResultSet, continuationToken = getResultForQuery(access_token, start=start, ct=continuationToken)
        loopNumber += 1
