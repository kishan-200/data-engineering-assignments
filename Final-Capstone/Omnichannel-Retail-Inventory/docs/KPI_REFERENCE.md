# KPI Reference

## Streaming Latency

- Measured: 29.499 seconds
- Target: less than 30 seconds
- Status: PASS

## Forecast Accuracy

- Model: Spark ML RandomForestRegressor
- MAPE: 16.71%
- Target: less than 25%
- Status: PASS

## Stock-Out Rate

- Measured: 0.0004%
- Target: less than 2%
- Status: PASS

## POS Deduplication

- Retry events tested: 1,000
- Retry events removed: 1,000
- Status: PASS

## Kafka Reliability

- Messages produced: 10
- Messages retained after broker failure: 10
- Status: PASS

## Spark Recovery

- Duplicate records after checkpoint restart: 0
- Status: PASS
