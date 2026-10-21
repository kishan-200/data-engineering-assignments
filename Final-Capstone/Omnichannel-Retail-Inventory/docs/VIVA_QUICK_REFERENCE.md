# Viva Quick Reference

## Kafka vs Spark

Kafka transports and stores event streams.

Spark processes those events and performs transformations, aggregation and stateful streaming logic.

## HDFS vs HBase

HDFS is distributed file storage.

HBase provides low-latency row-level access for current inventory state.

## Airflow vs Spark

Airflow orchestrates workflows.

Spark executes data-processing workloads.

## SKU

SKU means Stock Keeping Unit.

It uniquely identifies a product or product variant.

## MAPE

MAPE means Mean Absolute Percentage Error.

It measures forecasting error as a percentage.

Lower values indicate better forecast accuracy.

## Reorder Point

The reorder point is the inventory threshold at which replenishment should be considered.

## Idempotency

Idempotency means processing the same event more than once does not incorrectly apply the business effect more than once.

## Checkpointing

Spark checkpoints save streaming progress and state so processing can recover after restart.
