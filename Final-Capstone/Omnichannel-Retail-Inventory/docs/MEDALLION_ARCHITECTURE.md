# Medallion Architecture

The project uses Bronze, Silver and Gold data layers.

## Bronze

Bronze stores raw ingested data with minimal transformation.

Examples:

- POS sales events
- supplier files
- imported relational data

## Silver

Silver stores cleaned and standardized datasets.

Typical processing includes:

- type conversion
- invalid-record handling
- deduplication
- normalization
- schema cleanup

## Gold

Gold contains business-ready datasets used for analytics, forecasting and replenishment.

Examples:

- store-SKU performance
- replenishment recommendations
- forecast outputs

This separation improves traceability, data quality and maintainability.
