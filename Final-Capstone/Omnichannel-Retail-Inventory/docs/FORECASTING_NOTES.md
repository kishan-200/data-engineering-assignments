# Forecasting Notes

## Formal ML Model

The formal machine-learning model used in the project is Spark ML RandomForestRegressor.

## Why Random Forest

Random Forest was selected because:

- retail demand can have nonlinear patterns
- lag features and rolling features can interact in complex ways
- the model is more robust than a single decision tree
- feature scaling is generally not required
- it integrates directly with Spark ML

## Validation Result

- Random Forest MAPE: 16.71%
- Target: less than 25%
- Result: PASS

## Seasonal Baseline

A same-month-last-year seasonal baseline achieved 12.94% MAPE.

The seasonal baseline is not an ML model. Random Forest remains the formal machine-learning model used for project evaluation.
