# Replenishment Guide

The replenishment process converts inventory and demand information into draft purchase-order recommendations.

## Inputs

The process uses:

- current inventory
- forecast demand
- reorder point
- maximum stock level
- supplier mapping
- supplier lead-time information

## Output

The project generated:

- 26,545 replenishment recommendations
- 14,398 draft purchase orders
- 3,032,647 recommended units

## Draft PO Safety

Purchase orders are generated with status DRAFT.

The system does not automatically place a real supplier order. Human approval is expected before execution.
