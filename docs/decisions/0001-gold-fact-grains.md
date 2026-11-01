# 0001 Gold Fact Grains

Date: 2026-11-01

## Status

Accepted

## Context

The 12 Silver tables have different keys and one-to-many relationships. A single wide orders table would multiply item, payment, and review measures. The first Olist snapshot predates the first customer/product SCD2 observation.

## Decision

- Keep separate commerce facts at the order, order-item, payment, and review grains; behavior facts at event and session grains; and a daily currency-pair rate fact.
- Share date, customer, product, seller, category, geography, and channel dimensions. Retain every observed customer/product snapshot version; geography and category include values from all versions.
- Use `order_items.price` for historical sales. Payment value and freight are separate measures. Group returning shoppers by `customer_unique_id`.
- Assign an event to the observed SCD2 version valid at its timestamp. For an Olist event older than the first observation, use the first observed version and label its attributes as an approximation, not reconstructed history.
- Use the `America/Sao_Paulo` calendar for business date keys. Select incremental source changes by ingestion timestamp so old orders updated today remain eligible.
- Keep campaign data synthetic and unattached to order revenue until a real campaign identifier and attribution rule exist.

## Alternatives Considered

1. One wide order fact: rejected because source child tables are one-to-many and would duplicate measures.
2. Recompute old order price from current product catalog price: rejected because the line-item sale price is already recorded.

## Consequences

Positive:

- Counts and revenue can be reconciled at each source grain.
- Stage 5 marts can use the fact matching their question without accidental fanout.

Tradeoffs:

- Some reports need a documented bridge or allocation rule before mixing item category with payment method or review score.
- First-observed product/customer attributes cannot prove their historical state before snapshots began.

## Follow-Up

- Run dbt snapshots, generic tests, and incremental MERGE on Databricks after Silver is available there.
- Use an isolated Stage 1C source to prove price/address version changes without mutating the frozen Olist baseline.
