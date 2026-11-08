# 0002 Power BI Dashboard

Date: 2026-11-02

## Status

Power BI and Import remain accepted. The Databricks source choice is superseded
by [0004](0004-aws-glue-pipeline.md).

## Context

Stage 7 needs five report pages over the six existing Gold business marts and `ops_pipeline_runs`. The dashboard tool was an open choice in `PLAN.md`.

## Decision

Use Power BI for the Stage 7 dashboard. First prove that Power BI Desktop can read one Gold mart and `ops_pipeline_runs` from the project's Databricks SQL warehouse. Choose Import or DirectQuery, refresh behavior, and demo sharing only after that connection test.

## Consequences

- Build the report from the published marts at their documented grains; use `ops_pipeline_runs` for the Ops page.
- Keep credentials out of the repository and the report artifact.
- Do not claim automated refresh or externally accessible sharing until each is tested.

## Follow-Up

- Execute `docs/plans/active/stage-7-dashboard.md`.
