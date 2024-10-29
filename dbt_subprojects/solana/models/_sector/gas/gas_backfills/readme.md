# Solana Gas Backfills

This directory contains backfill models for Solana gas/fee data, split into two main categories:

## Transaction Fees (`tx_fees/`)
- Regular transaction fees paid by users
- Split into quarterly models (e.g. `gas_solana_tx_fees_2021_q1`)
- Each quarterly model uses the same macro to generate consistent data structure
- Models are materialized as tables using delta format
- Combined into a single view `gas_solana_tx_fees` using UNION ALL

## Vote Transaction Fees (`vote_fees/`) 
- Fees specifically from vote transactions
- Also split into quarterly models (e.g. `gas_solana_vote_fees_2021_q1`)
- Uses dedicated vote fees macro for data generation
- Models are materialized as views to contain logic, but not rewrite data (massive data volume for minimal transformation logic benefit, i.e. static fees)
- Combined into a single view `gas_solana_vote_fees` using UNION ALL

## Usage
The `gas_solana_tx_fees` view will feed into `gas_solana_fees`, which will ultimately feed into `gas.fees` cross-chain view.
The `gas_solana_vote_fees` will remain stand alone, but can be queried independently if needed. Performance may be questionable due to view materialization on massive data volume.

## Time Coverage
- Starts from Q4 2020
- Split into quarterly increments
- Models are pre-generated up to Q3 2024 for future data
- Current model will be updated with `current_date` as end_date
- As new quarters arrive, update the `current` model to point to the new quarter and create a prior quarter model for historical purposes (table, static)

## Model Configuration
- All quarterly models use `static` tags and therefore are not refreshed
- Materialized as tables with delta file format
- Final backfill views are materialized as views for efficient querying

## Why?
- Computing Solana history is computationally expensive
- Pre-generating backfill data allows for better execution of the historical data without having to scale up clusters to max sizes