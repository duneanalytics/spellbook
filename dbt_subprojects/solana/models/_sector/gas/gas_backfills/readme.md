# Solana Gas Backfills

This directory contains backfill models for Solana gas/fee data, split into two main categories:

## Transaction Fees (`tx_fees/`)
- Regular transaction fees paid by users
- Split into quarterly models (e.g. `gas_solana_tx_fees_2021_q1`)
- Each quarterly model uses the same macro to generate consistent data structure
- Models are materialized as tables using delta format
- Combined into a single view `tx_fees_backfill` using UNION ALL

## Vote Transaction Fees (`vote_fees/`) 
- Fees specifically from vote transactions
- Also split into quarterly models (e.g. `gas_solana_vote_fees_2021_q1`)
- Uses dedicated vote fees macro for data generation
- Models are materialized as tables using delta format
- Combined into a single view `vote_fees_backfill` using UNION ALL

## Usage
Both backfill views (`tx_fees_backfill` and `vote_fees_backfill`) are referenced by the main `gas_solana_fees` model, which combines them into a comprehensive view of all Solana transaction fees with appropriate type labeling.

## Time Coverage
- Starts from Q4 2020
- Split into quarterly increments
- Models are pre-generated up to Q3 2024 for future data

## Model Configuration
- All quarterly models use `static` tags and therefore are not refreshed
- Materialized as tables with delta file format
- Final backfill views are materialized as views for efficient querying

## Why?
- Computing Solana history is computationally expensive
- Pre-generating backfill data allows for better execution of the historical data without having to scale up clusters to ridicoulous sizes
