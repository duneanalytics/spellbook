# OVM 2.0 Abstractions (WIP)

**l1_gas_price_oracle_updates**: Reads in the L1 Gas Price from the OVM Gas Price Oracle (Gwei)
- L1 Gas Price is used for Transaction Fee Calculations.

### To Be Added:
- **get_l1_gas_used** funtion: Returns the L1 Gas Used for each transaction. Calculates calldata and approximates noncalldata (temporary band-aid until the transaction receipt fields are added by Dune).
- **get_fee_scalar** function: Returns the fee scalar given a block number (currently all 1.5x).
