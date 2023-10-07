version: 2

sources: 
  - name: op_optimism
    freshness:
          warn_after: { count: 12, period: hour }
    description: >
      Decoded event tables for Optimism token delegate transactions.
    tables:
      - name: GovernanceToken_evt_DelegateVotesChanged
        loaded_at_field: evt_block_time
      - name: GovernanceToken_evt_DelegateChanged
        loaded_at_field: evt_block_time