

{{ config(
        tags=['dunesql']
        , schema = 'op_token_optimism'
        , alias = alias('initial_allocations')
        , unique_key = ['allocation_category','allocation_subcategory']
        , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "op_token",
                                  \'["msilb7"]\') }}'
  )
}}

WITH initial_allocation_percentages AS (
  SELECT pct_supply_allocation, allocation_category, allocation_subcategory
  FROM (values
    --25% Ecosystem Fund
       (0.054,'Ecosystem Fund','Governance Fund')
      ,(0.054,'Ecosystem Fund','Partner Fund')
      ,(0.054,'Ecosystem Fund','Seed Fund')
      ,(0.088,'Ecosystem Fund','Unallocated')
    --20% RetroPGF
      ,(0.20, 'Retroactive Public Goods Funding (RetroPGF)','Retroactive Public Goods Funding (RetroPGF)')
    --19% User Airdrops
      ,(0.19, 'User Airdrops','User Airdrops')
    --19% Core Contributors
      ,(0.19, 'Core Contributors', 'Core Contributors')
    --17% Core Contributors
      ,(0.17, 'Investors', 'Investors')
  ) sup (pct_supply_allocation, allocation_category, allocation_subcategory)
)

SELECT

  pct_supply_allocation
, allocation_category
, allocation_subcategory
, pct_supply_allocation*total_initial_supply AS initial_allocated_supply

FROM {{ ref('op_token_optimism_metadata')}} md , initial_allocation_percentages allo