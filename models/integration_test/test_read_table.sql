{{ config(
  
  materialized='view',
  schema='integration_test', 
  alias = 'test_view') }}


select * from {{ ref('test_incremental_table') }} limit 10
