{{ config(
	tags=['legacy'],
        alias = alias('transfers', legacy_model=True),
        partition_by=['block_date'],
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['unique_transfer_id']
)
}}

SELECT 1