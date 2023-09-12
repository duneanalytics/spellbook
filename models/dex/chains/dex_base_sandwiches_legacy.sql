{{ config(
	tags=['legacy'],

        schema = 'dex_base',
        alias = alias('sandwiches', legacy_model=True),
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['sandwiched_pool', 'frontrun_tx_hash', 'frontrun_taker', 'frontrun_index', 'currency_address'],
        post_hook='{{ expose_spells(\'["base"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}
SELECT 1