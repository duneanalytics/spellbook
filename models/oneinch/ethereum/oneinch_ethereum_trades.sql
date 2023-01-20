{{  config(
        alias='trades',
        partition_by = ['block_date'],
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        materialized='incremental',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "oneinch",
                                    \'["jeff-dude", "dsalv", "k06a"]\') }}'
    )
}}

-- {% set project_start_date = '2019-06-03' %}
{% set project_start_date = '2023-01-01' %} --for dev, keep data to recent timeframe

