{{
    config(
        tags = ['dunesql'],
        alias = alias('mints'),
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "mint_fun",
                                \'["hildobby"]\') }}'
    )
}}

{% set mint_fun_hash = '0021fb3f' %}