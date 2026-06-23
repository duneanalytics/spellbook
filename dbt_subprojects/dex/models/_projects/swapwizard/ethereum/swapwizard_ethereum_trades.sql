{{ config(
    schema = 'swapwizard_ethereum',
    alias = 'trades',
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook='{{ expose_spells(
        blockchains = \'["ethereum"]\',
        spell_type = "project",
        spell_name = "swapwizard",
        contributors = \'["cmayorga"]\'
    ) }}'
) }}

{{ swapwizard_trades('ethereum', '0x649A382ab2aecEc5c1B9ac956c4CdaCEDaC96c80') }}
