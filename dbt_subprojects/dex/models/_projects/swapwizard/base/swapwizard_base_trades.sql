{{ config(
    schema = 'swapwizard_base',
    alias = 'trades',
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook='{{ expose_spells(
        blockchains = \'["base"]\',
        spell_type = "project",
        spell_name = "swapwizard",
        contributors = \'["cmayorga"]\'
    ) }}'
) }}

{{ swapwizard_trades('base', '0xA2ae391D0740Cb8C8aA40dB7c076591A8b5A2A4d') }}
