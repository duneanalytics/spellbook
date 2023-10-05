{{ config(
	tags=['legacy'],
	
    alias = alias('foundation_wallet_approvals', legacy_model=True),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'evt_block_time', 'evt_block_number', 'evt_tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "op_token_distributions",
                                \'["msilb7"]\') }}'
    )
}}

--Assuming: Governance Fund Wallet -> Project Wallet = Not Distributed OP
--          Project Wallet -> ? = Distributed OP
-- Starting Jul 13, 2022 - The Foundation wallet approved tokens to project wallets for grants rather than making the transfer directly.

{% set op_token_address = '0x4200000000000000000000000000000000000042' %}
{% set approvals_start_date = '2022-07-13'  %}
{% set foundation_label = 'OP Foundation'  %}
{% set grants_descriptor = 'OP Foundation Grants'  %}


WITH project_labels AS (
    SELECT * FROM {{ ref('op_token_distributions_optimism_project_wallets_legacy') }}
    WHERE label IS NOT NULL
)


SELECT
DATE_TRUNC('day',evt_block_time) AS block_date,
a.evt_block_time, a.evt_block_number, a.evt_tx_hash, a.evt_index,
a.spender AS project_address, al.project_name,

t.`from` AS tx_from_address, t.to AS tx_to_address, 

cast(a.value as double)/cast(1e18 as double) AS op_approved_to_project

FROM {{ source('erc20_optimism', 'evt_Approval') }} a
    INNER JOIN {{ source('optimism', 'transactions') }} t
        ON t.hash = a.evt_tx_hash
        AND t.block_number = a.evt_block_number
        {% if is_incremental() %} 
        AND t.block_time >= date_trunc('day', now() - interval '1 week')
        {% else %}
        AND t.block_time >= cast( '{{approvals_start_date}}' as date )
        {% endif %}
        AND t.to = a.owner
        AND t.to in (SELECT address FROM project_labels WHERE label = '{{foundation_label}}' AND address_descriptor = '{{grants_descriptor}}')
    LEFT JOIN project_labels al
        ON a.spender = al.address
WHERE
    a.contract_address = '{{op_token_address}}' --OP Token
    AND owner in (SELECT address FROM project_labels WHERE label = '{{foundation_label}}' AND address_descriptor = '{{grants_descriptor}}')
    {% if is_incremental() %} 
    AND a.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% else %}
    AND a.evt_block_time >= cast( '{{approvals_start_date}}' as date )
    {% endif %}