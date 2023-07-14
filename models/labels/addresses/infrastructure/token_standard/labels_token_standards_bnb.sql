{{config(alias='token_standards_bnb',
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby"]\') }}')}}

SELECT distinct 'bnb' AS blockchain
, erc20.contract_address AS address
, 'bep20' AS name
, 'infrastructure' AS category
, 'hildobby' AS contributor
, 'query' AS source
, date('2023-03-02') AS created_at
, NOW() AS modified_at
, 'token_standard' AS model_name
, 'persona' as label_type
FROM {{ source('erc20_bnb', 'evt_transfer') }} erc20
{% if is_incremental() %}
LEFT ANTI JOIN this t ON t.address = erc20.contract_address
WHERE erc20.evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}

UNION ALL

SELECT distinct 'bnb' AS blockchain
, nft.contract_address AS address
, token_standard AS name
, 'infrastructure' AS category
, 'hildobby' AS contributor
, 'query' AS source
, date('2023-03-02') AS created_at
, NOW() AS modified_at
, 'token_standard' AS model_name
, 'persona' as label_type
FROM {{ ref('nft_bnb_transfers_legacy') }} nft
{% if is_incremental() %}
LEFT ANTI JOIN this t ON t.address = nft.contract_address
WHERE nft.block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}