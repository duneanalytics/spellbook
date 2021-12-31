CREATE OR REPLACE VIEW nft.view_cryptopunk_owners AS

WITH
    initial_owners AS (
        SELECT DISTINCT UNNEST(indices) as token_id, UNNEST(addresses) as owner
        FROM cryptopunks."CryptoPunksMarket_call_setInitialOwners"
    ),
    current_owners AS (
        SELECT token_id, owner FROM (
            SELECT
                "punkIndex" AS token_id,
                "to" AS owner,
                evt_block_number AS block_number,
                ROW_NUMBER() OVER (PARTITION BY "punkIndex" ORDER BY evt_block_number DESC) AS idx
            FROM cryptopunks."CryptoPunksMarket_evt_PunkTransfer"
        ) owner_history WHERE idx = 1
    ),
    wrapped_owners AS (
        SELECT token_id, owner FROM view_erc721_owners
        WHERE contract_address = '\xb7f7f6c52f2e2fdb1963eab30438024864c313f6'
        AND owner != '\x0000000000000000000000000000000000000000'
    )

SELECT
    token_id.token_id AS token_id,
    COALESCE(wrapped.owner, current.owner, initial.owner) AS owner
FROM generate_series(0::numeric, 9999::numeric) token_id
LEFT JOIN initial_owners initial ON initial.token_id = token_id.token_id
LEFT JOIN current_owners current ON current.token_id = token_id.token_id
LEFT JOIN wrapped_owners wrapped ON wrapped.token_id = token_id.token_id
