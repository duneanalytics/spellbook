CREATE OR REPLACE VIEW nft.view_erc721_owners AS

SELECT contract_address, token_id, owner FROM (
    SELECT
        contract_address,
        "tokenId" AS token_id,
        "to" AS owner,
        ROW_NUMBER() OVER (PARTITION BY contract_address, "tokenId" ORDER BY evt_block_time DESC) AS idx
    FROM erc721."ERC721_evt_Transfer"
) owner_history WHERE idx = 1
