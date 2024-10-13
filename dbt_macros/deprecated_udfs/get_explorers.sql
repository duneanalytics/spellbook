{% macro get_explorers() %}
      create or replace function get_explorers(hash_ STRING, explorers_ STRING, hash_type_ STRING, blockchain_ STRING, concise_ BOOLEAN)
      returns STRING
      return
/* 
 get all the chain explorer links with some params.
• hash: address or transaction hash
• explorers: List all explorers you need as a single comma-separated string. Output order will match input ordering. Full list: Etherscan,GitHub,Arkham,Zerion,Parsec,Nansen,Dora,Zapper,OKLink,Blockscout,DeBank,Tokenview,Ethplorer,Bitquery,Blockchair,Ankr,Unmarshal,Beaconchain,Rated
• hash_type: 'tx' or 'address'
• blockchain: chain to explore
• concise: true/false

example: get_explorers(
      0xd8da6bf26964af9d7eed9e03e53415d37aa96045
      , 'Etherscan,GitHub,Arkham,Zerion,Parsec,Nansen,Dora,Zapper,OKLink,Blockscout,DeBank,Tokenview,Ethplorer,Bitquery,Blockchair,Ankr,Unmarshal,Beaconchain,Rated'
      , 'address'
      , true
      )
*/
WITH explorers AS (
    SELECT explorer, emoji, address, transaction
    FROM (VALUES
    ('Etherscan', '🔎', 'https://etherscan.io/address/hash_', 'https://etherscan.io/tx/hash_')
    , ('GitHub', '💻', 'https://github.com/search?q=hash_&type=code', 'https://github.com/search?q=hash_&type=code')
    , ('Arkham','⬛️', 'https://platform.arkhamintelligence.com/explorer/address/hash_', 'https://platform.arkhamintelligence.com/explorer/tx/hash_')
    , ('Zerion', '🟦', 'https://app.zerion.io/hash_/overview', NULL)
    , ('Nansen', '❇️', 'https://app.nansen.ai/profiler?address=hash_', 'https://app.nansen.ai/tx/hash_')
    , ('Parsec', '⪇', 'https://parsec.fi/address/hash_', 'https://parsec.fi/blockchain_/tx/hash_')
    , ('Zapper', '⚡️', 'https://zapper.xyz/account/hash_', 'https://zapper.xyz/event/blockchain_/hash_')
    , ('Blockscout', '🔭', 'https://eth.blockscout.com/address/hash_', 'https://eth.blockscout.com/tx/hash_')
    , ('Unmarshal', '⛨', 'https://xscan.io/address/hash_/assets?chain=blockchain_', NULL)
    , ('Dora', '🌼', 'https://www.ondora.xyz/accounts/hash_/all', 'https://www.ondora.xyz/network/blockchain_/interactions/hash_')
    , ('Ethplorer', '🔷', 'https://ethplorer.io/address/hash_#', 'https://ethplorer.io/tx/hash_#pageTab=transfers')
    , ('Socketscan', '🔌', NULL, 'https://www.socketscan.io/tx/0xb2c17ecd521f73d8d91aaf574ad43ff1bda3148996079fb2796aabd30dc6ddac')
    , ('Beaconchain', '📡', 'https://beaconcha.in/address/hash_', 'https://beaconcha.in/tx/hash_')
    , ('Rated', '🍬', 'https://explorer.rated.network/o/hash_', NULL)
    , ('Tokenview', '🎟️', 'https://eth.tokenview.io/en/address/hash_', 'https://eth.tokenview.io/en/tx/hash_')
    , ('Bitquery', '🅱️', 'https://explorer.bitquery.io/ethereum/address/hash_', 'https://explorer.bitquery.io/blockchain_/tx/hash_')
    , ('OKLink', '🆗', 'https://www.oklink.com/multi-search#key=hash_', 'https://www.oklink.com/eth/tx/hash_')
    , ('Blockchair', '🧊', 'https://blockchair.com/blockchain_/address/hash_', 'https://blockchair.com/blockchain_/transaction/hash_')
    , ('Debank', '🔓', 'https://debank.com/profile/hash_', NULL)
    , ('Ankr', '⚓️', 'https://ankrscan.io/address/hash_', 'https://ankrscan.io/transactions/chain/blockchain_/hash_')
    , ('ethVM', '🖥️', 'https://www.ethvm.com/address/hash_', 'https://www.ethvm.com/tx/hash_?t=actions')
    ) AS x (explorer, emoji, address, transaction)
    )

, results AS (
    SELECT CASE WHEN concise_ AND hash_type_ = 'tx' THEN get_href(transaction, emoji)
      WHEN NOT concise_ AND hash_type_ = 'tx' get_href(transaction, explorer)
      WHEN concise_ AND hash_type_ = 'address' get_href(address, emoji)
      WHEN NOT concise_ AND hash_type_ = 'address' get_href(address, explorer)
      WHEN hash_type_ = 'tx' get_href(transaction, explorer)
      ELSE get_href(address, explorer)
      END AS link
    ,regexp_count(substring('{{explorers}}' FROM 1 FOR position(explorer IN '{{explorers}}') - 1), ',') AS ordering
    FROM explorers
    WHERE '{{explorers}}' LIKE '%' || explorer || '%'
    )

SELECT array_join(array_agg(link ORDER BY ordering), ' ') AS links
FROM results
{% endmacro %}