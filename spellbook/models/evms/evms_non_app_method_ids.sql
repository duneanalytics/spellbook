
{{ config(
        tags = [ 'static'],
        schema = 'non_app_method_ids',
        alias = 'non_app_method_ids',
        post_hook='{{ expose_spells(\'["ethereum","optimism","arbitrum","polygon","gnosis","avalanche_c","fantom","goerli","bnb","base","celo", "zora"]\',
                                "sector",
                                "method_ids",
                                \'["msilb7"]\') }}'
        )
}}


{% set all_chains_array = [
    'ethereum'
    ,'optimism'
    ,'arbitrum'
    ,'polygon'
    ,'gnosis'
    ,'avalanche_c'
    ,'fantom'
    ,'goerli'
    ,'bnb'
    ,'base'
    ,'celo'
    ] %}

WITH aggrregate_methods AS (
-- Generic EVM methods, we read null array as "all"
SELECT NULL as blockchains, method_id, method_descriptor
    FROM (values
         (0x095ea7b3,'ERC20 Approval') --'ERC20 Approval'
        ,(0xa9059cbb,'ERC20 Transfer') --'ERC20 Transfer'
        ,(0xd0e30db0,'WETH Wrap') --'WETH Wrap'
        ,(0x2e1a7d4d,'WETH Unwrap') --'WETH Unwrap'
        ,(0x42842e0e,'ERC721 Transfer'), (0x23b872dd,'ERC721 Transfer') --'ERC721 Transfer' --safe transfer and transfer
        ,(0xb88d4fde,'ERC721 Transfer'), (0xf3993d11,'ERC721 Transfer')
        ,(0xf242432a,'ERC1155 Transfer'), (0x2eb2c2d6,'ERC1155 Transfer')
        ,(0xa22cb465,'ERC721/ERC1155 Approval') --'ERC721 Approval'
        ,(0x60806040,'Contract Creation'), (0x60c06040,'Contract Creation') --'Contract Creation'
        ) a (method_id, method_descriptor)

UNION ALL --Optimism-Specific Methods

SELECT 'optimism' AS blockchains, method_id, method_descriptor
    FROM (values
         (0xcbd4ece9,'Bridge In (L1 to L2)') --'Bridge In (L1 to L2)'
        ,(0x32b7006d,'Bridge Out (L2 to L1)') --'Bridge Out (L2 to L1)'
        ,(0xbede39b5,'OVM Gas Price Oracle'), (0xbf1fe420,'OVM Gas Price Oracle') --'OVM Gas Price Oracle'
        ,(0x015d8eb9,'Set L1 Block Values') -- Set L1 Block Values System Transaction (Bedrock and later)
        ) a (method_id, method_descriptor)

UNION ALL --Arbitrum-Specific Methods

SELECT 'arbitrum' AS blockchains, method_id, method_descriptor
    FROM (values
         (0x6bf6a42d,'ARBOS System Transaction') --ARBOS System Transaction
        ,(0xc9f95d32, 'Submit Retryable Tx') --Arb RetryableTx
        ,(0x25e16063,'Bridge Out (L2 to L1)') --WithdrawETH
        ,(0x7b3a3c8b,'Bridge Out (L2 to L1)') --OutboundTransfer (ERC20)
        ,(0x2e567b36,'Bridge In (L1 to L2)') --finalizeInboundTransfer (ERC20)
        ) a (method_id, method_descriptor)

)


SELECT *
FROM (
    {% for chain in all_chains_array %}
    SELECT '{{chain}}' AS blockchain, method_id, method_descriptor
    FROM aggrregate_methods
    WHERE
        blockchains IS NULL --If Null, make an entry for all chains
        OR blockchains = '{{chain}}'
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)