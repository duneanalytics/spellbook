import os

# make sure to set cwd to models/_sector/nft/trades/platform_views when running this
current_directory = os.getcwd()


platform_list = [
    ("aavegotchi", '["polygon"]'),
    ("archipelago", '["ethereum"]'),
    ("blur", '["ethereum"]'),
    ("collectionswap", '["ethereum"]'),
    ("cryptopunks", '["ethereum"]'),
    ("element", '["ethereum"]'),
    ("foundation", '["ethereum"]'),
    ("fractal", '["polygon"]'),
    ("looksrare", '["ethereum"]'),
    ("magiceden", '["solana", "polygon"]'),
    ("nftb", '["bnb"]'),
    ("nftearth", '["optimism"]'),
    ("nftrade", '["bnb"]'),
    ("oneplanet", '["polygon"]'),
    ("opensea", '["ethereum", "solana", "polygon"]'),
    ("pancakeswap", '["bnb"]'),
    ("quix", '["optimism"]'),
    ("rarible", '["polygon"]'),
    ("stealcam", '["arbitrum"]'),
    ("sudoswap", '["ethereum"]'),
    ("superrare", '["ethereum"]'),
    ("tofu", '["optimism", "arbitrum", "polygon", "bnb"]'),
    ("x2y2", '["ethereum"]'),
    ("zonic", '["optimism"]'),
    ("zora", '["ethereum"]')
]

with open(os.path.join(current_directory, '_schema.yml'), "w") as schema:
    schema.write(
        """
version: 2
models:
"""
    )

    for platform in platform_list:
        with open(os.path.join(current_directory, f"{platform[0]}_trades_view.sql"), "w") as p_file:
            schema.write(f"""
 - name: {platform[0]}_trades_view
   meta:
     blockchain: {platform[1]}
     sector: nft
     contributors: 0xRob
   config:
     tags: ['nft', 'trades', '{platform[0]}']
   description: "NFT trades view for {platform[0]}"

""")
            p_file.write(f"""
{{{{ config(
        schema = '{platform[0]}',
        alias ='trades',
        materialized = 'view',
        post_hook='{{{{ expose_spells(\\\'{platform[1]}\\\',
                                    "project",
                                    "{platform[0]}",
                                    \\\'["0xRob"]\\\') }}}}')
}}}}

SELECT *
FROM {{{{ ref('nft_trades') }}}}
WHERE project = "{platform[0]}"
""")
