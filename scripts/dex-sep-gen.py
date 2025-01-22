# known blockchains
blockchains = ['ethereum', 'bnb', 'optimism', 'fantom', 'avalanche_c', 'polygon', 'celo', 'base']

model_names = [
    'uniswap_trades', 'sushiswap_trades', 'fraxswap_trades',
    'airswap_ethereum_trades', 'clipper_trades', 'shibaswap_ethereum_trades',
    'swapr_ethereum_trades', 'defiswap_ethereum_trades', 'dfx_ethereum_trades',
    'pancakeswap_trades', 'velodrome_optimism_trades', 'woofi_trades',
    'bancor_ethereum_trades', 'platypus_finance_avalanche_c_trades', 'trader_joe_trades',
    'mstable_ethereum_trades', 'zigzag_trades', 'gmx_trades',
    'biswap_bnb_trades', 'wombat_bnb_trades', 'iziswap_bnb_trades', 'babyswap_bnb_trades',
    'apeswap_trades', 'spartacus_exchange_fantom_trades', 'spookyswap_fantom_trades',
    'beethoven_x_trades', 'rubicon_trades', 'equalizer_fantom_trades',
    'wigoswap_fantom_trades', 'arbswap_trades', 'spiritswap_fantom_trades',
    'quickswap_trades', 'integral_trades', 'maverick_trades',
    'verse_dex_ethereum_trades', 'onepunchswap_bnb_trades', 'glacier_avalanche_c_trades',
    'thena_trades', 'camelot_trades', 'xchange_trades', 'mdex_bnb_trades',
    'nomiswap_bnb_trades', 'kyberswap_trades', 'zeroex_native_trades',
    'zipswap_trades', 'balancer_trades', 'hashflow_trades',
    'honeyswap_trades', 'synthetix_spot_trades', 'dodo_trades',
    'curvefi_trades', 'ellipsis_finance_trades', 'aerodrome_base_trades',
    'carbon_defi_ethereum_trades', 'ubeswap_celo_trades', 'opx_finance_optimism_trades',
    'mauve_trades', 'openxswap_optimism_trades', 'wardenswap_optimism_trades',
    'openocean_optimism_trades', 'woofi_optimism_trades', 'oneinch_lop_own_trades',
    'mummy_finance_optimism_trades', 'hashflow_optimism_trades'
]

# Initialize lists to store model names
with_blockchain = []
without_blockchain = []

# Iterate model_names and classify
for model_name in model_names:
    if any(blockchain in model_name for blockchain in blockchains):
        with_blockchain.append(model_name)
    else:
        without_blockchain.append(model_name)

print("With blockchain:", with_blockchain)
print("len(with_blockchain):", len(with_blockchain))
print("Without blockchain:", without_blockchain)
print("len(without_blockchain):", len(without_blockchain))

import os

# Path prefix for models
path_prefix = '../models/_project/'

# Iterate over models with blockchain information
for model_name in with_blockchain:
    parts = model_name.split('_')
    project_name = '_'.join(parts[:-2])
    blockchain = parts[-2]

    directory = f"{path_prefix}{project_name}/"
    file_name = f"{project_name}_trades.sql"
    file_path = os.path.join(directory, file_name)

    os.makedirs(directory, exist_ok=True)

    content = f"""{{{{ config(
    schema = '{project_name}',
    alias = 'trades',
    materialized = 'view',
    post_hook='{{{{ expose_spells(blockchains = \'["{blockchain}"]\', 
                                  spell_type = "project", 
                                  spell_name = "{project_name}", 
                                  contributors = \'["jeff-dude", "hosuke", "soispoke"]\') }}}}'
    )
}}}}


SELECT  blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , token_bought_symbol
        , token_sold_symbol
        , token_pair
        , token_bought_amount
        , token_sold_amount
        , token_bought_amount_raw
        , token_sold_amount_raw
        , amount_usd
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , tx_hash
        , tx_from
        , tx_to
        , evt_index
FROM ref('dex_trades')
WHERE project = '{project_name}'"""

    with open(file_path, 'w') as f:
        f.write(content)
    print(f"Created: {file_path}")

for model_name in without_blockchain:
    parts = model_name.split('_')
    project_name = '_'.join(parts[:-1])
    directory = f"{path_prefix}{project_name}/"
    file_name = f"{project_name}_trades.sql"
    file_path = os.path.join(directory, file_name)

    os.makedirs(directory, exist_ok=True)

    content = f"""{{{{ config(
        schema = '{project_name}',
        alias = 'trades',
        materialized = 'view',
        post_hook='{{{{ expose_spells(blockchains = \'[]\', 
                                      spell_type = "project", 
                                      spell_name = "{project_name}", 
                                      contributors = \'["jeff-dude", "hosuke", "soispoke"]\') }}}}'
        )
}}}}


SELECT  blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , token_bought_symbol
        , token_sold_symbol
        , token_pair
        , token_bought_amount
        , token_sold_amount
        , token_bought_amount_raw
        , token_sold_amount_raw
        , amount_usd
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , tx_hash
        , tx_from
        , tx_to
        , evt_index
FROM ref('dex_trades')
WHERE project = '{project_name}'"""

    with open(file_path, 'w') as f:
        f.write(content)
    print(f"Created: {file_path}")