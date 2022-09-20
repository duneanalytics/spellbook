{% macro alter_table_properties() %}
{%- if target.name == 'prod'-%}

{% set addresses_optimism_airdrop_1 %}
ALTER TABLE addresses_optimism.airdrop_1 SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["optimism"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='addresses',
                                                    'dune.data_explorer.contributors'='["Msilb7","soispoke"]');
{% endset %}

{% set addresses_ethereum_safe_airdrop %}
ALTER TABLE addresses_ethereum.safe_airdrop SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='addresses',
                                                    'dune.data_explorer.contributors'='["springzh"]');
{% endset %}

{% set addresses_ethereum_cex %}
ALTER VIEW addresses_ethereum.cex SET TBLPROPERTIES ('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='cex',
                                                    'dune.data_explorer.contributors'='["hildobby"]');
{% endset %}

{% set nomad_bridge_transactions %}
ALTER VIEW nomad_ethereum.view_bridge_transactions SET TBLPROPERTIES ('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='nomad',
                                                    'dune.data_explorer.contributors'='["springzh"]');
{% endset %}

{% set prices_usd_latest %}
ALTER VIEW prices.usd_latest  SET TBLPROPERTIES ('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='prices',
                                                    'dune.data_explorer.contributors'='["hildobby"]');
{% endset %}

{% set sudoswap_ethereum_events %}
ALTER TABLE sudoswap_ethereum.events  SET TBLPROPERTIES ('dune.public'='true',
                                                        'dune.data_explorer.blockchains'='["ethereum"]',
                                                        'dune.data_explorer.category'='abstraction',
                                                        'dune.data_explorer.abstraction.type'='project',
                                                        'dune.data_explorer.abstraction.name'='sudoswap',
                                                        'dune.data_explorer.contributors'='["ilemi"]');
{% endset %}

{% set sudoswap_ethereum_trades %}
ALTER VIEW sudoswap_ethereum.trades  SET TBLPROPERTIES ('dune.public'='true',
                                                            'dune.data_explorer.blockchains'='["ethereum"]',
                                                            'dune.data_explorer.category'='abstraction',
                                                            'dune.data_explorer.abstraction.type'='project',
                                                            'dune.data_explorer.abstraction.name'='sudoswap',
                                                            'dune.data_explorer.contributors'='["ilemi"]');
{% endset %}

{% set sudoswap_ethereum_fees %}
ALTER VIEW sudoswap_ethereum.fees  SET TBLPROPERTIES ('dune.public'='true',
                                                        'dune.data_explorer.blockchains'='["ethereum"]',
                                                        'dune.data_explorer.category'='abstraction',
                                                        'dune.data_explorer.abstraction.type'='project',
                                                        'dune.data_explorer.abstraction.name'='sudoswap',
                                                        'dune.data_explorer.contributors'='["ilemi"]');
{% endset %}


{% set cryptopunks_ethereum_events %}
ALTER TABLE cryptopunks_ethereum.events  SET TBLPROPERTIES ('dune.public'='true',
                                                        'dune.data_explorer.blockchains'='["ethereum"]',
                                                        'dune.data_explorer.category'='abstraction',
                                                        'dune.data_explorer.abstraction.type'='project',
                                                        'dune.data_explorer.abstraction.name'='cryptopunks',
                                                        'dune.data_explorer.contributors'='["masquot"]');
{% endset %}

{% set cryptopunks_ethereum_trades %}
ALTER VIEW cryptopunks_ethereum.trades  SET TBLPROPERTIES ('dune.public'='true',
                                                            'dune.data_explorer.blockchains'='["ethereum"]',
                                                            'dune.data_explorer.category'='abstraction',
                                                            'dune.data_explorer.abstraction.type'='project',
                                                            'dune.data_explorer.abstraction.name'='cryptopunks',
                                                            'dune.data_explorer.contributors'='["masquot"]');
{% endset %}
{% set safe_ethereum_safes %}
ALTER TABLE safe_ethereum.safes SET TBLPROPERTIES ('dune.public'='true',
                                            'dune.data_explorer.blockchains'='["ethereum"]',
                                            'dune.data_explorer.category'='abstraction',
                                            'dune.data_explorer.abstraction.type'='project',
                                            'dune.data_explorer.abstraction.name'='safe',
                                            'dune.data_explorer.contributors'='["sche"]');
{% endset %}

{% set safe_ethereum_eth_transfers %}
ALTER TABLE safe_ethereum.eth_transfers SET TBLPROPERTIES ('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='safe',
                                                    'dune.data_explorer.contributors'='["sche"]');
{% endset %}

{% set prices_tokens %}
ALTER VIEW prices.tokens SET TBLPROPERTIES ('dune.public'='true',
                                                'dune.data_explorer.blockchains'='["ethereum", "bnb", "solana"]',
                                                'dune.data_explorer.category'='abstraction',
                                                'dune.data_explorer.abstraction.type'='sector',
                                                'dune.data_explorer.abstraction.name'='prices',
                                                'dune.data_explorer.contributors'='["aalan3", "jeff-dude"]');
{% endset %}

{% set tornado_cash_deposits %}
ALTER TABLE tornado_cash.deposits SET TBLPROPERTIES ('dune.public'='true',
                                                'dune.data_explorer.blockchains'='["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum"]',
                                                'dune.data_explorer.category'='abstraction',
                                                'dune.data_explorer.abstraction.type'='project',
                                                'dune.data_explorer.abstraction.name'='tornado_cash',
                                                'dune.data_explorer.contributors'='["hildobby", "dot2dotseurat"]');
{% endset %}

{% set tornado_cash_withdrawals %}
ALTER TABLE tornado_cash.withdrawals SET TBLPROPERTIES ('dune.public'='true',
                                                'dune.data_explorer.blockchains'='["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum"]',
                                                'dune.data_explorer.category'='abstraction',
                                                'dune.data_explorer.abstraction.type'='project',
                                                'dune.data_explorer.abstraction.name'='tornado_cash',
                                                'dune.data_explorer.contributors'='["hildobby", "dot2dotseurat"]');
{% endset %}

{% set transfers_optimism_eth %}
ALTER TABLE transfers_optimism.eth SET TBLPROPERTIES ('dune.public'='true',
                                                'dune.data_explorer.blockchains'='["optimism"]',
                                                'dune.data_explorer.category'='abstraction',
                                                'dune.data_explorer.abstraction.type'='sector',
                                                'dune.data_explorer.abstraction.name'='transfers',
                                                'dune.data_explorer.contributors'='["msilb7", "chuxinh"]');
{% endset %}

{% set uniswap_v3_optimism_pools %}
ALTER VIEW uniswap_v3_optimism.pools SET TBLPROPERTIES ('dune.public'='true',
                                                'dune.data_explorer.blockchains'='["optimism"]',
                                                'dune.data_explorer.category'='abstraction',
                                                'dune.data_explorer.abstraction.type'='project',
                                                'dune.data_explorer.abstraction.name'='uniswap_v3',
                                                'dune.data_explorer.contributors'='["msilb7", "chuxinh"]');
{% endset %}

{% set archipelago_ethereum_events %}
ALTER TABLE archipelago_ethereum.events  SET TBLPROPERTIES ('dune.public'='true',
                                                        'dune.data_explorer.blockchains'='["ethereum"]',
                                                        'dune.data_explorer.category'='abstraction',
                                                        'dune.data_explorer.abstraction.type'='project',
                                                        'dune.data_explorer.abstraction.name'='archipelago',
                                                        'dune.data_explorer.contributors'='["0xRob"]');
{% endset %}

{% set archipelago_ethereum_trades %}
ALTER VIEW archipelago_ethereum.trades  SET TBLPROPERTIES ('dune.public'='true',
                                                            'dune.data_explorer.blockchains'='["ethereum"]',
                                                            'dune.data_explorer.category'='abstraction',
                                                            'dune.data_explorer.abstraction.type'='project',
                                                            'dune.data_explorer.abstraction.name'='archipelago',
                                                            'dune.data_explorer.contributors'='["0xRob"]');
{% endset %}

{% set archipelago_ethereum_fees %}
ALTER VIEW archipelago_ethereum.fees  SET TBLPROPERTIES ('dune.public'='true',
                                                        'dune.data_explorer.blockchains'='["ethereum"]',
                                                        'dune.data_explorer.category'='abstraction',
                                                        'dune.data_explorer.abstraction.type'='project',
                                                        'dune.data_explorer.abstraction.name'='archipelago',
                                                        'dune.data_explorer.contributors'='["0xRob"]');
{% endset %}

{% set cow_protocol_ethereum_solvers %}
ALTER VIEW cow_protocol_ethereum.solvers SET TBLPROPERTIES ('dune.public'='true',
                                                'dune.data_explorer.blockchains'='["ethereum"]',
                                                'dune.data_explorer.category'='abstraction',
                                                'dune.data_explorer.abstraction.type'='project',
                                                'dune.data_explorer.abstraction.name'='cow_protocol',
                                                'dune.data_explorer.contributors'='["bh2smith", "gentrexha"]');
{% endset %}


{% do run_query(transfers_ethereum_erc20) %}
{% do run_query(nomad_bridge_transactions) %}
{% do run_query(prices_usd_latest) %}
{% do run_query(sudoswap_ethereum_events) %}
{% do run_query(sudoswap_ethereum_trades) %}
{% do run_query(sudoswap_ethereum_fees) %}
{% do run_query(cryptopunks_ethereum_events) %}
{% do run_query(cryptopunks_ethereum_trades) %}
{% do run_query(safe_ethereum_safes) %}
{% do run_query(safe_ethereum_eth_transfers) %}
{% do run_query(prices_tokens) %}
{% do run_query(addresses_optimism_airdrop_1) %}
{% do run_query(addresses_ethereum_safe_airdrop) %}
{% do run_query(tornado_cash_deposits) %}
{% do run_query(tornado_cash_withdrawals) %}
{% do run_query(tokens_erc20) %}
{% do run_query(tokens_nft) %}
{% do run_query(transfers_optimism_eth) %}
{% do run_query(uniswap_v3_optimism_pools) %}
{% do run_query(archipelago_ethereum_events) %}
{% do run_query(archipelago_ethereum_trades) %}
{% do run_query(archipelago_ethereum_fees) %}
{% do run_query(cow_protocol_ethereum_solvers) %}

{% do log("Tables generated", info=True) %}
{%- else -%}
{%- endif -%}
{% endmacro %}
