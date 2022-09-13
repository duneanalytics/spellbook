{% macro alter_table_properties() %}
{%- if target.name == 'prod'-%}

{% set balances_ethereum_erc20_day %}
ALTER VIEW balances_ethereum.erc20_day SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='balances',
                                                    'dune.data_explorer.contributors'='["soispoke","dot2dotseurat"]');
{% endset %}

{% set balances_ethereum_erc20_hour %}
ALTER VIEW balances_ethereum.erc20_hour SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='balances',
                                                    'dune.data_explorer.contributors'='["soispoke","dot2dotseurat"]');
{% endset %}

{% set balances_ethereum_erc20_latest %}
ALTER VIEW balances_ethereum.erc20_latest SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='balances',
                                                    'dune.data_explorer.contributors'='["soispoke","dot2dotseurat"]');
{% endset %}

{% set balances_ethereum_erc721_day %}
ALTER VIEW balances_ethereum.erc721_day SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='balances',
                                                    'dune.data_explorer.contributors'='["hildobby","soispoke","dot2dotseurat"]');
{% endset %}

{% set balances_ethereum_erc721_hour %}
ALTER VIEW balances_ethereum.erc721_hour SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='balances',
                                                    'dune.data_explorer.contributors'='["hildobby","soispoke","dot2dotseurat"]');
{% endset %}

{% set balances_ethereum_erc721_latest %}
ALTER VIEW balances_ethereum.erc721_latest SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='balances',
                                                    'dune.data_explorer.contributors'='["hildobby","soispoke","dot2dotseurat"]');
{% endset %}

{% set balances_ethereum_erc1155_day %}
ALTER VIEW balances_ethereum.erc1155_day SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='balances',
                                                    'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set balances_ethereum_erc1155_hour %}
ALTER VIEW balances_ethereum.erc1155_hour SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='balances',
                                                    'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set balances_ethereum_erc1155_latest %}
ALTER VIEW balances_ethereum.erc1155_latest SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='balances',
                                                    'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set opensea_events %}
ALTER VIEW opensea.events SET TBLPROPERTIES('dune.public'='true',
                                            'dune.data_explorer.blockchains'='["ethereum","solana"]',
                                            'dune.data_explorer.category'='abstraction',
                                            'dune.data_explorer.abstraction.type'='project',
                                            'dune.data_explorer.abstraction.name'='opensea',
                                            'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set opensea_trades %}
ALTER VIEW opensea.trades SET TBLPROPERTIES('dune.public'='true',
                                            'dune.data_explorer.blockchains'='["ethereum","solana"]',
                                            'dune.data_explorer.category'='abstraction',
                                            'dune.data_explorer.abstraction.type'='project',
                                            'dune.data_explorer.abstraction.name'='opensea',
                                            'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set opensea_mints %}
ALTER VIEW opensea.mints SET TBLPROPERTIES('dune.public'='true',
                                            'dune.data_explorer.blockchains'='["ethereum","solana"]',
                                            'dune.data_explorer.category'='abstraction',
                                            'dune.data_explorer.abstraction.type'='project',
                                            'dune.data_explorer.abstraction.name'='opensea',
                                            'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set opensea_burns %}
ALTER VIEW opensea.burns SET TBLPROPERTIES('dune.public'='true',
                                            'dune.data_explorer.blockchains'='["ethereum","solana"]',
                                            'dune.data_explorer.category'='abstraction',
                                            'dune.data_explorer.abstraction.type'='project',
                                            'dune.data_explorer.abstraction.name'='opensea',
                                            'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set opensea_fees %}
ALTER VIEW opensea.fees SET TBLPROPERTIES('dune.public'='true',
                                            'dune.data_explorer.blockchains'='["ethereum","solana"]',
                                            'dune.data_explorer.category'='abstraction',
                                            'dune.data_explorer.abstraction.type'='project',
                                            'dune.data_explorer.abstraction.name'='opensea',
                                            'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set looksrare_ethereum_events %}
ALTER TABLE looksrare_ethereum.events SET TBLPROPERTIES('dune.public'='true',
                                                        'dune.data_explorer.blockchains'='["ethereum"]',
                                                        'dune.data_explorer.category'='abstraction',
                                                        'dune.data_explorer.abstraction.type'='project',
                                                        'dune.data_explorer.abstraction.name'='looksrare',
                                                        'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set looksrare_ethereum_trades %}
ALTER VIEW looksrare_ethereum.trades SET TBLPROPERTIES('dune.public'='true',
                                                        'dune.data_explorer.blockchains'='["ethereum"]',
                                                        'dune.data_explorer.category'='abstraction',
                                                        'dune.data_explorer.abstraction.type'='project',
                                                        'dune.data_explorer.abstraction.name'='looksrare',
                                                        'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set looksrare_ethereum_mints %}
ALTER VIEW looksrare_ethereum.mints SET TBLPROPERTIES('dune.public'='true',
                                                        'dune.data_explorer.blockchains'='["ethereum"]',
                                                        'dune.data_explorer.category'='abstraction',
                                                        'dune.data_explorer.abstraction.type'='project',
                                                        'dune.data_explorer.abstraction.name'='looksrare',
                                                        'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set looksrare_ethereum_burns %}
ALTER VIEW looksrare_ethereum.burns SET TBLPROPERTIES('dune.public'='true',
                                                        'dune.data_explorer.blockchains'='["ethereum"]',
                                                        'dune.data_explorer.category'='abstraction',
                                                        'dune.data_explorer.abstraction.type'='project',
                                                        'dune.data_explorer.abstraction.name'='looksrare',
                                                        'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set looksrare_ethereum_fees %}
ALTER VIEW looksrare_ethereum.fees SET TBLPROPERTIES('dune.public'='true',
                                                        'dune.data_explorer.blockchains'='["ethereum"]',
                                                        'dune.data_explorer.category'='abstraction',
                                                        'dune.data_explorer.abstraction.type'='project',
                                                        'dune.data_explorer.abstraction.name'='looksrare',
                                                        'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set x2y2_ethereum_events %}
ALTER TABLE x2y2_ethereum.events SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='x2y2',
                                                    'dune.data_explorer.contributors'='["hildobby","soispoke"]');
{% endset %}

{% set x2y2_ethereum_trades %}
ALTER VIEW x2y2_ethereum.trades SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='x2y2',
                                                    'dune.data_explorer.contributors'='["hildobby","soispoke"]');
{% endset %}

{% set x2y2_ethereum_mints %}
ALTER VIEW x2y2_ethereum.mints SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='x2y2',
                                                    'dune.data_explorer.contributors'='["hildobby","soispoke"]');
{% endset %}

{% set x2y2_ethereum_burns %}
ALTER VIEW x2y2_ethereum.burns SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='x2y2',
                                                    'dune.data_explorer.contributors'='["hildobby","soispoke"]');
{% endset %}

{% set x2y2_ethereum_fees %}
ALTER VIEW x2y2_ethereum.fees SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='x2y2',
                                                    'dune.data_explorer.contributors'='["hildobby","soispoke"]');
{% endset %}

{% set magiceden_events %}
ALTER VIEW magiceden.events SET TBLPROPERTIES('dune.public'='true',
                                                'dune.data_explorer.blockchains'='["solana"]',
                                                'dune.data_explorer.category'='abstraction',
                                                'dune.data_explorer.abstraction.type'='project',
                                                'dune.data_explorer.abstraction.name'='magiceden',
                                                'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set magiceden_trades %}
ALTER VIEW magiceden.trades SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["solana"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='magiceden',
                                                    'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set magiceden_mints %}
ALTER VIEW magiceden.mints SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["solana"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='magiceden',
                                                    'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set magiceden_fees %}
ALTER VIEW magiceden.fees SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["solana"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='magiceden',
                                                    'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set nft_events %}
ALTER VIEW nft.events SET TBLPROPERTIES('dune.public'='true',
                                        'dune.data_explorer.blockchains'='["ethereum","solana"]',
                                        'dune.data_explorer.category'='abstraction',
                                        'dune.data_explorer.abstraction.type'='sector',
                                        'dune.data_explorer.abstraction.name'='nft',
                                        'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set nft_trades %}
ALTER VIEW nft.trades SET TBLPROPERTIES('dune.public'='true',
                                        'dune.data_explorer.blockchains'='["ethereum","solana"]',
                                        'dune.data_explorer.category'='abstraction',
                                        'dune.data_explorer.abstraction.type'='sector',
                                        'dune.data_explorer.abstraction.name'='nft',
                                        'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set nft_mints %}
ALTER VIEW nft.mints SET TBLPROPERTIES('dune.public'='true',
                                        'dune.data_explorer.blockchains'='["ethereum","solana"]',
                                        'dune.data_explorer.category'='abstraction',
                                        'dune.data_explorer.abstraction.type'='sector',
                                        'dune.data_explorer.abstraction.name'='nft',
                                        'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set nft_burns %}
ALTER VIEW nft.burns SET TBLPROPERTIES('dune.public'='true',
                                        'dune.data_explorer.blockchains'='["ethereum","solana"]',
                                        'dune.data_explorer.category'='abstraction',
                                        'dune.data_explorer.abstraction.type'='sector',
                                        'dune.data_explorer.abstraction.name'='nft',
                                        'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set nft_fees %}
ALTER VIEW nft.fees SET TBLPROPERTIES('dune.public'='true',
                                        'dune.data_explorer.blockchains'='["ethereum","solana"]',
                                        'dune.data_explorer.category'='abstraction',
                                        'dune.data_explorer.abstraction.type'='sector',
                                        'dune.data_explorer.abstraction.name'='nft',
                                        'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set labels_all %}
ALTER TABLE labels.all SET TBLPROPERTIES('dune.public'='true',
                                        'dune.data_explorer.blockchains'='["ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]',
                                        'dune.data_explorer.category'='abstraction',
                                        'dune.data_explorer.abstraction.type'='sector',
                                        'dune.data_explorer.abstraction.name'='labels',
                                        'dune.data_explorer.contributors'='["soispoke","hildobby"]');
{% endset %}

{% set labels_cex %}
ALTER VIEW labels.cex SET TBLPROPERTIES('dune.public'='true',
                                        'dune.data_explorer.blockchains'='["ethereum"]',
                                        'dune.data_explorer.category'='abstraction',
                                        'dune.data_explorer.abstraction.type'='sector',
                                        'dune.data_explorer.abstraction.name'='labels',
                                        'dune.data_explorer.contributors'='["hildobby"]');
{% endset %}

{% set labels_funds %}
ALTER VIEW labels.funds SET TBLPROPERTIES('dune.public'='true',
                                        'dune.data_explorer.blockchains'='["ethereum"]',
                                        'dune.data_explorer.category'='abstraction',
                                        'dune.data_explorer.abstraction.type'='sector',
                                        'dune.data_explorer.abstraction.name'='labels',
                                        'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set labels_nft %}
ALTER VIEW labels.nft SET TBLPROPERTIES('dune.public'='true',
                                        'dune.data_explorer.blockchains'='["ethereum","solana"]',
                                        'dune.data_explorer.category'='abstraction',
                                        'dune.data_explorer.abstraction.type'='sector',
                                        'dune.data_explorer.abstraction.name'='labels',
                                        'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set labels_safe %}
ALTER VIEW labels.safe SET TBLPROPERTIES('dune.public'='true',
                                        'dune.data_explorer.blockchains'='["ethereum"]',
                                        'dune.data_explorer.category'='abstraction',
                                        'dune.data_explorer.abstraction.type'='sector',
                                        'dune.data_explorer.abstraction.name'='labels',
                                        'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set labels_submitted_contracts %}
ALTER VIEW labels.submitted_contracts SET TBLPROPERTIES('dune.public'='true',
                                        'dune.data_explorer.blockchains'='["ethereum", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]',
                                        'dune.data_explorer.category'='abstraction',
                                        'dune.data_explorer.abstraction.type'='sector',
                                        'dune.data_explorer.abstraction.name'='labels',
                                        'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set labels_tornado_cash %}
ALTER VIEW labels.tornado_cash SET TBLPROPERTIES('dune.public'='true',
                                        'dune.data_explorer.blockchains'='["ethereum", "arbitrum","bnb","avalanche_c","optimism","gnosis"]',
                                        'dune.data_explorer.category'='abstraction',
                                        'dune.data_explorer.abstraction.type'='sector',
                                        'dune.data_explorer.abstraction.name'='labels',
                                        'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set nft_aggregators %}
ALTER VIEW nft.aggregators SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["avalanche_c","bnb","ethereum","polygon"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='nft',
                                                    'dune.data_explorer.contributors'='["hildobby","soispoke"]');
{% endset %}

{% set nft_linked_addresses %}
ALTER TABLE nft.linked_addresses SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum","solana"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='nft',
                                                    'dune.data_explorer.contributors'='["springzh"]');
{% endset %}

{% set tokens_erc20 %}
ALTER VIEW tokens.erc20 SET TBLPROPERTIES ('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["avalanche_c","bnb","ethereum","optimism", "gnosis"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='tokens',
                                                    'dune.data_explorer.contributors'='["0xManny","hildobby","soispoke","dot2dotseurat"]');
{% endset %}

{% set tokens_nft %}
ALTER VIEW tokens.nft SET TBLPROPERTIES ('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["avalanche_c","bnb","ethereum","optimism", "gnosis"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='tokens',
                                                    'dune.data_explorer.contributors'='["0xManny","hildobby","soispoke","dot2dotseurat"]');
{% endset %}

{% set transfers_ethereum_erc20 %}
ALTER VIEW transfers_ethereum.erc20 SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='transfers',
                                                    'dune.data_explorer.contributors'='["soispoke","dot2dotseurat"]');
{% endset %}

{% set uniswap_v1_ethereum_trades %}
ALTER TABLE uniswap_v1_ethereum.trades SET TBLPROPERTIES('dune.public'='true',
                                                            'dune.data_explorer.blockchains'='["ethereum"]',
                                                            'dune.data_explorer.category'='abstraction',
                                                            'dune.data_explorer.abstraction.type'='project',
                                                            'dune.data_explorer.abstraction.name'='uniswap_v1',
                                                            'dune.data_explorer.contributors'='["jeff-dude"]');
{% endset %}

{% set uniswap_v2_ethereum_trades %}
ALTER TABLE uniswap_v2_ethereum.trades SET TBLPROPERTIES('dune.public'='true',
                                                            'dune.data_explorer.blockchains'='["ethereum"]',
                                                            'dune.data_explorer.category'='abstraction',
                                                            'dune.data_explorer.abstraction.type'='project',
                                                            'dune.data_explorer.abstraction.name'='uniswap_v2',
                                                            'dune.data_explorer.contributors'='["jeff-dude"]');
{% endset %}

{% set uniswap_ethereum_trades %}
ALTER VIEW uniswap_ethereum.trades SET TBLPROPERTIES('dune.public'='true',
                                                        'dune.data_explorer.blockchains'='["ethereum"]',
                                                        'dune.data_explorer.category'='abstraction',
                                                        'dune.data_explorer.abstraction.type'='project',
                                                        'dune.data_explorer.abstraction.name'='uniswap',
                                                        'dune.data_explorer.contributors'='["jeff-dude"]');
{% endset %}

{% set uniswap_trades %}
ALTER VIEW uniswap.trades SET TBLPROPERTIES('dune.public'='true',
                                                'dune.data_explorer.blockchains'='["ethereum"]',
                                                'dune.data_explorer.category'='abstraction',
                                                'dune.data_explorer.abstraction.type'='project',
                                                'dune.data_explorer.abstraction.name'='uniswap',
                                                'dune.data_explorer.contributors'='["jeff-dude"]');
{% endset %}

{% set dex_trades %}
ALTER VIEW dex.trades SET TBLPROPERTIES('dune.public'='true',
                                            'dune.data_explorer.blockchains'='["ethereum"]',
                                            'dune.data_explorer.category'='abstraction',
                                            'dune.data_explorer.abstraction.type'='sector',
                                            'dune.data_explorer.abstraction.name'='dex',
                                            'dune.data_explorer.contributors'='["jeff-dude"]');
{% endset %}

{% set seaport_ethereum_view_transactions %}
ALTER VIEW seaport_ethereum.view_transactions SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='seaport',
                                                    'dune.data_explorer.contributors'='["sohawk","soispoke"]');
{% endset %}

{% set seaport_ethereum_transfers %}
ALTER TABLE seaport_ethereum.transfers SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='seaport',
                                                    'dune.data_explorer.contributors'='["sohawk","soispoke"]');
{% endset %}

{% set addresses_optimism_airdrop_1 %}
ALTER TABLE addresses_optimism.airdrop_1 SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["optimism"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='airdrop_1',
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

{% set ens_view_expirations %}
ALTER VIEW ens.view_expirations SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='ens',
                                                    'dune.data_explorer.contributors'='["antonio-mendes","mewwts"]');
{% endset %}

{% set ens_view_registrations %}
ALTER VIEW ens.view_registrations SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='ens',
                                                    'dune.data_explorer.contributors'='["antonio-mendes","mewwts"]');
{% endset %}

{% set ens_view_registries %}
ALTER VIEW ens.view_registries SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='ens',
                                                    'dune.data_explorer.contributors'='["antonio-mendes","mewwts"]');
{% endset %}

{% set ens_view_renewals %}
ALTER VIEW ens.view_renewals SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='ens',
                                                    'dune.data_explorer.contributors'='["antonio-mendes","mewwts"]');
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


{% do run_query(balances_ethereum_erc20_day) %}
{% do run_query(balances_ethereum_erc20_hour) %}
{% do run_query(balances_ethereum_erc20_latest) %}
{% do run_query(balances_ethereum_erc721_day) %}
{% do run_query(balances_ethereum_erc721_hour) %}
{% do run_query(balances_ethereum_erc721_latest) %}
{% do run_query(balances_ethereum_erc1155_day) %}
{% do run_query(balances_ethereum_erc1155_hour) %}
{% do run_query(balances_ethereum_erc1155_latest) %}
{% do run_query(opensea_events) %}
{% do run_query(opensea_trades) %}
{% do run_query(opensea_mints) %}
{% do run_query(opensea_burns) %}
{% do run_query(opensea_fees) %}
{% do run_query(looksrare_ethereum_events) %}
{% do run_query(looksrare_ethereum_trades) %}
{% do run_query(looksrare_ethereum_mints) %}
{% do run_query(looksrare_ethereum_burns) %}
{% do run_query(looksrare_ethereum_fees) %}
{% do run_query(x2y2_ethereum_events) %}
{% do run_query(x2y2_ethereum_trades) %}
{% do run_query(x2y2_ethereum_mints) %}
{% do run_query(x2y2_ethereum_burns) %}
{% do run_query(x2y2_ethereum_fees) %}
{% do run_query(magiceden_events) %}
{% do run_query(magiceden_trades) %}
{% do run_query(magiceden_mints) %}
{% do run_query(magiceden_fees) %}
{% do run_query(nft_events) %}
{% do run_query(nft_trades) %}
{% do run_query(nft_mints) %}
{% do run_query(nft_burns) %}
{% do run_query(nft_fees) %}
{% do run_query(labels_all) %}
{% do run_query(labels_cex) %}
{% do run_query(labels_funds) %}
{% do run_query(labels_nft) %}
{% do run_query(labels_safe) %}
{% do run_query(labels_submitted_contracts) %}
{% do run_query(labels_tornado_cash) %}
{% do run_query(uniswap_v1_ethereum_trades) %}
{% do run_query(uniswap_v2_ethereum_trades) %}
{% do run_query(uniswap_ethereum_trades) %}
{% do run_query(uniswap_trades) %}
{% do run_query(dex_trades) %}
{% do run_query(nft_aggregators) %}
{% do run_query(nft_linked_addresses) %}
{% do run_query(transfers_ethereum_erc20) %}
{% do run_query(seaport_ethereum_view_transactions) %}
{% do run_query(seaport_ethereum_transfers) %}
{% do run_query(uniswap_trades) %}
{% do run_query(ens_view_expirations) %}
{% do run_query(ens_view_registrations) %}
{% do run_query(ens_view_registries) %}
{% do run_query(ens_view_renewals) %}
{% do run_query(cex_addresses) %}
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
{% do run_query(airdrop_optimism_addresses_1) %}
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

{% do log("Tables generated", info=True) %}
{%- else -%}
{%- endif -%}
{% endmacro %}
