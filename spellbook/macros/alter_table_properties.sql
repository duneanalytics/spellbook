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

{% set magiceden_trades %}
ALTER TABLE magiceden.trades SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["solana"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='magiceden',
                                                    'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set nft_trades %}
ALTER TABLE nft.trades SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum","solana"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='nft',
                                                    'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set opensea_active_traders_day %}
ALTER VIEW opensea.active_traders_day SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum","solana"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='opensea',
                                                    'dune.data_explorer.contributors'='["rchen8"]');
{% endset %}

{% set opensea_trades %}
ALTER TABLE opensea.trades SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum","solana"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='opensea',
                                                    'dune.data_explorer.contributors'='["rchen8","soispoke"]');
{% endset %}

{% set opensea_txns_day %}
ALTER VIEW opensea.txns_day SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum","solana"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='opensea',
                                                    'dune.data_explorer.contributors'='["rchen8"]');
{% endset %}

{% set opensea_volume_day %}
ALTER VIEW opensea.volume_day SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum","solana"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='opensea',
                                                    'dune.data_explorer.contributors'='["rchen8"]');
{% endset %}

{% set tokens_ethereum_erc20 %}
ALTER VIEW tokens_ethereum.erc20 SET TBLPROPERTIES ('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='tokens',
                                                    'dune.data_explorer.contributors'='["soispoke","dot2dotseurat"]');
{% endset %}

{% set transfers_ethereum_erc20 %}
ALTER VIEW transfers_ethereum.erc20 SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='transfers',
                                                    'dune.data_explorer.contributors'='["soispoke","dot2dotseurat"]');
{% endset %}

{% set tokens_ethereum_nft %}
ALTER VIEW tokens_ethereum.nft SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='tokens',
                                                    'dune.data_explorer.contributors'='["dot2dotseurat","soispoke"]');
{% endset %}

{% set uniswap_trades %}
ALTER TABLE uniswap.trades SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='uniswap',
                                                    'dune.data_explorer.contributors'='["soispoke"]');
{% endset %}

{% set seaport_ethereum_view_transactions %}
ALTER VIEW seaport_ethereum.view_transactions SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='seaport',
                                                    'dune.data_explorer.contributors'='["sohawk","soispoke"]');
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

{% do run_query(balances_ethereum_erc20_day) %}
{% do run_query(balances_ethereum_erc20_hour) %}
{% do run_query(balances_ethereum_erc20_latest) %}
{% do run_query(balances_ethereum_erc721_day) %}
{% do run_query(balances_ethereum_erc721_hour) %}
{% do run_query(balances_ethereum_erc721_latest) %}
{% do run_query(balances_ethereum_erc1155_day) %}
{% do run_query(balances_ethereum_erc1155_hour) %}
{% do run_query(balances_ethereum_erc1155_latest) %}
{% do run_query(magiceden_trades) %}
{% do run_query(nft_trades) %}
{% do run_query(opensea_active_traders_day) %}
{% do run_query(opensea_trades) %}
{% do run_query(opensea_txns_day) %}
{% do run_query(opensea_volume_day) %}
{% do run_query(tokens_ethereum_erc20) %}
{% do run_query(transfers_ethereum_erc20) %}
{% do run_query(tokens_ethereum_nft) %}
{% do run_query(seaport_ethereum_view_transactions) %}
{% do run_query(uniswap_trades) %}
{% do run_query(ens_view_expirations) %}
{% do run_query(ens_view_registrations) %}
{% do run_query(ens_view_registries) %}
{% do run_query(ens_view_renewals) %}

{% do log("Tables generated", info=True) %}
{%- else -%}
{%- endif -%}
{% endmacro %}