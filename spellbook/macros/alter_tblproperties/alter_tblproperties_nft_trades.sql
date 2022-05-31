{% macro alter_tblproperties_nft_trades() -%}
{%- if target.name == 'prod'-%}
ALTER VIEW nft.trades SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum","solana"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='nft',
                                                    'dune.data_explorer.contributors'='["soispoke"]');
{%- else -%}
{%- endif -%}
{%- endmacro %}
