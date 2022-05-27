{% macro alter_tblproperties_opensea_trades() -%}
{%- if target.name == 'prod'-%}
ALTER VIEW opensea.trades SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum","solana"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='opensea',
                                                    'dune.data_explorer.contributors'='["rchen8","soispoke"]');
{%- else -%}
{%- endif -%}
{%- endmacro %}
