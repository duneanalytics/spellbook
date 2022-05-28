{% macro alter_tblproperties_balances_ethereum_erc20_day() -%}
{%- if target.name == 'prod'-%}
ALTER VIEW balances_ethereum.erc20_day SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='balances',
                                                    'dune.data_explorer.contributors'='["soispoke","dot2dotseurat"]');
{%- else -%}
{%- endif -%}
{%- endmacro %}