{% macro alter_tblproperties_transfers_ethereum_erc20() -%}
{%- if target.name == 'prod'-%}
ALTER VIEW transfers_ethereum.erc20 SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='sector',
                                                    'dune.data_explorer.abstraction.name'='transfers',
                                                    'dune.data_explorer.contributors'='["soispoke","dot2dotseurat"]');
{%- else -%}
{%- endif -%}
{%- endmacro %}
