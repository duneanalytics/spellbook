{% macro deprecated_evm_chains() %}
  {#
    EVM chains whose ingestion is turned off: the harvester is disabled and
    the raw, base, ABI and decoding jobs are off. Source of truth is the
    Ingestion Data Eng Tracker in Notion.

    Their per-chain models stay as frozen `table` + `tag:static` models.
    Cross-chain models filter them out of their chain lists:
      {% set chains = some_chain_list() | reject('in', deprecated_evm_chains()) | list %}
    Label stores and input registries that per-chain models read keep them.

    Deprecating a chain = add it here and freeze its per-chain models.
    Order: alphabetical.
  #}
  {{ return([
    'b3',
    'blast',
    'bob',
    'corn',
    'degen',
    'fantom',
    'henesys',
    'nova',
    'rise_testnet',
    'shape',
    'sophon',
    'stable',
    'story',
    'superseed',
    'tac',
    'zetachain',
    'zkevm',
    'zora',
  ]) }}
{% endmacro %}
