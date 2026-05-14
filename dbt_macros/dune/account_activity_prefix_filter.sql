{# Generate an `address_prefix` partition predicate for `solana.account_activity`. #}
{# The table is partitioned by `(year, month, address_prefix)` where             #}
{# `address_prefix = substring(address, 1, 2)`. Trino cannot derive this         #}
{# predicate from `address = '<literal>'` (trinodb/trino#19455), so we must      #}
{# emit it explicitly to enable partition pruning at plan time.                  #}
{#                                                                                #}
{# Accepts either a single address string or a list of addresses. Computes the   #}
{# 2-char prefix(es) at compile time and emits either `= 'XX'` or `IN (...)`.    #}
{# Pass `alias` when the column needs a table alias prefix (e.g. `i`).           #}
{% macro account_activity_prefix_filter(addresses, alias='') -%}
  {%- if addresses is string -%}
    {%- set addr_list = [addresses] -%}
  {%- else -%}
    {%- set addr_list = addresses -%}
  {%- endif -%}
  {%- set prefixes = [] -%}
  {%- for addr in addr_list -%}
    {%- do prefixes.append(addr[:2]) -%}
  {%- endfor -%}
  {%- set unique_prefixes = prefixes | unique | list -%}
  {%- set col_prefix = alias ~ '.' if alias else '' -%}
  {%- if unique_prefixes | length == 1 -%}
AND {{ col_prefix }}address_prefix = '{{ unique_prefixes[0] }}'
  {%- else -%}
AND {{ col_prefix }}address_prefix IN ({% for p in unique_prefixes %}'{{ p }}'{% if not loop.last %}, {% endif %}{% endfor %})
  {%- endif -%}
{%- endmacro %}
