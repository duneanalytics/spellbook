-- The cross-chain lending tables are built from a hand-written list of chain models
-- (lending_<chain>_base_supply, lending_<chain>_base_borrow, lending_<chain>_base_flashloans).
-- A chain model that exists but is not in the list builds without error and its rows never reach the
-- sector table. This test reads the dbt graph and returns the name of every such chain model.
-- union_model_name: the model that holds the list, such as lending_base_flashloans.
-- exclude: chain models that are left out on purpose.
{% test lending_sector_union_includes_all_chain_models(model, union_model_name, exclude=[]) %}

{%- set missing = [] -%}
{%- if execute -%}
  {%- set suffix = union_model_name | replace('lending_', '', 1) -%}
  {%- set union_nodes = graph.nodes.values() | selectattr('resource_type', 'equalto', 'model') | selectattr('name', 'equalto', union_model_name) | list -%}
  {%- if union_nodes | length != 1 -%}
    {{ exceptions.raise_compiler_error("lending_sector_union_includes_all_chain_models: model '" ~ union_model_name ~ "' not found") }}
  {%- endif -%}
  {%- set referenced = union_nodes[0].depends_on.nodes -%}
  {%- for node in graph.nodes.values() | selectattr('resource_type', 'equalto', 'model') -%}
    {%- if node.name.startswith('lending_') and node.name.endswith('_' ~ suffix) and node.name != union_model_name
          and node.name not in exclude and node.unique_id not in referenced -%}
      {%- do missing.append(node.name) -%}
    {%- endif -%}
  {%- endfor -%}
{%- endif %}

{% if missing | length > 0 -%}
select chain_model_missing_from_union
from (values
  {%- for name in missing %}
  ('{{ name }}'){% if not loop.last %},{% endif %}
  {%- endfor %}
) as t (chain_model_missing_from_union)
{%- else -%}
select cast(null as varchar) as chain_model_missing_from_union
where false
{%- endif %}

{% endtest %}
