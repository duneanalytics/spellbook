{% macro get_balancer_link() %}
      create or replace function get_balancer_link(chain_ STRING, address STRING)
      returns STRING 
      return concat('<a href="','https://app.balancer.fi/#/',chain_,'/pool/', address, '"target ="_blank">', 'balancer')
{% endmacro %}
