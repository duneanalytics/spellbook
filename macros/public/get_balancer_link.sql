{% macro get_balancer_link() %}
      create or replace function get_balancer_link(chain_ STRING, address STRING)
      returns STRING 
      return 
      SELECT
      CASE WHEN 
      'avalanche_c' = chain_ THEN concat('<a href="','https://app.balancer.fi/#/avalanche/pool/', address, '"target ="_blank">', 'balancer')
      'gnosis' = chain_ THEN concat('<a href="','https://app.balancer.fi/#/gnosis-chain/pool/', address, '"target ="_blank">', 'balancer')
      ELSE concat('<a href="','https://app.balancer.fi/#/',chain_,'/pool/', address, '"target ="_blank">', 'balancer')
{% endmacro %}
