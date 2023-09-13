{% macro get_balancer_link() %}
      create or replace function get_balancer_link(chain_ STRING, pool_id STRING)
      returns STRING 
      return 
      SELECT
      CASE  
            WHEN 'avalanche_c' = chain_ THEN concat('<a href="','https://app.balancer.fi/#/avalanche/pool/', pool_id, '"target ="_blank">', 'balancer')
            WHEN 'gnosis' = chain_ THEN concat('<a href="','https://app.balancer.fi/#/gnosis-chain/pool/', pool_id, '"target ="_blank">', 'balancer')
            ELSE concat('<a href="','https://app.balancer.fi/#/',chain_,'/pool/', pool_id, '"target ="_blank">', 'balancer')
      END as balancer_link
{% endmacro %}
