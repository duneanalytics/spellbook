-- NB: this is a generated query, do not edit it directly, instead edit the template and re-generate the query
-- hydrate the generated ouput here: https://dune.com/queries/4407620
{% set blockchain_var = "{{blockchain}}" %}
{% set contract_address_var = "{{contract_address}}" %}
{{ paraswap_all_entities() }}
select 
    entity, blockchain, contract_address, block_time, tx_hash    
from entities
where blockchain = '{{blockchain_var}}' and contract_address = {{contract_address_var}}
order by block_time, tx_hash
