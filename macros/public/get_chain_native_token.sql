 {% macro get_chain_native_token(chain, column) %}


    {% set native_tokens_dict = ({'ethereum': {'symbol': 'ETH', 'prices_symbol': 'WETH'},
                                 'polygon': {'symbol': 'MATIC', 'prices_symbol': 'MATIC'}}) %}

    {% set result = native_tokens_dict.get(chain, {}).get(column) %}
g

    '{{ result }}'
{% endmacro %}
