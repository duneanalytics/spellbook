{% macro get_chain_native_token(chain, column) %}
   {% set query %}
      SELECT
         {{ column }} --should be either symbol, prices_symbol, prices_address
      FROM (VALUES
      ('ethereum', 'ETH', 'WETH', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', timestamp('2022-11-07'), now())
      , ('optimism', 'ETH', 'WETH', '0x4200000000000000000000000000000000000006', timestamp('2022-11-07'), now())
      , ('polygon', 'MATIC', 'MATIC', '0x0000000000000000000000000000000000001010', timestamp('2022-11-07'), now())
      , ('arbitrum', 'ETH', 'WETH', '0x82af49447d8a07e3bd95bd0d56f35241523fbab1', timestamp('2022-11-07'), now())
      , ('avalanche_c', 'AVAX', 'WAVAX', '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7', timestamp('2022-11-07'), now())
      , ('gnosis', 'xDAI', 'WXDAI', '0xe91d153e0b41518a2ce8dd3d7944fa863463a97d', timestamp('2022-11-07'), now())
      , ('bnb', 'BNB', 'WBNB', '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c', timestamp('2022-11-07'), now())
      , ('solana', 'SOL', 'SOL', 'so11111111111111111111111111111111111111112', timestamp('2022-11-07'), now())
      ) AS x (chain, symbol, prices_symbol, prices_address, created_at, updated_at)
      WHERE chain = '{{ chain }}' 
   {% endset %}

   {% set runner = run_query(query) %}

   {% if execute %} --required to await for results  
      {% set results = runner.rows[0][0] %} --get first row and then first element
   {% endif %}

   '{{ results }}' --return with quotes
{% endmacro %}
