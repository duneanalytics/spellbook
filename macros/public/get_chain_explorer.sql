{% macro get_chain_explorer(chain) %}
   {% set query %}
      SELECT
         explorer_url
      FROM (VALUES
      ('ethereum', 'https://etherscan.io', timestamp('2022-11-07'), now())
      , ('optimism', 'https://optimistic.etherscan.io', timestamp('2022-11-07'), now())
      , ('polygon', 'https://polygonscan.com', timestamp('2022-11-07'), now())
      , ('arbitrum', 'https://arbiscan.io', timestamp('2022-11-07'), now())
      , ('avalanche_c', 'https://avascan.io', timestamp('2022-11-07'), now())
      , ('gnosis', 'https://gnosisscan.io', timestamp('2022-11-07'), now())
      , ('bnb', 'https://bscscan.com', timestamp('2022-11-07'), now())
      , ('solana', 'https://solscan.io', timestamp('2022-11-07'), now())
      ) AS x (chain, explorer_url, created_at, updated_at)
      WHERE chain = '{{chain}}'
   {% endset %}

   {% set runner = run_query(query) %}

   {% if execute %} --required to await for results  
      {% set results = runner.rows[0][0] %} --get first row and then first element
   {% endif %}

   '{{ results }}' --return with quotes
   
{% endmacro %}
