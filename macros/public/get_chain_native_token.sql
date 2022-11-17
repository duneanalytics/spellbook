{% macro get_chain_native_token() %}
      create or replace function get_chain_native_token(chain_ STRING, column_ STRING)
      returns STRING
      return
      SELECT
         case
            when 'symbol' = column_ then first(symbol)
            when 'price_symbol' = column_ then first(price_symbol)
            when 'price_address' = column_ then first(price_address)
            end as result
      FROM tokens.native
      WHERE chain = chain_;
{% endmacro %}
