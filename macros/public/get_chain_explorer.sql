{% macro get_chain_explorer() %}
   create or replace function get_chain_explorer(chain_ STRING)
   returns STRING
   return
   SELECT
      case 
         when 'ethereum' = chain_ then 'https://etherscan.io'
         when 'optimism' = chain_ then 'https://optimistic.etherscan.io'
         when 'polygon' = chain_ then 'https://polygonscan.com'
         when 'arbitrum' = chain_ then 'https://arbiscan.io'
         when 'avalanche_c' = chain_ then 'https://snowtrace.io'
         when 'gnosis' = chain_ then 'https://gnosisscan.io'
         when 'bnb' = chain_ then 'https://bscscan.com'
         when 'solana' = chain_ then 'https://solscan.io'
         else 'https://etherscan.io'
      end as explorer_url;
{% endmacro %}
