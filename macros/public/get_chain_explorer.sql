{% macro get_chain_explorer() %}
   create or replace function get_chain_explorer(chain_ STRING)
   returns STRING
   return
   SELECT
      case 
         when 'ethereum' = chain_ then 'https://etherscan.io'
         when 'optimism' = chain_ then 'https://explorer.optimism.io'
         when 'polygon' = chain_ then 'https://polygonscan.com'
         when 'arbitrum' = chain_ then 'https://arbiscan.io'
         when 'avalanche_c' = chain_ then 'https://snowtrace.io'
         when 'gnosis' = chain_ then 'https://gnosisscan.io'
         when 'bnb' = chain_ then 'https://bscscan.com'
         when 'solana' = chain_ then 'https://solscan.io'
         when 'fantom' = chain_ then 'https://ftmscan.com'
         when 'base' = chain_ then 'https://basescan.org'
         when 'bitcoin' = chain_ then 'https://blockstream.info'
         when 'celo' = chain_ then 'https://celoscan.io'
         when 'goerli' = chain_ then 'https://goerli.basescan.org'
         when 'zksync' = chain_ then 'https://explorer.zksync.io'
         else 'https://etherscan.io'
      end as explorer_url;
{% endmacro %}
