{% macro get_chain_explorer_address() %}
   create or replace function get_chain_explorer_address(chain_ STRING, column_)
   returns STRING
   return
   SELECT
      case 
         when 'ethereum' = chain_ then 'https://etherscan.io/address/' || CAST(column_ AS VARCHAR)
         when 'optimism' = chain_ then 'https://explorer.optimism.io/address/' || CAST(column_ AS VARCHAR)
         when 'polygon' = chain_ then 'https://polygonscan.com/address/' || CAST(column_ AS VARCHAR)
         when 'arbitrum' = chain_ then 'https://arbiscan.io/address/' || CAST(column_ AS VARCHAR)
         when 'avalanche_c' = chain_ then 'https://snowtrace.io/address/' || CAST(column_ AS VARCHAR)
         when 'gnosis' = chain_ then 'https://gnosisscan.io/address/' || CAST(column_ AS VARCHAR)
         when 'bnb' = chain_ then 'https://bscscan.com/address/' || CAST(column_ AS VARCHAR)
         when 'solana' = chain_ then 'https://solscan.io/address/' || CAST(column_ AS VARCHAR)
         when 'fantom' = chain_ then 'https://ftmscan.com/address/' || CAST(column_ AS VARCHAR)
         when 'celo' = chain_ then 'https://celoscan.io/address/' || CAST(column_ AS VARCHAR)
         when 'base' = chain_ then 'https://basescan.org/address/' || CAST(column_ AS VARCHAR)
         when 'bitcoin' = chain_ then 'https://blockstream.info/address/' || CAST(column_ AS VARCHAR)
         when 'goerli' = chain_ then 'https://goerli.basescan.org/address/' || CAST(column_ AS VARCHAR)
         when 'zksync' = chain_ then 'https://explorer.zksync.io' || CAST(column_ AS VARCHAR)
         else 'https://etherscan.io/address/' || CAST(column_ AS VARCHAR)
      end as explorer_address_url;
{% endmacro %}
