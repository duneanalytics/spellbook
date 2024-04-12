{% macro get_chain_explorer_tx_hash() %}
   create or replace function get_chain_explorer_tx_hash(chain_ STRING, hash_ STRING)
   returns STRING
   return
   SELECT
      case 
         when 'ethereum' = chain_ then 'https://etherscan.io/tx/' || CAST(hash_ AS VARCHAR)
         when 'optimism' = chain_ then 'https://explorer.optimism.io/tx/' || CAST(hash_ AS VARCHAR)
         when 'polygon' = chain_ then 'https://polygonscan.com/tx/' || CAST(hash_ AS VARCHAR)
         when 'arbitrum' = chain_ then 'https://arbiscan.io/tx/' || CAST(hash_ AS VARCHAR)
         when 'avalanche_c' = chain_ then 'https://snowtrace.io/tx/' || CAST(hash_ AS VARCHAR)
         when 'gnosis' = chain_ then 'https://gnosisscan.io/tx/' || CAST(hash_ AS VARCHAR)
         when 'bnb' = chain_ then 'https://bscscan.com/tx/' || CAST(hash_ AS VARCHAR)
         when 'solana' = chain_ then 'https://solscan.io/tx/' || CAST(hash_ AS VARCHAR)
         when 'fantom' = chain_ then 'https://ftmscan.com/tx/' || CAST(hash_ AS VARCHAR)
         when 'celo' = chain_ then 'https://celoscan.io/tx/' || CAST(hash_ AS VARCHAR)
         when 'base' = chain_ then 'https://basescan.org/tx/' || CAST(hash_ AS VARCHAR)
         when 'bitcoin' = chain_ then 'https://blockstream.info/tx/' || CAST(hash_ AS VARCHAR)
         when 'goerli' = chain_ then 'https://goerli.basescan.org/tx/' || CAST(hash_ AS VARCHAR)
         when 'zksync' = chain_ then 'https://explorer.zksync.io' || CAST(hash_ AS VARCHAR)
         else 'https://etherscan.io/tx/' || CAST(hash_ AS VARCHAR)
      end as explorer_tx_hash_url;
{% endmacro %}
