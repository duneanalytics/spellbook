{% macro get_chain_explorer_tx_hash() %}
   create or replace function get_chain_explorer_tx_hash(hash_ STRING, chain_ STRING, link_text STRING)
   returns STRING
   return
   SELECT
      case 
         when 'ethereum' = chain_ then concat('<a href="','https://etherscan.io/tx/', hash_ '"target ="_blank">', link_text)
         when 'optimism' = chain_ then concat('<a href="','https://optimistic.etherscan.io/tx/', hash_ '"target ="_blank">', link_text)
         when 'polygon' = chain_ then concat('<a href="','https://polygonscan.com/tx/', hash_ '"target ="_blank">', link_text)
         when 'arbitrum' = chain_ then concat('<a href="','https://arbiscan.io/tx/', hash_ '"target ="_blank">', link_text)
         when 'avalanche_c' = chain_ then concat('<a href="','https://snowtrace.io/tx/', hash_ '"target ="_blank">', link_text)
         when 'gnosis' = chain_ then concat('<a href="','https://gnosisscan.io/tx/', hash_ '"target ="_blank">', link_text)
         when 'bnb' = chain_ then concat('<a href="','https://bscscan.com/tx/', hash_ '"target ="_blank">', link_text)
         when 'solana' = chain_ then concat('<a href="','https://solscan.io/tx/', hash_ '"target ="_blank">', link_text)
         when 'fantom' = chain_ then concat('<a href="','https://ftmscan.com/tx/', hash_ '"target ="_blank">', link_text)
         when 'celo' = chain_ then concat('<a href="','https://celoscan.io/tx/', hash_ '"target ="_blank">', link_text)
         when 'base' = chain_ then concat('<a href="','https://basescan.org/tx/', hash_ '"target ="_blank">', link_text)
         when 'bitcoin' = chain_ then concat('<a href="','https://blockstream.info/tx/', hash_ '"target ="_blank">', link_text)
         when 'goerli' = chain_ then concat('<a href="','https://goerli.basescan.org/tx/', hash_ '"target ="_blank">', link_text)
         else concat('<a href="','https://etherscan.io/tx/', hash_ '"target ="_blank">', link_text)
      end as explorer_tx_hash_url;
{% endmacro %}
