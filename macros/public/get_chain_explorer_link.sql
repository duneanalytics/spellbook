{% macro get_chain_explorer_link() %}
   create or replace function get_chain_explorer_link(chain_ STRING, column_ STRING)
   returns STRING
   return
   SELECT
      case 
         when 'ethereum' = chain_ then concat('<a href="','https://etherscan.io/address/', column, '"target ="_blank">', 'etherscan')
         when 'optimism' = chain_ then concat('<a href="','https://optimistic.etherscan.io/address/', column, '"target ="_blank">', 'opetherscan')
         when 'polygon' = chain_ then concat('<a href="','https://polygonscan.com/address/', column, '"target ="_blank">', 'polygonscan')
         when 'arbitrum' = chain_ then concat('<a href="','https://arbiscan.io/address/', column, '"target ="_blank">', 'arbiscan')
         when 'avalanche_c' = chain_ then concat('<a href="','https://snowtrace.io/address/', column, '"target ="_blank">', 'snowtrace')
         when 'gnosis' = chain_ then concat('<a href="','https://gnosisscan.io/address/', column, '"target ="_blank">', 'gnosisscan')
         when 'bnb' = chain_ then concat('<a href="','https://bscscan.com/address/', column, '"target ="_blank">', 'bscscan')
         when 'solana' = chain_ then concat('<a href="','https://solscan.io/address/', column, '"target ="_blank">', 'solscan')
         when 'fantom' = chain_ then concat('<a href="','https://ftmscan.com/address/', column, '"target ="_blank">', 'ftmscan')
         when 'celo' = chain_ then concat('<a href="','https://celoscan.io/address/', column, '"target ="_blank">', 'celoscan')
         when 'base' = chain_ then concat('<a href="','https://basescan.org/address/', column, '"target ="_blank">', 'basescan')
         when 'bitcoin' = chain_ then concat('<a href="','https://blockstream.info/address/', column, '"target ="_blank">', 'blockstream')
         when 'goerli' = chain_ then concat('<a href="','https://goerli.basescan.org/address/', column, '"target ="_blank">', 'goerlibasescan')
         else concat('<a href="','https://etherscan.io/address/', column, '"target ="_blank">', 'etherscan')
      end as explorer_address_link;
{% endmacro %}
