# Prices

Dune tracks USD prices from coinpaprika and coingecko. If you want to add an asset to `prices.usd` or `prices.layer1_usd`
follow these steps and open a PR:

## Ethereum

1. Find the asset on coinpaprika. Note the symbol
2. Note the ID of the asset from coinpaprikas URL. In https://coinpaprika.com/coin/dmg-dmm-governance/ `dmg-dmm-governance` is the ID.
3. If the asset is a token on Ethereum, find the contract address and number of decimals e.g. through etherscan.
4. The last step is to add an entry to `prices/ethereum/coinpaprika.yaml` with the following format (Without comments)

```yaml
- name: dmg_dmm_governance
  id: dmg-dmm-governance    # the id from above
  symbol: DMG 				      # the asset ticker
  address: 0xdeadbeef       # the contract address
  decimals: 18              # the number of decimals the token contract uses
```

When the PR is merged we will deploy the changes and you will soon see USD prices for your asset.

## Polygon

1. Find the asset on coingecko.
2. Go the page for the given coin. Under the `Info` section there should be a field called `API id`. This it the `id`
3. The last step is to add an entry to `prices/polygon/coingecko.yaml` with the following format (Without comments)

```yaml
- id: dmg-dmm-governance    # the id from above
```
**NOTE**: For coingecko only the id is required. All other fields will be generated when the feed begins.

## Binance Smart Chain

1. Find the asset on coinpaprika. Note the symbol
2. Note the ID of the asset from coinpaprikas URL. In https://coinpaprika.com/coin/dmg-dmm-governance/ `dmg-dmm-governance` is the ID.
3. If the asset is a token on Binance Smart Chain, find the contract address and number of decimals e.g. through https://bscscan.com/.
4. The last step is to add an entry to `prices/bsc/coinpaprika.yaml` with the following format (Without comments)

```yaml
- name: dmg_dmm_governance
  id: dmg-dmm-governance    # the id from above
  symbol: DMG 				      # the asset ticker
  address: 0xdeadbeef       # the contract address
  decimals: 18              # the number of decimals the token contract uses
```
