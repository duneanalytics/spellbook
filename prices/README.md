# Prices

Dune tracks USD prices from coinpaprika. If you want to add an asset to `prices.usd` or `prices.layer1_usd`
follow these steps and open a PR:
1. Find the asset on coinpaprika. Note the symbol
2. Note the ID of the asset from coinpaprikas URL. In https://coinpaprika.com/coin/dmg-dmm-governance/ `dmg-dmm-governance` is the ID. 
3. If the asset is a token on Ethereum, find the contract address and number of decimals e.g. through etherscan.

The last step is to add an entry to `prices.ini` with the following format (Without comments)
```ini
[assets.id_but_with_underscores]
id = dmg-dmm-governance     # the id from above
symbol = DMG                # the asset ticker
address = 0xdeadbeef        # the contract address
decimals = 18               # the number of decimals the token contract uses
```

When the PR is merged we will deploy the changes and you will soon see USD prices for your asset.