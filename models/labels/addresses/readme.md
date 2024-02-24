# Welcome to Labels!

The models here allow you to add static or query labels to any address. The idea here is to be able to identify key addresses like CEX or Fund wallets, or identify the top NFT and DEX traders. Labels should ideally be built upon existing spellbook tables like `dex.trades` and `nft.trades` to ensure data quality. Feel free to build labels out of other labels too!

The most important thing to learn are the label types below. If you have a label type that you think does not belong in one of these three, then we'll handle it on a case by case basis.

**There are currently 8 high-level categories of labels**, and we are always open to adding more. If the model is likely to be the only model of it's category, it will go in the `__single_category_labels__` folder until we find something to expand on or attach it to. Those models have the exact same schemas as others, but are put into their own folder so that high-level categories are easily found and navigated.

## Contributing

Anyone can contribute, if you have an idea you would just like to discuss first then go ahead and start a [new discussion](https://github.com/duneanalytics/spellbook/discussions/categories/labels-discussion) in the labels section.

1. Add a label sql and schema file under the intended category/label type folder
2. Add the new label model to the top-level model in the category folder (i.e. `labels_dex.sql`)
3. Create a new PR with your changed/added models.

If you want to update/improve upon an existing label, start a discussion as well and tag the contributors (their names can be found in the schema file, which usually map closely to their github names). `ilemi` is `andrewhong5297`, I should be your default tag for questions.

The schema of all models should follow this format:

![alt text](/models/labels/addresses/labels_schema.PNG)

You can add extra columns to the base label if it is helpful, but then make sure you select only the columns from the schema above when adding it to the top-level model of the category.

## Label Types:

Here’s a list of label types:

- **Identifiers**: Most static labels should be this label type, as well as common usernames such as Farcaster, ENS, and Lens names. As a rule of thumb, identifiers should usually specify a unique entity name.
- **Usage:** These are the existing top volume and frequency (or some other percentile-able metric) within a domain and the usage of specific protocols. There must be some sort of ranking/percentile involved!
- **Personas:** These are for on-chain curated behaviors (like common CT memes) or protocol user tagging. They should be easily understood to non-analysts, though the underlying calculation methods may be more subjective.

This is just the starting list. If a label is created that clearly fits inside a new type, we will create a type for it. The idea is to try and keep types smaller in number and very clear so that it's easier for analysts to navigate, use, and contribute to.

### Example Hierarchies:

To make this clear, let’s do some examples (some of these don't exist yet in the repo).

**Category: NFT**

- Label_types
  - Identifier: Opensea Username (traderName_opensea)
  - Usage: top volume by nft.trades
  - Usage: top transactions by nft.trades
  - Persona: Opensea User, Sudoswap User, Blur User
  - Persona: Wash Trader
  - Persona: Art Blocks Curator (holds all art blocks curated collections)
  - Persona: Airdrop Hunter
  - Persona: Early NFT Trader (first year of nft trading)

**Category: Social**

- Label_types
  - Identifier: Lens username (.lens) ENS reverse resolver (.eth), farcaster (\_farcaster)
  - Usage: top holders from ENS
  - Usage: top posters from lens
  - Persona: Lens User, ENS User
  - Persona: Squatter (sitting on dozens of ENS names)

**Category: DEX**

- Label_types
  - Usage: top volume by dex.trades
  - Usage: top transactions by dex.trades
  - Persona: Uniswap v2 User, Cowswap User
  - Persona: Sandwiched User
  - Persona: Yield Farmer
  - Persona: Liquidity Provider
