# .cursor/

AI-assisted development setup for spellbook.

## Structure

```
.cursor/
├── commands/
│   └── catalyst/           # chain onboarding commands
│       ├── _shared.md      # common steps (git, prep vars)
│       ├── foundational-metadata.md
│       ├── gas-and-transfers.md
│       └── dex-integration.md
├── rules/
│   └── catalyst.md         # auto-loaded conventions
└── scripts/
    ├── dune_query.py       # query Dune API
    └── check_amp_support.py # check chain AMP support
```

## Commands

| Command | Description |
|---------|-------------|
| `/catalyst/foundational-metadata <issue_id> <chain>` | EVM info, prices, sources |
| `/catalyst/gas-and-transfers <issue_id> <chain>` | Gas fees, token transfers |
| `/catalyst/dex-integration <issue_id> <chain> <project> <namespace>` | DEX trades integration |

## Setup

Required env vars in `.env`:
- `DUNE_API_KEY`
- `SIM_METADATA_API_KEY`
