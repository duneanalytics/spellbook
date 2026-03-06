# .cursor/

AI-assisted development setup for spellbook.

## Structure

```
.cursor/
└── skills/     # agent skills – invoke via / in Agent
    ├── catalyst-dex-integration
    ├── catalyst-foundational-metadata
    ├── catalyst-gas-and-transfers
    └── sql-style-guide
```

## Skills (invoke via / in Agent)

| Skill | Description |
|-------|-------------|
| `/catalyst-foundational-metadata` | EVM info, prices, sources for a new chain |
| `/catalyst-gas-and-transfers` | Gas fees, token transfers for a new chain |
| `/catalyst-dex-integration` | DEX trades integration (chain, project, namespace). Self-contained (conventions, prep vars, git workflow, final checks inlined). |
| `/sql-style-guide` | SQL formatting conventions |

## Dune MCP

Dune queries run via the **Dune MCP**:

- **query_sql** – execute raw SQL
- **run_query_by_id** – run a saved Dune query by ID with parameters (e.g. `query_parameters: '{"chain":"<chain>"}'`)

### AMP metadata support

Use Dune MCP **run_query_by_id** with `query_id: 6637901`, `query_parameters: '{"chain":"<chain>","sim_api_key":"<key>"}'`. When used in this repo, the agent should read `SIM_METADATA_API_KEY` from `.env` for the `sim_api_key` value (or run the saved query in Dune with parameters).

## Setup

Required env vars in `.env` at project root (used by the Dune MCP server; query 6637901 uses `sim_api_key` from `SIM_METADATA_API_KEY`):

```
DUNE_API_KEY=your_api_key
SIM_METADATA_API_KEY=your_sim_key
```
