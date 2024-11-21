# Issues

There are a few main use cases for opening GH issues in spellbook:

1. **Report a bug** found on an existing spell.
2. **Suggest enhancements** on existing spells.
3. **Share an idea for a new spell** & have upfront discussions on the design prior to developing.
    - **Note**: This is especially important for new sector spells, that contain multiple projects across multiple blockchains.

Once an issue is opened, the Dune team will respond as soon as possible and help resolve or find the correct people who can help.

# Pull Requests

If the new spell(s) are still in development on the user's feature branch, it is recommended to open PR as a draft. When the code is ready for review, the PR can be taken out of draft mode and the Dune team will help as soon as possible.

# Discussions

Longer-form conversations about spell(s) can be initiated here. Discussions will also be used for announcements at times, such as examples like [this](https://github.com/duneanalytics/spellbook/discussions/4662) throughout the DuneSQL migration.

# Actions

There are two main use cases for users in the GH actions section:

1. **Monitor continuous integration (CI) tests** run on each PR.
    - It’s easiest to access directly from PR itself.
2. **Reference the ‘Commit Manifest’ workflow status.**
    - When the Dune team merges code into main branch, this workflow kicks off.
    - If in progress and/or failed on the most recent run, users may notice spells outside of their PR running within their own CI test – this is due to the ‘Commit Manifest’ job needing to succeed and load new DBT manifest file for CI test state comparisons.
    - Please be patient while the Dune team resolves if this job fails, but feel free to tag Dune team if not resolved in a timely manner.