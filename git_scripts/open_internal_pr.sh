#!/bin/bash
### DEPRECATED IN FAVOR OF GITHUB ACTION ON SELF HOSTED RUNNER
### For Dune employees.
### This script fetches a branch from a forked repo, opens a new internal branch, and opens a pull request
### Example usage:
### To open a pull request from 0xbitfly from their master branch
### run the following command from your terminal in the abstractions repo
### sh open_internal_pr.sh 0xbitfly master

### If you run into an error that looks like:
### fatal: The upstream branch of your current branch does not match
### the name of your current branch.  To push to the upstream branch
### on the remote, use...
### Run `git branch --unset-upstream` from your terminal

### Print total arguments and their values
echo "Total Arguments:" $#
echo "All Arguments values:" $@

### Command arguments can be accessed as
echo "Github User->"  $1
echo "Branch name->" $2

### Add remote. If already added, expect an error "error: remote $1 already exists."
git remote add $1 https://github.com/$1/abstractions
### Fetch remote
git fetch $1
### Checkout developers remote branch and open a new branch with their username and todays date
git checkout -b $1_$(date '+%Y-%m-%d') $1/$2
### Push the new branch
git push --set-upstream origin $1_$(date '+%Y-%m-%d')
### Open pull request
open https://github.com/duneanalytics/abstractions/pull/new/$1_$(date '+%Y-%m-%d')
