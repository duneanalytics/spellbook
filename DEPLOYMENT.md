# Steps to deploy code to production
These steps are for Dune employees. If you are a wizard, reach out to a Data Experience team member. 

After you have merged your code into master. 

| **git checkout master** |
| --- |

Double check you have all the latest changes

| git pull --rebase |
| --- |

Create an annotated [tag](https://git-scm.com/book/en/v2/Git-Basics-Tagging) using [semantic versioning](https://semver.org/). See the [Releases](https://github.com/duneanalytics/abstractions/tags) page to check the previous version.

Given a version number MAJOR.MINOR.PATCH, increment the:

1. MAJOR version when you make incompatible changes,
2. MINOR version when you add functionality in a backwards compatible manner, and
3. PATCH version when you make backwards compatible bug fixes.

| git tag -a vX-X-X -m &quot;some notes on the changes&quot; |
| --- |

Push your tag to github

| git push origin vX-X-X |
| --- |

Navigate to the [Releases](https://github.com/duneanalytics/abstractions/tags) page for the repo. Click create a release.


From the release creation page, use the drop down menu to select your tag. Write some notes to describe the changes since last release.


Update the branch name on the [production environment](https://cloud.getdbt.com/#/accounts/58579/projects/95826/environments/83086/settings/) on DBT CLoud.

# Rollback
Check the Releases page and deploy the previous tag to DBT Cloud. 
