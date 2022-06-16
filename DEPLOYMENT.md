# Steps to deploy abstractions to production
These steps are for Dune employees. If you are a wizard, reach out to a Data Experience team member. 

After you have merged your code into master. (Soon to be updated to `main`)

`git checkout master`

Double check you have all the latest changes

`git pull --rebase`

Create an annotated [tag](https://git-scm.com/book/en/v2/Git-Basics-Tagging) using [semantic versioning](https://semver.org/). See the [Releases](https://github.com/duneanalytics/abstractions/tags) page to check the previous version.

Given a version number MAJOR.MINOR.PATCH, increment the:

1. MAJOR version when you make incompatible changes,
2. MINOR version when you add functionality in a backwards compatible manner, and
3. PATCH version when you make backwards compatible bug fixes.

`git tag -a vX-X-X -m "some notes on the changes"`

Push your tag to github

`git push origin vX-X-X`

Navigate to the [Releases](https://github.com/duneanalytics/abstractions/tags) page for the repo. Click create a release.

<img width="1416" alt="Screen Shot 2022-05-27 at 2 44 14 PM" src="https://user-images.githubusercontent.com/9472574/171188506-4abfcd4c-6a0d-4d89-ae53-9a1d27befdee.png">


From the release creation page, use the drop down menu to select your tag. Write some notes to describe the changes since last release.

<img width="1411" alt="Screen Shot 2022-05-27 at 2 44 25 PM" src="https://user-images.githubusercontent.com/9472574/171188577-244cd752-926d-44c3-a9b0-c4541504bc0f.png">



Update the branch name on the [production environment](https://cloud.getdbt.com/#/accounts/58579/projects/95826/environments/83086/settings/) on DBT Cloud.

<img width="1411" alt="Screen Shot 2022-05-31 at 9 46 24 AM" src="https://user-images.githubusercontent.com/9472574/171188799-20649e07-aed9-4d0a-b840-50b5a8bf7e78.png">

To deploy the new version, run the "Manually Deploy New Version" job in the Production environment in dbt Cloud. This will ensure models and tests are updated.

# Rollback
Check the Releases page and deploy the previous tag to DBT Cloud. 
