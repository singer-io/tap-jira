# tap-jira

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [JIRA Cloud REST
  API](https://docs.atlassian.com/jira/REST/cloud/#api/2/)
- Extracts the following resources:
  - [`projects`](https://docs.atlassian.com/jira/REST/cloud/#api/2/project-getAllProjects)
  - [`versions`](https://docs.atlassian.com/jira/REST/cloud/#api/2/project-getProjectVersionsPaginated)
  - [`project_types`](https://docs.atlassian.com/jira/REST/cloud/#api/2/project/type-getAllProjectTypes)
  - [`project_categories`](https://docs.atlassian.com/jira/REST/cloud/#api/2/projectCategory-getAllProjectCategories)
  - [`resolutions`](https://docs.atlassian.com/jira/REST/cloud/#api/2/resolution-getResolutions)
  - [`roles`](https://docs.atlassian.com/jira/REST/cloud/#api/2/role-getProjectRoles)
  - [`users`](https://docs.atlassian.com/jira/REST/cloud/#api/2/user-findUsers)
  - [`issues`](https://docs.atlassian.com/jira/REST/cloud/#api/2/search-search)
  - [`issue_comments`](https://docs.atlassian.com/jira/REST/cloud/#api/2/search-search)
  - [`issue_transitions`](https://docs.atlassian.com/jira/REST/cloud/#api/2/search-search)  
  - [`worklogs`](https://docs.atlassian.com/jira/REST/cloud/#api/2/worklog-getWorklogsForIds)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick Start

1. Install

    pip install tap-jira

2. Create the config file

   Create a JSON file called `config.json`. Its contents should look like:

   ```json
    {
        "start_date": "2010-01-01",
        "username": "your-jira-username",
        "password": "your-jira-password",
        "base_url": "https://your-jira-domain"
    }
    ```

   The `start_date` specifies the date at which the tap will begin pulling data
   (for those resources that support this).

   The `base_url` is the URL where your Jira installation can be found. For
   example, it might look like: `https://mycompany.atlassian.net`.

4. Run the Tap in Discovery Mode

    tap-jira -c config.json -d

   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/BEST_PRACTICES.md#discover-mode-and-connection-checks).

5. Run the Tap in Sync Mode

    tap-jira -c config.json -p catalog-file.json

## Test Data

This repo provides a script for creating some data in JIRA. Use `./create.py
--help` for details. Examples:

```
./create.py --config config.json project
./create.py --config config.json --num-issues 10 issue
```

---

Copyright &copy; 2017 Stitch
