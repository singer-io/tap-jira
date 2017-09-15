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
  - [`worklogs`](https://docs.atlassian.com/jira/REST/cloud/#api/2/worklog-getWorklogsForIds)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Test Data

This repo provides a script for creating some data in JIRA. Use `./create.py
--help` for details. Examples:

```
./create.py --config config.json project
./create.py --config config.json --num-issues 10 issue
```

---

Copyright &copy; 2017 Stitch
