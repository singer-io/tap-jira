# Changelog

## [v2.0.1](https://github.com/singer-io/tap-jira/tree/v2.0.1) (2021-04-06)

[Full Changelog](https://github.com/singer-io/tap-jira/compare/v2.0.0...v2.0.1)

**Merged pull requests:**

- Adding new components stream and updating / improving circle [\#59](https://github.com/singer-io/tap-jira/pull/59) ([asaf-erlich](https://github.com/asaf-erlich))
- add slack orb [\#58](https://github.com/singer-io/tap-jira/pull/58) ([kspeer825](https://github.com/kspeer825))
- limit setuptools to 51.0.0 [\#56](https://github.com/singer-io/tap-jira/pull/56) ([kspeer825](https://github.com/kspeer825))
- add context user [\#55](https://github.com/singer-io/tap-jira/pull/55) ([kspeer825](https://github.com/kspeer825))
- Document that user\_agent is required in all modes [\#53](https://github.com/singer-io/tap-jira/pull/53) ([rpaterson](https://github.com/rpaterson))

## 2.0.0
  * Change primary key of `users` stream to from `key` to `accountId` [#46](https://github.com/singer-io/tap-jira/pull/46)

## 1.0.8
  * Add additional error types to retry logic and ensure correct functions get retried [#44](https://github.com/singer-io/tap-jira/pull/44)

## 1.0.7
  * Update `users` query to include inactive users [#42](https://github.com/singer-io/tap-jira/pull/42)

## 1.0.6
  * Update `users` schema to match the results of `/group/member` endpoint [#41](https://github.com/singer-io/tap-jira/pull/41)

## 1.0.3
  * Refactoring

## 1.0.2
  * Refactor the base class `Stream` to remove the child class `Everything`

## 1.0.1
  * Add support for Basic Auth and Oauth

## 0.3.3
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 0.3.2
  * Changes `search` endpoint for issues to use the user's timezone [#15](https://github.com/singer-io/tap-jira/pull/15)

## 0.2.3
  * Added `jsdPublic` field to `ticket_comments` schema.

## 0.2.2
  * Fixes a pagination bug that would cause Issue and Versions to return more pages than necessary [#14](https://github.com/singer-io/tap-jira/pull/14)
