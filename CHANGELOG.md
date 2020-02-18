# Changelog

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
