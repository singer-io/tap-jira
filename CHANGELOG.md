# Changelog

## [v2.5.0]
  * Add extra tags to `http_request_timer` metrics and `record_counter` metrics [#129](https://github.com/singer-io/tap-jira/pull/129)

## [v2.4.0]
  * Adds `forced_replication_method` and `parent_tap_stream_id` as discoverable metadata [#120](https://github.com/singer-io/tap-jira/pull/120)
## [v2.3.0]
  * Change endpoint for `issues` Stream [#122](https://github.com/singer-io/tap-jira/pull/122)
## [v2.2.1]
  * Bump depedency versions for twistlock compliance [#118](https://github.com/singer-io/tap-jira/pull/118)

## [v2.2.0]
  * Updates to run on python 3.11.7 [#111](https://github.com/singer-io/tap-jira/pull/111)
## [v2.1.5]
  * Skipped the record for out of range date values [#87](https://github.com/singer-io/tap-jira/pull/87)
## [v2.1.4]
  * Updated README [#88](https://github.com/singer-io/tap-jira/pull/88)
## [v2.1.3]
  * Fixed Tranform Issues [#84](https://github.com/singer-io/tap-jira/pull/84)
## [v2.1.2]
  * Added Missing Test Cases [#74](https://github.com/singer-io/tap-jira/pull/74)
  * Updated Project stream with new endpoint as old one is deprecated [#75](https://github.com/singer-io/tap-jira/pull/75)
  * Primary Key Switching change for On prem JIRA [#76](https://github.com/singer-io/tap-jira/pull/76)
  * Updating primary key for issue_transition_stream [#81](https://github.com/singer-io/tap-jira/pull/81)
## [v2.1.1]
  * Request Timeout Implementation [#71](https://github.com/singer-io/tap-jira/pull/71)
## [v2.1.0]
  * Prevent connections that would yield a 401 from becoming fully_configured [#66] (https://github.com/singer-io/tap-jira/pull/66)

  * Updated error handling of HTTP error and added Retry for error code 503 [#64] (https://github.com/singer-io/tap-jira/pull/64)

  * Upgraded singer python dependency,upgraded high-level exception handling [#65] (https://github.com/singer-io/tap-jira/pull/65)

[Full Changelog](https://github.com/singer-io/tap-jira/compare/v2.0.0...v2.0.1)

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
