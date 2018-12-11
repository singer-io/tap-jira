# tap-jira

This is a spike that investigates the structure of the objects returned by
the `issues` and `fields` endpoints.

## Quick Start

Note: This spike assumes you have just run `tap-jira` recently because it relies on having the `properties.json`, `tap_config.json`, `catalog.json`, and `tap_state.json` files from `/tmp`.

In `./tap_jira` there is a `Makefile` with two targets, `run` and `setup`:
* `make setup` should be run first
  * This just grabs the above mentioned files from `/tmp` and places them into `./tap_jira/tap_files`
* `make run` currently runs the only spike available, `looking_at_issues_and_fields.py`

---

Copyright &copy; 2017 Stitch
