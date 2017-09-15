#!/usr/bin/env python
import requests
from requests.models import Response
from tap_jira.http import Client
import uuid
import json
import random
import argparse


def _uuid():
    return uuid.uuid4().hex


class WriteableClient(Client):
    headers = {"Content-Type": "application/json"}
    def _write(self, method, *args, data=None, **kwargs):
        data = json.dumps(data)
        return self.send(method, *args, headers=self.headers, data=data, **kwargs)
    def post(self, *args, **kwargs):
        return self._write("POST", *args, **kwargs)
    def put(self, *args, **kwargs):
        return self._write("PUT", *args, **kwargs)


def project(client):
    key = ("GEN" + _uuid().replace("-", "").upper())[:10]
    return client.post(
        "/rest/api/2/project",
        data={
            "key": key,
            "name": "tap-jira generated " + key,
            "projectTypeKey": "software",
            "description": "Generated description for " + key,
            "lead": "admin",
        }
    )


def random_project(client):
    return random.choice(client.send("GET", "/rest/api/2/project").json())


def version(client):
    project = random_project(client)
    key = project["key"]
    versions = client.send("GET", "/rest/api/2/project/" + key + "/versions").json()
    maxv = max(v["name"] for v in versions) if versions else "v0"
    version = "v" + str(int(maxv[1:]) + 1)
    return client.post(
        "/rest/api/2/version",
        data={
            "project": key,
            "name": version,
            "released": True,
        }
    )


def project_category(client):
    name = "Generated " + _uuid()
    return client.post(
        "/rest/api/2/projectCategory",
        data={
            "name": name,
            "description": "Description for " + name,
        }
    )


def issue(client, args):
    project = random_project(client)
    issues = []
    for _ in range(args.num_issues):
        issues.append({
            "fields": {
                "project": {"id": project["id"]},
                "summary": "something's wrong " + _uuid(),
                "issuetype": {"id": "10002"},
            }
        })
    return client.post(
        "/rest/api/2/issue/bulk",
        data={"issueUpdates": issues},
    )


def random_issue(client):
    return random.choice(client.send("GET", "/rest/api/2/search").json()["issues"])


def comment(client):
    issue = random_issue(client)
    return client.post(
        "/rest/api/2/issue/{}/comment".format(issue["id"]),
        data={
            "body": "generated comment " + _uuid(),
        }
    )


def worklog(client):
    issue = random_issue(client)
    return client.post(
        "/rest/api/2/issue/{}/worklog".format(issue["id"]),
        data={
            "comment": "generated worklog " + _uuid(),
            "timeSpentSeconds": random.randint(30, 12000),
        }
    )


def jpretty(text):
    try:
        return json.dumps(json.loads(text), indent=2)
    except:
        return text


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", required=True, help="config file")
    parser.add_argument("--num-issues", type=int, default=1,
                        help="when creating issues, the number to create "
                        "(using the bulk endpoint)")
    parser.add_argument("resource",
                        choices=["project",
                                 "version",
                                 "project_category",
                                 "issue",
                                 "comment",
                                 "worklog"])
    s_args = parser.parse_args()
    with open(s_args.config) as f:
        config = json.loads(f.read())
    func = eval(s_args.resource)
    f_args = [WriteableClient(config)]
    if "args" in func.__code__.co_varnames:
        f_args.append(s_args)
    res = func(*f_args)
    if type(res) == Response:
        print(res.status_code, jpretty(res.text))
    else:
        print(res)

if __name__ == "__main__":
    main()
