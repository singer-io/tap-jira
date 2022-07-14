import argparse
import boto3
from botocore.exceptions import ClientError
import datetime
import os
import requests
import subprocess

yesterday = datetime.date.today() - datetime.timedelta(days=1)
PARAMS = {"circle-token": os.getenv("CIRCLE_USER_TOKEN")}
name = 'com-stitchdata-dev-qa-artifacts'

def setUp(repo):
    """
    1) Setup S3 destination in QA bucket.
      - List folders in bucket
      - Create folder for repo if does not exist already
    """
    repo_key = repo + '/'

    try:
        client = boto3.client('s3')
        response = client.get_object(Bucket=name, Key=repo_key)
        print(f"Key {repo_key} exists in bucket {name}.")

    except client.exceptions.NoSuchKey as ex:
        print(f"Key {repo_key} does not exist in bucket {name}.")
        response = client.put_object(Bucket=name, Key=repo_key)
        print(f"Created new key {repo_key} in bucket {name}.")

    except ClientError as err:
        print(f"Couldn't create an object in {name}. Reason: "
              f"{err.response['Error']}")

def get_artifacts(repo):
    """
    2) Gather files to save.
      - Look for yesterday's jobs on master (or main) in CircleCI
      - Get log files from jobs in CircleCI
      - Generate a file name like <repo>-<date>-<workflow_id>-test.log
    """
    base_url = "https://circleci.com/api/v2"
    gh_org = "stitchdata" if repo == "ui-automation" else "singer-io"

    # get all pipelines
    url = f"{base_url}/project/gh/{gh_org}/{repo}/pipeline"
    additional_params = {"branch": "master",
                         "page_token": None}
    additional_params.update(PARAMS)
    response = requests.get(url, params=additional_params)
    print(f"GET url={url} STATUS: {response.status_code}")
    response.raise_for_status()
    results = response.json()
    pipeline_ids = []
    for item in results['items']:
        pipeline_updated_at = datetime.datetime.strptime(
            item['updated_at'], "%Y-%m-%dT%H:%M:%S.%fZ").date()
        if yesterday == pipeline_updated_at:
            pipeline_ids.append(item['id'])

    # get the workflow_id of the latest pipeline
    workflow_ids = []
    for pipeline_id in pipeline_ids:
        url = f"{base_url}/pipeline/{pipeline_id}/workflow"
        response = requests.get(url, params=PARAMS)
        print(f"GET url={url} STATUS: {response.status_code}")
        response.raise_for_status()
        results = response.json()
        for item in results['items']:
            workflow_stopped_at = datetime.datetime.strptime(
                item['stopped_at'], "%Y-%m-%dT%H:%M:%SZ").date()
            if yesterday == workflow_stopped_at:
                workflow_ids.append(item['id'])

    for workflow_id in workflow_ids:

        # get the job(s) from the workflow
        url = f"{base_url}/workflow/{workflow_id}/job"
        response = requests.get(url, params=PARAMS)
        print(f"GET url={url} STATUS: {response.status_code}")
        response.raise_for_status()
        results = response.json()
        job_numbers = [item['job_number']
                       for item in results['items']
                       if 'test' in item.get('name')
                       and item.get('job_number')]

        # get the artifact urls from the job
        artifacts = []
        for job_number in job_numbers:
            artifacts_endpoint = f"{base_url}/project/gh/{gh_org}/{repo}/{job_number}/artifacts"
            response = requests.get(artifacts_endpoint, params=PARAMS)
            print(f"GET url={url} STATUS: {response.status_code}")
            response.raise_for_status()
            results = response.json()
            urls = [item['url']
                    for item in results['items']
                    if item['path'].endswith('.log')]
            artifacts += urls

        # pull down the artifacts
        for artifact in artifacts:
            cmd = ["tests/artifacts.sh", artifact, repo, workflow_id]
            print("**************************************************")
            proc = subprocess.run(cmd, capture_output=True)
            print(f"executing subprocess: {' '.join(cmd)}")
            print(proc.stdout.decode('utf8'))
            print(proc.stderr.decode('utf8'))
            print(f"Finished with exit_code {proc.returncode}")
            print("**************************************************")

def save_artifacts(repo):
    """
    3) Save file(s) to folder in s3 QA bucket
       Ensure saving file was succesful
    """
    artifacts_path = f"/opt/code/tap-tester/artifacts/{repo}"
    uuids = os.listdir(artifacts_path)
    date = str(yesterday)
    try:
        client = boto3.client('s3')
        for uuid in uuids:
            key = f"{repo}/{date}/{uuid}/"
            response = client.put_object(Bucket=name, Key=key)
            print(f"Successfully uploaded key {key} to bucket {name}")
            artifacts = os.listdir(f"{artifacts_path}/{uuid}")
            for artifact in artifacts:
                with open(f"{artifacts_path}/{uuid}/{artifact}", "rb") as art_file:
                    target_file = key + artifact
                    response = client.upload_fileobj(art_file, name, target_file)
                    print(f"Successfully uploaded file {name}/{key}/{artifact}")

    except ClientError as err:
        print(f"Couldn't create an object in {name}. Reason: "
              f"{err.response['Error']}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--repo',
                        help='Github repo / CircleCI project to pull artifacts from.', required=False)
    parser.add_argument('-w', '--workflow',
                        help='Github repo / CircleCI project to pull artifacts from.', required=False)
    args = parser.parse_args()

    if args.repo:

        # step 1) ensure the repo's key is in the bucket
        setUp(args.repo)

        # step 2) get the log artifacts from yesterday's master builds
        get_artifacts(args.repo, args)

        # step 3) upload artifacts to qa bucket
        save_artifacts(args.repo)

    else:
        parser.error("No repo/project provided! "
                     "Please specify the name of a Github repo corresponding to a CircleCI project")


if __name__ == '__main__':
    main()
