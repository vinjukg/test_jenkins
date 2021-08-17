import six
import click
import subprocess

from google.cloud import bigquery
from google.cloud import storage


@click.command()
@click.option(
    "--env",
    required=True,
    type=click.Choice(["dev", "stg", "pre_prd", "prd"]),
    help="Environment",
)
@click.option("--gcp-project-id", required=True, help="GCP project Id")
@click.option("--gcs-bucket", required=True, help="GCS bucket name")
@click.option(
    "--execution-order-file",
    required=True,
    help="File path, which contains the Execution order of the BigQuery DDL statements",
)
@click.option(
    "--dry-run", type=click.BOOL, default=False, help="Dry run",
)
def main(
    env, gcp_project_id, gcs_bucket, execution_order_file, dry_run,
):

    bq_client = bigquery.Client(project=gcp_project_id)
    storage_client = storage.Client(project=gcp_project_id)

    success = []
    failure = []

    sql_order_list = gcs_download_data(
        storage_client, gcs_bucket, execution_order_file
    ).split("\n")
    sql_order_list = list(filter(None, sql_order_list))

    for sql_file in sql_order_list:
        try:
            query_template = gcs_download_data(
                storage_client, gcs_bucket, sql_file
            )
            query = query_template.format(project_id=gcp_project_id, env=env)
        except Exception as e:
            print(f"Failure: {sql_file}")
            print(str(e))
            failure.append(sql_file)
            continue

        if dry_run:
            status = bq_execute(bq_client, query, dry_run)
        else:
            status = bq_execute(bq_client, query)
        if status:
            print(f"Success: {sql_file}")
            success.append(sql_file)
        else:
            print(f"Failure: {sql_file}")
            failure.append(sql_file)
    total_sql_files = len(sql_order_list)
    print(
        f"{20*'='} {len(success)} jobs success out of {total_sql_files} {20*'='}"
    )
    [print(i) for i in success]
    print(
        f"{20*'='} {len(failure)} jobs failure out of {total_sql_files} {20*'='}"
    )
    [print(i) for i in failure]


def run_cli(cmd, as_shell=False, log_cmd=True):
    """
    run CLI command
    """

    if isinstance(cmd, six.string_types):
        # single string have to be passed with `shell=True`
        as_shell = True
        if log_cmd:
            print(cmd)
    else:
        if log_cmd:
            print(" ".join(cmd))
    sp = subprocess.Popen(
        cmd, shell=as_shell, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    stdout = ""
    while True:
        line = sp.stdout.readline()
        if not line:
            break
        stdout += line.decode("UTF-8")
        print(line.decode("UTF-8").strip())
    sp.wait()
    if sp.returncode:
        raise Exception(stdout)

    return stdout


def bq_execute(client, query, dry_run=False):
    """
    Execute SQL file in BigQuery

    :param client: BigQuery client
    :param query: SQL Query
    :param dry_run: Rehearsal of BigQuery run before the real execution.

    :return: True if SQL execution was success else False
    """

    job_config = bigquery.QueryJobConfig(dry_run=dry_run)
    try:
        query_job = client.query(query, job_config=job_config)
        if not dry_run:
            query_job.result()
    except Exception as e:
        print(str(e))
        return False

    return True


def gcs_download_data(client, gcs_bucket, gcs_file):
    """
    Download data from GCS

    :param client: GCS client
    :param gcs_bucket: GCS bucket name
    :param gcs_file: GCS file path

    :return: list of lines that read from GCS file
    """

    bucket = client.get_bucket(gcs_bucket)
    blob = bucket.blob(gcs_file)
    string_data = blob.download_as_string().decode("utf-8")

    return string_data


if __name__ == "__main__":
    main()
