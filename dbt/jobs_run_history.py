from google.cloud import bigquery
import requests
from datetime import datetime
import pandas as pd

GCP_PROJECT_ID = ''
DBT_PROJECT_ID = ''
DBT_ENVIROMENT_ID = ''
today = datetime.today().strftime('%Y%m%d')
DBT_JOBS_RUN_TABLE_ID = f''
now = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
bq_client = bigquery.Client(GCP_PROJECT_ID)
OFFSET = 0
LIMIT = 100

class DBT_Jobs_Run():
    def __init__(self, bq_client, project_id, dbt_jobs_run_table_id):
        self.project_id = project_id
        self.dbt_jobs_run_table_id = dbt_jobs_run_table_id
        self.client = bq_client
    
    def get_list_job_run(self, offset):
        headers = {
            'header': 'Content-Type: application/json',
            'Authorization': ''
        }
        print(offset)
        get_jobs_api = f'https://cloud.getdbt.com/api/v2/accounts/31254/runs/?project_id={DBT_PROJECT_ID}' \
                       f'&environment_id={DBT_ENVIROMENT_ID}&&limit={LIMIT}&offset={offset}'
        list_jobs_json=requests.get(get_jobs_api, headers=headers).json()
        return list_jobs_json

    def get_pagination(self):
        get_result = self.get_list_job_run(0)
        return get_result['extra']['pagination']['total_count']

    def set_offset(self):
        total_count = self.get_pagination()
        return total_count - LIMIT
    
    def preprocessing(self):
        new_offset = self.set_offset()

        list_jobs_run_json = self.get_list_job_run(new_offset)

        def get_list_jobs_run():
            return list_jobs_run_json['data']

        def dict_to_df(list_jobs):
            my_df=pd.DataFrame.from_dict(list_jobs)
            return my_df

        def json_to_df():
            list_jobs = get_list_jobs_run()
            my_df = dict_to_df(list_jobs)
            return my_df
        
        def jobs_to_bigquery(my_df):
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
            )
            job = self.client.load_table_from_dataframe(
                my_df, self.dbt_jobs_run_table_id, job_config=job_config
            ) 
            job.result()
        
        print("Start")
        my_df = json_to_df()
        print(my_df)
        jobs_to_bigquery(my_df)
        print("Done")

    def get_job_failed(self):
        sql_get_job_failed = f"""
        SELECT distinct job_definition_id, status, status_message, status_humanized,
        in_progress, is_complete, is_success, is_error, is_cancelled, href
        FROM `project_id.dbt_metadata.dbt_jobs_run_{today}`
        where git_branch = 'master' limit 3
        """
        list_jobs_failed = self.client.query(sql_get_job_failed).result()
        return list_jobs_failed


def main():
    jobs_run = DBT_Jobs_Run(bq_client, GCP_PROJECT_ID, DBT_JOBS_RUN_TABLE_ID)
    aa = jobs_run.get_job_failed()
    if(aa.total_rows > 0):
        for row in aa:
            status = row.status
            status_message = row.status_message
            job_definition_id = row.job_definition_id
            url = row.href
            


if __name__ == "__main__":
    main()
