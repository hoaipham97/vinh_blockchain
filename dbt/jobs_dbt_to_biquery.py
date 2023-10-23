from google.cloud import bigquery
import requests
from datetime import datetime
import pandas as pd

GCP_PROJECT_ID = ''
DBT_PROJECT_ID = ''
DBT_ENVIROMENT_ID = ''
DBT_JOBS_TABLE_ID = 'project_id.dbt_metadata.dbt_jobs'
now = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
bq_client = bigquery.Client(GCP_PROJECT_ID)

class DBT_Jobs():
    def __init__(self, bq_client, project_id, dbt_jobs_table_id):
        self.project_id = project_id
        self.dbt_jobs_table_id = dbt_jobs_table_id
        self.client = bq_client
    
    def get_list_jobs(self):
        headers = {
            'header': 'Content-Type: application/json',
            'Authorization': ''
        }
        get_jobs_api = f'https://cloud.getdbt.com/api/v2/accounts/31254/jobs/?project_id={DBT_PROJECT_ID}&environment_id={DBT_ENVIROMENT_ID}'
        list_jobs_json=requests.get(get_jobs_api, headers=headers).json()
        return list_jobs_json
    
    def preprocessing(self):

        list_jobs_json = self.get_list_jobs()

        def get_list_job():
            return list_jobs_json['data']

        def convert_timestamp_to_date(ts):
            date_time_obj = datetime.strptime(ts, '%Y-%m%dT%H:%M:%S')
            return date_time_obj.strftime("%Y-%m-%d %H:%m%s")

        def dict_to_df(list_jobs):
            my_df=pd.DataFrame.from_dict(list_jobs)
            return my_df

        def get_job_info(job):
            job_json = {}
            job_json['id'] = str(job['id'])
            job_json['account_id'] = str(job['account_id'])
            job_json['project_id'] = str(job['project_id'])
            job_json['environment_id'] = str(job['environment_id'])
            job_json['name'] = str(job['name'])
            job_json['created_at'] = str(job['created_at'])
            job_json['updated_at'] = str(job['updated_at'])
            job_json['run_failure_count'] = str(job['run_failure_count'])
            job_json['schedule'] = str(job['schedule']['cron'])
            job_json['ingestion_time'] = now
            return job_json
        

        def json_to_df():
            list_jobs = get_list_job()
            my_list = []
            for job in list_jobs:
                job_info = get_job_info(job)
                my_list.append(job_info)
            my_df = dict_to_df(my_list)
            return my_df
        
        def jobs_to_bigquery(my_df):
            my_schema = [
                bigquery.SchemaField("id", "STRING"),
                bigquery.SchemaField("account_id", "STRING"),
                bigquery.SchemaField("project_id", "STRING"),
                bigquery.SchemaField("environment_id", "STRING"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("created_at", "STRING"),
                bigquery.SchemaField("updated_at", "STRING"),
                bigquery.SchemaField("run_failure_count", "STRING"),
                bigquery.SchemaField("schedule", "STRING"),
                bigquery.SchemaField("ingestion_time", "STRING")
            ]
            job_config = bigquery.LoadJobConfig(
                schema=my_schema,
                write_disposition="WRITE_TRUNCATE",
            )

            job = self.client.load_table_from_dataframe(
                my_df, self.dbt_jobs_table_id, job_config=job_config
            ) 
            job.result()
        
        print("Start")
        my_df = json_to_df()
        jobs_to_bigquery(my_df)
        print("Done")
    
def main():
    jobs = DBT_Jobs(bq_client, GCP_PROJECT_ID, DBT_JOBS_TABLE_ID)
    jobs.preprocessing()

if __name__ == "__main__":
    main()

            

