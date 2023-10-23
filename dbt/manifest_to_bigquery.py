import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import sys
from google.oauth2 import service_account

PROJECT_ID = ''
MANIFEST_URL = 'https://git-gst.mto.zing.vn/api/v4/projects/70/repository/files/target%2Fmanifest.json/raw?ref=master'
GITLAB_TOKEN = ''
MANIFEST_TABLE_ID = 'project_id.dbt_metadata.manifest_metadata'
now = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
bq_client = bigquery.Client(PROJECT_ID)

# def create_client_bigquery():
#     acc_info = gcs_info["mps_data_service_account"]
#     gcloud_project_id = gcs_info["mps_data_service_account"]["project_id"]
#     client = bigquery.Client(
#         project=gcloud_project_id,
#         credentials=service_account.Credentials.from_service_account_info(
#             acc_info)
#     )
#     return client

class ManifestRaw():
    def __init__(self, bq_client, project_id, manifest_url, manifest_table_id):
        self.project_id = project_id
        self.gitlab_token = ''
        self.manifest_url = manifest_url
        self.manifest_table_id = manifest_table_id
        self.client = bq_client

    def get_manifest_json(self):
        headers = {
            'PRIVATE-TOKEN': self.gitlab_token
        }
        metadata_json=requests.get(self.manifest_url, headers=headers).json()
        return metadata_json

    def get_list_nodes(self, metadata_json):
        list_nodes = metadata_json['nodes']
        return list_nodes

    def get_node_by_name(self, name, list_nodes):
        return list_nodes[name]

    # return a list of nodes depend on another node
    def get_nodes_depend_on(self, node):
        return node['depends_on']['nodes']

    # return a list tags of node
    def get_tags_node(self, node):
        return node['tags']

    def get_created(self, node):
        return node['created_at']

    def get_unique_name(self, node):
        return node['unique_id']

    def get_materialized(self, node):
        return node['config']['materialized']

    def get_source_table(self, my_arr):
        list_source = []
        list_source_str = ''
        if(len(my_arr) > 0):
            for tag in my_arr:
                if ("source_table" in tag):
                    src_table = tag.replace("source_table:", "")
                    list_source.append(src_table)
                    list_source_str = list_source_str + ',' + src_table
        return list_source, list_source_str

    def get_sink_table(self, my_arr):
        list_sink = []
        list_sink_str = ''
        if(len(my_arr) > 0):
            for tag in my_arr:
                if ("sink_table" in tag):
                    sink_table = tag.replace("sink_table:", "")
                    list_sink.append(sink_table)
                    list_sink_str = list_sink_str + ',' + sink_table
        return list_sink, list_sink_str

    def get_source_model(self, my_arr):
        list_source_model = []
        list_source_model_str = ''
        if(len(my_arr) > 0):
            for tag in my_arr:
                if ("source_model" in tag):
                    source_model = tag.replace("source_model:", "")
                    list_source_model.append(source_model)
                    list_source_model_str =  list_source_model_str + ',' + source_model
        return list_source_model, list_source_model_str

    def get_sink_model(self, my_arr):
        list_sink_model = []
        list_sink_model_str = ''
        if(len(my_arr) > 0):
            for tag in my_arr:
                if ("sink_model" in tag):
                    sink_model = tag.replace("sink_model:", "")
                    list_sink_model.append(sink_model)
                    list_sink_model_str = list_sink_model_str + ',' + sink_model
        return list_sink_model, list_sink_model_str

    def convert_timestamp_to_date(self, ts):
        ts = int(ts)
        return (datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'))

    def dict_to_df(self, list_nodes):
        my_df=pd.DataFrame.from_dict(list_nodes)
        return my_df

    def process_manifest_to_bigquery(self):
        def processing_df():
            json_manifest = self.get_manifest_json()
            list_nodes = self.get_list_nodes(json_manifest)
            my_dict = []
            for key, value in list_nodes.items():
                json_object = {}
                json_object['name'] = key
                node = self.get_node_by_name(key, list_nodes)
                json_object['unique_id'] = self.get_unique_name(node)
                json_object['depends_on'] = self.get_nodes_depend_on(node)
                json_object['tags'] = self.get_tags_node(node)
                list_source_table, list_source_table_str = self.get_source_table(self.get_tags_node(node))
                json_object['source_table'] = list_source_table_str
                list_sink_table, list_sink_table_str = self.get_sink_table(self.get_tags_node(node))
                json_object['sink_table'] = list_sink_table_str
                list_source_model, list_source_model_str = self.get_source_model(self.get_tags_node(node))
                json_object['source_model'] = list_source_model_str
                list_sink_model, list_sink_model_str = self.get_sink_model(self.get_tags_node(node))
                json_object['sink_model'] = list_sink_model_str
                json_object['materialized'] = self.get_materialized(node)
                json_object['created_at'] = self.convert_timestamp_to_date(self.get_created(node))
                json_object['ingestion_time'] = now
                my_dict.append(json_object)

            return self.dict_to_df(my_dict)

        def to_bigquery(my_df):
            my_schema = [
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("unique_id", "STRING"),
                bigquery.SchemaField("depends_on", "STRING", "repeated"),
                bigquery.SchemaField("tags", "STRING", "repeated"),
                bigquery.SchemaField("source_table", "STRING", "repeated"),
                bigquery.SchemaField("sink_table", "STRING", "repeated"),
                bigquery.SchemaField("source_model", "STRING", "repeated"),
                bigquery.SchemaField("sink_model", "STRING", "repeated"),
                bigquery.SchemaField("materialized", "STRING"),
                bigquery.SchemaField("created_at", "STRING"),
                bigquery.SchemaField("ingestion_time", "STRING")
            ]
            job_config = bigquery.LoadJobConfig(
                schema=my_schema,
                write_disposition="WRITE_APPEND",
            )

            job = self.client.load_table_from_dataframe(
                my_df, self.manifest_table_id, job_config=job_config
            )
            job.result()

        print("Start to get manifest from gitlab")
        my_df = processing_df()
        print("Start to push into bigquery")
        to_bigquery(my_df)
        print("Done")

def main():
    # bq_client = create_client_bigquery()
    manifest_object = ManifestRaw(bq_client, PROJECT_ID, MANIFEST_URL, MANIFEST_TABLE_ID)
    manifest_object.process_manifest_to_bigquery()

if __name__ == "__main__":
    main()

