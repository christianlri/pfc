from datetime import timedelta, datetime

from airflow import DAG
from airflow.configuration import conf

import dwh_import
from configuration import config
from configuration.default_params import CURATION_DEFAULT_SCHEDULE_INTERVAL, CURATION_DEFAULT_START_DATE
from datahub.curated_data.entities_config import EntitiesConfig
from dev_env_conf.process_curated_data import ProcessCuratedData
from datahub.dwh_import_read_replica import DwhImportReadReplica
from helpers.common import set_schedule_interval

version = 1
default_args = {
    'owner': 'commercial-analytics',
    'table_priority': '1',
    'job_resource_category': '3',
    'start_date': datetime(2023, 12, 3),
    'retries': 1,
    'max_active_tasks': 3,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5)
    }
dags_folder = conf.get('core', 'dags_folder')

dag_id_prefix = 'curated-data-supplier-promo-funding-calculator-weekly'
dag_id = f'{dag_id_prefix}-v{version}'

doc_md = """
# Supplier Promo Funding Calculator Migration to QC Datahub.
This DAG automates the supplier promo funding calculation process.

## Disclaimer:
DO NOT change the schedule of this pipeline, without assessing the tasks.
The scheduling of this pipeline is one of the key criterion in reading and filtering data for the module.
Please refer the table descriptions of the tasks & README.md.
    - (/dags/dmart/curated_data_supplier_promo_funding_calculator/README.md)
"""

table_conf = config.get('curated_data_supplier_promo_funding_calculator').get('dmart').get('tables')
reports = config.get('curated_data_supplier_promo_funding_calculator').get('dmart').get('reports')

dwh_import_read_replicas = DwhImportReadReplica(config)

with DAG(
        dag_id=dag_id,
        description='Create curated data on BigQuery for the supplier promo funding calculator',
        schedule=set_schedule_interval(config, dag_id_prefix, '30 10 * * 1'), # DO NOT change the schedule of this pipeline, without assessing the tasks.
        default_args={**dwh_import.DEFAULT_ARGS, **default_args},
        tags=['curated-data'],
        max_active_runs=1,
        template_searchpath=[
            f'{dags_folder}/curated_data_supplier_promo_funding_calculator/sql/'
        ],
        catchup=False
    ) as dag:

    dag.doc_md = doc_md

    etl = ProcessCuratedData(
        dag=dag,
        project_id=config.get('bigquery').get('project_id'),
        dataset_id=config.get('bigquery').get('dataset').get('cl'),
        dataset_id_expectations=config.get('bigquery').get('dataset').get('expectations'),
        config=config.get('curated_data_supplier_promo_funding_calculator'),
        dwh_import=dwh_import_read_replicas,
        policy_tags=config.get('policy_tags', []),
        entities=EntitiesConfig().entities,
        create_daily_tasks=False,
        pool_name_queries='dmart_curated_data_queries',
        evolve_stream_table_schema=True
    )

    tasks = etl.render()
