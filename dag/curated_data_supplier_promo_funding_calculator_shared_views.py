from datetime import datetime, timedelta
from copy import deepcopy

from airflow import DAG, configuration

import dwh_import
from configuration import config
from datahub.curated_data.entities_config import EntitiesConfig
from dev_env_conf.process_curated_data import ProcessCuratedData
from datahub.dwh_import_read_replica import DwhImportReadReplica
from helpers.common import set_schedule_interval

version = 1

default_args = {
    'owner': 'commercial-analytics',
    'start_date': datetime(2025, 5, 20),
    'retries': 2,
    'max_active_tasks': 20,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5)
}

dags_folder = configuration.get('core', 'dags_folder')

doc_md = f"""
# Create the shared views (Supplier Funding Calculator)

This DAG takes care of creating the views in the `supplier-funding-calculator` module of QC.

This DAG runs only once a day and is dependent on the schedule of the maintenance tasks (see
`configuration/yaml/config.yaml` for changes to that).
"""

dwh_import_read_replicas = DwhImportReadReplica(config)

dag_id_prefix = "curated-data-supplier-funding-calculator-shared-views"
with DAG(
        dag_id = f"{dag_id_prefix}-v{version}",
        description=f'Create Supplier Promo Funding Calculator shared views',
        schedule=set_schedule_interval(config, dag_id_prefix,'30 11 1 * *'),
        default_args={**dwh_import.DEFAULT_ARGS, **default_args},
        tags=['curated-data-shared'],
        max_active_runs=1,
        template_searchpath=[
            f'{dags_folder}/curated_data_supplier_promo_funding_calculator/sql/',
            f'{dags_folder}/common/sql/functions/'
        ],
        catchup=False
    ) as dag:

    dag.doc_md = doc_md

    configs = deepcopy(config['curated_data_supplier_promo_funding_calculator'])

    for table in config['curated_data_supplier_promo_funding_calculator']['dmart']['tables']:
        configs['dmart']['tables'].append(table)

    etl = ProcessCuratedData(
        dag=dag,
        project_id=config.get('bigquery').get('project_id'),
        dataset_id=config.get('bigquery').get('dataset').get('cl'),
        config=configs,
        entities=EntitiesConfig().entities,
        policy_tags=config.get('policy_tags', []),
        create_daily_tasks=True,
        dwh_import=dwh_import_read_replicas
    )

    tasks = etl.render()
