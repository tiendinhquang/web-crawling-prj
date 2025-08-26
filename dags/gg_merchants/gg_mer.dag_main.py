from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from services.notification_handler import send_failure_notification, send_success_notification, send_sla_notification
from datetime import timedelta
import logging

# Define the table codes to process
DAG_CONFIGS = [
    {
        'dag_id': 'gg_merchants.dag_gmc_sku_performance_main',
        'table_name': 'gmc_sku_performance',
        'table_code': 'arielbath.gmc_sku_performance',
        'schedule': "15 8 * * *",
        'cfg': {
            "reports": ["sku_visibility"]
        }
    }
]

# Create DAGs dynamically for each table code
for cfg in DAG_CONFIGS:
    # Extract table name for DAG ID
    table_name = cfg['table_name']
    dag_id = cfg['dag_id']
    schedule = cfg['schedule']
    
    @dag(
        dag_id=dag_id,
        schedule=schedule,
        start_date=days_ago(1),
        catchup=False,
        tags=["daily", "gg_merchants", "dag main", table_name],
        on_failure_callback=send_failure_notification,
        on_success_callback=send_success_notification,
        sla_miss_callback=send_sla_notification,
        description=f"Main DAG for gg merchants processing table: {cfg['table_code']}",
        default_args={
            'owner': 'data_team',
            'depends_on_past': False,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 3,
            'retry_delay': timedelta(minutes=5),
        }
    )
    def create_table_dag():
        
        @task(task_id='start_processing')
        def start_processing():
            """Start processing task for the specific table"""
            logging.info(f"Starting Criteo DAG main processing for table: {cfg['table_code']}")
            return f"Processing started for {cfg['table_code']}"
        
        @task(task_id='validate_table_code')
        def validate_table_code():
            """Validate the table code format"""
            if '.' not in cfg['table_code']:
                raise ValueError(f"Invalid table code format: {cfg['table_code']}")
            logging.info(f"Table code {cfg['table_code']} validated successfully")
            return cfg['table_code']
        
        @task(task_id='process_table')
        def process_table():
            """Process the specific table"""
            logging.info(f"Processing table: {cfg['table_code']}")
            # Add your specific processing logic here for each table
            return f"Completed processing {cfg['table_code']}"
        
        @task(task_id='finalize_processing')
        def finalize_processing():
            """Finalize processing and log completion"""
            logging.info(f"Criteo DAG main processing completed successfully for {cfg['table_code']}")
            return f"Processing finalized for {cfg['table_code']}"
        
        # Define task dependencies
        start = start_processing()
        validation = validate_table_code()
        process = process_table()
        
        # Create TriggerDagRunOperator instances for external DAGs
        get_report_trigger = TriggerDagRunOperator(
            task_id=f'trigger_get_report_{cfg["table_name"]}',
            trigger_dag_id=f'gg_merchants.dag_get_all_report',
            conf=cfg['cfg'],
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=5,
            retries=3,
            retry_delay=timedelta(minutes=30),
            sla=timedelta(minutes=5),
            trigger_rule='none_failed',
        )
        
        iload_report_trigger = TriggerDagRunOperator(
            task_id=f'trigger_iload_report_{table_name}',
            trigger_dag_id=f'gg_merchants.dag_iload_{table_name}',
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=5,
            retries=3,
            retry_delay=timedelta(minutes=30),
        )
        
        # Create optional DAG triggers FIRST
        optional_dag_triggers = {}
        for opt_dag in cfg.get('optional_dags', []):
            if opt_dag.get('enabled', False):
                trigger = TriggerDagRunOperator(
                    task_id=opt_dag['task_id'],
                    trigger_dag_id=opt_dag['dag_id'],
                    wait_for_completion=True,
                    reset_dag_run=True,
                    poke_interval=5,
                    retries=2,
                    retry_delay=timedelta(minutes=15),
                    sla=timedelta(minutes=10),
                )
                optional_dag_triggers[opt_dag['task_id']] = trigger
        
        # Create finalize task
        finalize = finalize_processing()
        
        # Check if this table has optional DAGs
        has_optional_dags = len(cfg.get('optional_dags', [])) > 0
        
        if has_optional_dags:
            # Optional DAGs - complex workflow: start -> validation -> process -> [optional_dags] -> get_report_trigger -> iload_report_trigger -> finalize
            start >> validation >> process
            
            # Set dependencies for optional DAG triggers
            for trigger in optional_dag_triggers.values():
                process >> trigger >> get_report_trigger
            
            get_report_trigger >> iload_report_trigger >> finalize
        else:
            # No optional DAGs - simple workflow: start -> validation -> process -> get_report -> iload_report -> finalize
            start >> validation >> process >> get_report_trigger >> iload_report_trigger >> finalize
        
        # Return all tasks for reference
        if has_optional_dags:
            return {
                'start': start,
                'validation': validation,
                'process': process,
                'optional_dag_triggers': optional_dag_triggers,
                'get_report_trigger': get_report_trigger,
                'iload_report_trigger': iload_report_trigger,
                'finalize': finalize
            }
        else:
            return {
                'start': start,
                'validation': validation,
                'process': process,
                'get_report_trigger': get_report_trigger,
                'iload_report_trigger': iload_report_trigger,
                'finalize': finalize
            }
    
    # Create the DAG instance
    globals()[dag_id] = create_table_dag()

