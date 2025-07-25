from airflow.decorators import dag,task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from dags.notification_handler import send_failure_notification, send_success_notification
from datetime import timedelta

@dag(
    dag_id = "wayfair.dag_main",
    schedule="0 1,8,15 * * *",  # 1h, 8h, 15h hàng ngày,  # 7h sáng mỗi ngày
    start_date=datetime(2025, 6, 20),
    catchup=False,
    tags=["daily", "wayfair"],
    on_failure_callback=send_failure_notification,
    on_success_callback=send_success_notification,
    description="Main DAG for Wayfair with branching load type, daily load target skus to table wayfair.product_skus",
)

def wayfair_dag_main():
    trigger_iload_product_detail = TriggerDagRunOperator(
        task_id='trigger_iload_product_detail',
        trigger_dag_id='wayfair.dag_get_product_detail',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=5,
        retries=3,
        retry_delay=timedelta(minutes=30),
    )

    trigger_iload_product_info = TriggerDagRunOperator(
        task_id='trigger_iload_product_info',
        trigger_dag_id='wayfair.dag_get_product_info',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=5,
        retries=3,
        retry_delay=timedelta(minutes=30),
    )

    trigger_iload_product_skus = TriggerDagRunOperator(
        task_id='trigger_iload_product_skus',
        trigger_dag_id='wayfair.dag_iload_product_skus',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=5,
        retries=3,
        retry_delay=timedelta(minutes=30),
    )

    trigger_iload_product_detail >> trigger_iload_product_info >> trigger_iload_product_skus

dag_instance = wayfair_dag_main()

