import requests
from abc import ABC, abstractmethod
import yaml
import os
import logging

class BaseNotificationHandler(ABC):
    @abstractmethod
    def send_notification(self, context):
        pass

    def _build_airflow_dag_run_structure(self, context, custom_msg: dict = None):
        from airflow.utils.state import State
        from airflow.utils.dates import timezone
        dag_run = context['dag_run']
        dag_id = dag_run.dag_id
        task_instance = context['task_instance']
        task_id = task_instance.task_id
        execution_date = task_instance.execution_date
        log_url = task_instance.log_url
        start_date = task_instance.start_date
        end_date = task_instance.end_date
        status = dag_run.state
        current_time = timezone.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        status_emoji = {
            State.SUCCESS: '✅',
            State.FAILED: '❌',
            State.UPSTREAM_FAILED: '❌',
        }

        facts = [
            {"title": "📌 DAG", "value": dag_id},
            {"title": "📦 Task", "value": task_id},
            {"title": "📅 Execution Date", "value": str(execution_date)},
            {"title": "📈 Status", "value": f"{status_emoji.get(status, '❔')} {status}"},
            {"title": "📝 View Log", "value": f"[📝 View Log]({log_url})"},
            
        ]

        if custom_msg:
            facts.append({"title": "📊 Statistics:", "value": ""})
            for k, v in custom_msg.items():
                facts.append({"title": f"{k.capitalize()}", "value": str(v)})

        msg = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard",
                        "version": "1.5",
                        "body": [
                            {
                                "type": "TextBlock",
                                "text": "📢 Airflow DAG Notification",
                                "wrap": True,
                                "size": "Large",
                                "weight": "Bolder"
                            },
                            {
                                "type": "FactSet",
                                "facts": facts
                            }
                        ],
                        "msteams": {
                            "width": "Full"
                        }
                    }
                }
            ]
        }
        return msg


class TeamsNotificationHandler(BaseNotificationHandler):
    def __init__(self):
        config_path = 'config/notification.yaml'
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        self.success_webhook_url = self.config['SUCCESS_CHANNEL']
        self.failure_webhook_url = self.config['FAILURE_CHANNEL']

    def send_notification(self, context, custom_msg=None, is_success=True):
        if is_success:
            webhook_url = self.success_webhook_url
        else:
            webhook_url = self.failure_webhook_url
        payload = self._build_airflow_dag_run_structure(context, custom_msg)

        response = requests.post(
            webhook_url,
            json=payload,
            headers={'Content-Type': 'application/json'}
        )
        print(f"Teams response: {response.status_code} - {response.text}")
        return response
        


def send_success_notification(context, custom_msg=None):
    noti = TeamsNotificationHandler()
    noti.send_notification(context, custom_msg, is_success=True)
    
def send_failure_notification(context, custom_msg=None):
    noti = TeamsNotificationHandler()
    noti.send_notification(context, custom_msg, is_success=False)

