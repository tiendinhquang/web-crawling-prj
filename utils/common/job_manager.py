import requests
import logging
from typing import Dict, Any
import time
import yaml


# Load token
try:
    with open("config/credentials.yaml", "r") as f:
        credentials = yaml.safe_load(f)
        token = credentials.get("token")
        if not token:
            raise ValueError("Token not found in credentials.yaml")
except Exception as e:
    logging.error(f"Failed to load credentials: {e}")
    raise



import requests
import logging
from typing import Dict, Any
import time
import yaml


class JobManager:
    def __init__(self):
        self.get_job_url = "http://172.17.1.205:8000/api/v1/jobs"

        # Load token khi tạo instance
        try:
            with open("config/credentials.yaml", "r") as f:
                credentials = yaml.safe_load(f)
                self.token = credentials.get("token")
                if not self.token:
                    raise ValueError("Token not found in credentials.yaml")
        except Exception as e:
            logging.error(f"Failed to load credentials: {e}")
            raise

        self.headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

    def create_job(self, create_job_url: str, payload: dict) -> str:
        resp = requests.post(create_job_url, headers=self.headers, json=payload)
        resp.raise_for_status()
        job_id = resp.json()["job_id"]
        logging.info(f"Job created successfully: {job_id}")
        return job_id

    def poll_job_until_done(self, job_id: str) -> Dict[str, Any]:
        while True:
            resp = requests.get(f"{self.get_job_url}/{job_id}", headers=self.headers)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") == "success":
                logging.info(f"Job {job_id} completed successfully")
                return data.get("result", {}).get("data", data)

            time.sleep(1)
