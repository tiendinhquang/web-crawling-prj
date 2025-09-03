from abc import ABC, abstractmethod
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import asyncio
import logging
import time
from typing import List, Dict, Any, Optional


class BaseReportsDAG(ABC):
    """
    Base abstract class for source DAGs that follow the pattern:
    1. Refresh credentials
    2. Create reports
    3. Monitor and wait for completion
    4. Save/download reports
    """        
    def __init__(self, dag_id: str):
        self.dag_id = dag_id
    @abstractmethod
    def get_service(self):
        """Return the service instance for this source"""
        pass
    
    @abstractmethod
    def get_report_configs(self, **context) -> List[Dict[str, Any]]:
        """Return the list of report configurations"""
        pass
    
    @abstractmethod
    def refresh_credentials(self):
        """Refresh credentials (tokens, cookies, etc.)"""
        pass
    
    @abstractmethod
    def create_report(self, report_config: Dict[str, Any]) -> Any:
        """Create a single report and return the report ID/response"""
        pass
    
    @abstractmethod
    def get_report_status(self, report_id: Any, report_config: Dict[str, Any]) -> Dict[str, Any]:
        """Get the status of a report"""
        pass
    
    @abstractmethod
    def is_report_ready(self, status_response: Dict[str, Any]) -> bool:
        """Check if a report is ready for download"""
        pass
    
    @abstractmethod
    def save_report(self, report_id: Any, report_config: Dict[str, Any], status_response: Dict[str, Any]) -> str:
        """Save/download the completed report and return the save path"""
        pass
    
    @abstractmethod
    def get_report_identifier(self, report_config: Dict[str, Any]) -> str:
        """Get a unique identifier for the report (for logging/monitoring)"""
        pass
    
    def create_dag_tasks(self):
        """Create the DAG with the common workflow pattern"""
        @task
        def refresh_credentials_task():
            """Refresh credentials (tokens, cookies, etc.)"""
            try:
                self.refresh_credentials()
                logging.info(f"✅ Credentials refreshed successfully for {self.dag_id}")
                return True
            except Exception as e:
                logging.error(f"❌ Failed to refresh credentials for {self.dag_id}: {e}")
                raise e
        
        @task
        def create_all_reports_task(**context):
            """Create all reports and return their configurations with IDs"""
            service = self.get_service()
            report_configs = self.get_report_configs(context)
            results = []
            
            for report_config in report_configs:
                try:
                    report_id = asyncio.run(self.create_report(report_config))
                    
                    logging.info(f"✅ Report {self.get_report_identifier(report_config)} created with ID: {report_id}")
                    results.append({
                        'report_config': report_config,
                        'report_id': report_id,
                        'created_at': datetime.now(),
                        'status': 'CREATED'
                    })
                except Exception as e:
                    logging.error(f"❌ Failed to create report {self.get_report_identifier(report_config)}: {e}")
                    results.append({
                        'report_config': report_config,
                        'report_id': None,
                        'created_at': datetime.now(),
                        'status': 'CREATION_FAILED',
                        'error': str(e)
                    })
            
            return results
        
        @task(execution_timeout=timedelta(minutes=30))
        def monitor_and_save_reports_task(reports_data: List[Dict[str, Any]]):
            """Monitor report status and save when ready"""
            async def process_single_report(report_data: Dict[str, Any]):
                """Process a single report - monitor status and save when ready"""
                if not report_data.get('report_id'):
                    logging.error(f"Report {self.get_report_identifier(report_data['report_config'])} has no ID, skipping")
                    return report_data
                
                report_id = report_data['report_id']
                report_config = report_data['report_config']
                report_identifier = self.get_report_identifier(report_config)
                
                logging.info(f"Starting to monitor report {report_identifier} with ID: {report_id}")
                
                # Monitor status every 10 seconds
                max_wait_time = 3600  # 1 hour max wait
                start_time = time.time()
                
                while time.time() - start_time < max_wait_time:
                    try:
                        status_response = await self.get_report_status(report_id, report_config)
                        
                        logging.info(f"Report {report_identifier} (ID: {report_id}) status: {status_response}")
                        
                        if self.is_report_ready(status_response):
                            # Report is ready, save it immediately
                            try:
                                save_path = await self.save_report(report_id, report_config, status_response)
                                
                                logging.info(f"✅ Report {report_identifier} saved successfully to {save_path}")
                                report_data['status'] = 'COMPLETED'
                                report_data['saved_path'] = save_path
                                report_data['completed_at'] = datetime.now()
                                return report_data
                                
                            except Exception as save_error:
                                logging.error(f"❌ Failed to save report {report_identifier}: {save_error}")
                                report_data['status'] = 'SAVE_FAILED'
                                report_data['error'] = str(save_error)
                                return report_data
                        
                        # Wait 10 seconds before next check
                        await asyncio.sleep(10)
                        
                    except Exception as e:
                        logging.error(f"Error checking status for report {report_identifier}: {e}")
                        await asyncio.sleep(10)
                
                # Timeout reached
                logging.warning(f"Report {report_identifier} timed out after {max_wait_time} seconds")
                report_data['status'] = 'TIMEOUT'
                return report_data
            
            async def process_all_reports():
                """Process all reports in parallel"""
                tasks = [process_single_report(report) for report in reports_data]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Handle any exceptions from individual tasks
                processed_results = []
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logging.error(f"Task {i} failed with exception: {result}")
                        reports_data[i]['status'] = 'TASK_FAILED'
                        reports_data[i]['error'] = str(result)
                        processed_results.append(reports_data[i])
                    else:
                        processed_results.append(result)
                
                return processed_results
            
            # Run the async processing
            try:
                final_results = asyncio.run(process_all_reports())
                
                # Log summary
                completed = sum(1 for r in final_results if r.get('status') == 'COMPLETED')
                failed = sum(1 for r in final_results if r.get('status') in ['CREATION_FAILED', 'SAVE_FAILED', 'TASK_FAILED', 'TIMEOUT'])
                total_reports = len(final_results)
                
                logging.info(f"Report processing completed. Success: {completed}, Failed: {failed}, Total: {total_reports}")
                
                # Check failure threshold - fail if failed reports >= max_reports/2
                max_failures = (total_reports + 1) // 2  
                if failed >= max_failures:
                    error_msg = f"❌ Too many report failures: {failed}/{total_reports} (threshold: {max_failures})"
                    logging.error(error_msg)
                    raise Exception(error_msg)
                
                return final_results
                
            except Exception as e:
                logging.error(f"Failed to process reports: {e}")
                # Re-raise the exception to ensure Airflow marks the task as failed
                raise e
        
        # Return the task functions
        return refresh_credentials_task, create_all_reports_task, monitor_and_save_reports_task