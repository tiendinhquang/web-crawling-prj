from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from services.error_handler import create_error_handler
from utils.common.sharepoint.sharepoint_manager import SharepointManager
import os
import httpx
import asyncio
import zipfile
from pathlib import Path
import logging
class BaseBackupDataToSharepoint:
    def __init__(self):

        self.error_handler = create_error_handler(source='backup')
        self.sharepoint_manager = SharepointManager()
        self.site_id = 'atlasintl.sharepoint.com,ca126560-d5a9-4ea1-ace9-4361095e806f,1704d547-3d85-4235-9d34-ab57ec9572c4'
        self.drive_name = 'Data Backup'
        self.drive_id = 'b!YGUSyqnVoU6s6UNhCV6Ab0fVBBeFPTVCnTSrV-yVcsSKBY_zx9UOTbKKYxGAKdDc'

    def backup_data_to_sharepoint(self):
        pass


class BackupDataToSharepoint(BaseBackupDataToSharepoint):
    async def upload_file_with_retry(self, client: httpx.AsyncClient, site_id: str, drive_id: str, remote_path: str, local_path: str):
        return await self.error_handler.execute_with_retry_async(
            self.sharepoint_manager.upload_large_file,
            client=client,
            site_id=site_id,
            drive_id=drive_id,
            local_path=local_path,
            remote_path=remote_path,
            chunk_size=100,
        )
    
    def zip_folder(self, source_dir: str, zip_path: str) -> bool:
        """
        Zip a folder and its contents.
        
        Args:
            source_dir (str): Path to the source directory to zip
            zip_path (str): Path where the zip file should be created
            
        Returns:
            bool: True if successful, False otherwise
            
        Raises:
            FileNotFoundError: If source directory doesn't exist
            PermissionError: If insufficient permissions
            Exception: For other zip-related errors
        """
        try:
            source_path = Path(source_dir).resolve()
            zip_path_obj = Path(zip_path).resolve()
            
            # Validate source directory exists
            if not source_path.exists():
                logging.error(f"❌ Source directory does not exist: {source_dir}")
                raise FileNotFoundError(f"Source directory does not exist: {source_dir}")
            
            if not source_path.is_dir():
                logging.error(f"❌ Source path is not a directory: {source_dir}")
                raise ValueError(f"Source path is not a directory: {source_dir}")
            
            # Create output directory if it doesn't exist
            zip_path_obj.parent.mkdir(parents=True, exist_ok=True)
            
            total_files = 0
            with zipfile.ZipFile(zip_path_obj, 'w', zipfile.ZIP_DEFLATED) as zf:
                for file_path in source_path.rglob("*"):
                    if file_path.is_file():
                        arcname = file_path.relative_to(source_path)
                        zf.write(file_path, arcname=arcname)
                        total_files += 1
                        logging.debug(f"📁 Added file to zip: {arcname}")
            
            logging.info(f"✅ Successfully zipped {source_dir} to {zip_path} with {total_files} files")
            return True
            
        except FileNotFoundError:
            logging.error(f"❌ File not found error while zipping {source_dir}")
            raise
        except PermissionError:
            logging.error(f"❌ Permission error while zipping {source_dir}")
            raise
        except Exception as e:
            logging.error(f"❌ Unexpected error while zipping {source_dir}: {str(e)}")
            raise

    def create_dag_tasks(self):
        @task
        def backup_data_to_sharepoint_task(**context):
            """
            Zip all folders in the folder_path (folder path should be like wayfair/2025/6, this will zip folder by day).
            
            Args:
                **context: Airflow context containing dag_run configuration
                
            Returns:
                dict: Summary of zip operations performed
                
            Raises:
                ValueError: If required configuration is missing
                FileNotFoundError: If source folder doesn't exist
                Exception: For other zip-related errors
            """
            try:
                dag_run = context.get('dag_run')
                folder_path = dag_run.conf.get("source_folder")
                output_folder = dag_run.conf.get("output_folder")
                processed_folders = dag_run.conf.get("folders", [])
                # Validate required parameters
                if not folder_path:
                    logging.error("❌ source_folder is required in DAG configuration")
                    raise ValueError("source_folder is required")
                if not output_folder:
                    logging.error("❌ output_folder is required in DAG configuration")
                    raise ValueError("output_folder is required")
                
                # Validate source folder exists
                if not os.path.exists(folder_path):
                    logging.error(f"❌ Source folder does not exist: {folder_path}")
                    raise FileNotFoundError(f"Source folder does not exist: {folder_path}")
                
                logging.info(f"📁 Starting zip process for folder: {folder_path}")
                logging.info(f"📁 Output folder: {output_folder}")
                
                zipped_folders = []
                failed_folders = []
                folders = processed_folders if processed_folders else os.listdir(folder_path)
                for folder in folders:
                    folder_full_path = os.path.join(folder_path, folder)
                    
                    if os.path.isdir(folder_full_path):
                        try:
                            # Create zip filename based on folder structure
                            file_name = f"{int(folder):02d}" 
                            
                            zip_path = os.path.join(output_folder, f"{file_name}.zip")
                            
                            logging.info(f"📦 Zipping folder: {folder} -> {zip_path}")
                            success = self.zip_folder(folder_full_path, zip_path)

                            
                            if success:
                                zipped_folders.append(folder)

                                async def upload():
                                    async with httpx.AsyncClient() as client:
                                        await self.upload_file_with_retry(client, self.site_id, self.drive_id, zip_path, zip_path)

                                asyncio.run(upload())  # ✅ Gọi hàm async từ sync context

                                logging.info(f"✅ Successfully zipped and uploaded: {folder}")
                            else:
                                failed_folders.append(folder)
                                logging.error(f"❌ Failed to zip: {folder}")
                                
                        except Exception as e:
                            failed_folders.append(folder)
                            logging.error(f"❌ Error zipping folder {folder}: {str(e)}")
                
                # Log summary
                logging.info(f"📊 Zip operation summary:")
                logging.info(f"   ✅ Successfully zipped: {len(zipped_folders)} folders")
                logging.info(f"   ❌ Failed to zip: {len(failed_folders)} folders")
                
                if failed_folders:
                    logging.warning(f"⚠️ Failed folders: {', '.join(failed_folders)}")
                
                return {
                    "zipped_folders": zipped_folders,
                    "failed_folders": failed_folders,
                    "total_processed": len(zipped_folders) + len(failed_folders)
                }
                
            except Exception as e:
                logging.error(f"❌ Fatal error in zip_folder_task: {str(e)}")
                raise
                    
                    

        backup = backup_data_to_sharepoint_task()
        return backup
    
@dag(
    dag_id="bak.backup_data_to_sharepoint",
    description="Backup data to Sharepoint",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)
def backup_data_to_sharepoint_dag():
    backup_data_to_sharepoint = BackupDataToSharepoint()
    backup_data_to_sharepoint.create_dag_tasks()
instance = backup_data_to_sharepoint_dag()
        






