import boto3
import gzip as gz
import os
import shutil
from copy import deepcopy
from io import BytesIO
from pathlib import Path
import yaml
from botocore.exceptions import ClientError
from boto3.s3.transfer import S3Transfer, TransferConfig
from mypy_boto3_s3.service_resource import (
    Object as S3ResourceObject,
)
import logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler()]
                    )
with open('config/s3.yaml', 'r') as f:
    config = yaml.safe_load(f)
AWS_ACCESS_KEY_ID = config['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = config['AWS_SECRET_ACCESS_KEY']
AWS_REGION = config['REGION_NAME']
BUCKET_NAME = config['BUCKET_NAME']


class S3Hook:
    def __init__(
        self,
        transfer_config_args: dict | None = None,
        extra_args: dict | None = None,
    ):
        self.client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION,
        )

        self.resource = boto3.resource(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION,
        )
        
        self.bucket_name = BUCKET_NAME

        if transfer_config_args and not isinstance(transfer_config_args, dict):
            raise TypeError(
                f"transfer_config_args expected dict, got {type(transfer_config_args).__name__}."
            )
        self.transfer_config = TransferConfig(**transfer_config_args or {})

        if extra_args and not isinstance(extra_args, dict):
            raise TypeError(
                f"extra_args expected dict, got {type(extra_args).__name__}."
            )
        self._extra_args = extra_args or {}

    @property
    def extra_args(self):
        """Return hook's extra arguments (immutable)."""
        return deepcopy(self._extra_args)

    def load_file(
        self,
        filename: Path | str,
        key: str,
        bucket_name: str | None = None,
        replace: bool = False,
        encrypt: bool = False,
        gzip: bool = False,
        acl_policy: str | None = None,
    ) -> None:
        """
        Load a local file to S3.

        .. see also::
            - :external+boto3:py:meth:`S3.Client.upload_file`

        :param filename: path to the file to load.
        :param key: S3 key that will point to the file
        :param bucket_name: Name of the bucket in which to store the file
        :param replace: A flag to decide whether or not to overwrite the key
            if it already exists. If replace is False and the key exists, an
            error will be raised.
        :param encrypt: If True, the file will be encrypted on the server-side
            by S3 and will be stored in an encrypted form while at rest in S3.
        :param gzip: If True, the file will be compressed locally
        :param acl_policy: String specifying the canned ACL policy for the file being
            uploaded to the S3 bucket.
        """
        filename = str(filename)
        if not replace and self.check_for_key(key, bucket_name):
            raise ValueError(f"The key {key} already exists.")

        extra_args = self.extra_args
        if encrypt:
            extra_args["ServerSideEncryption"] = "AES256"
        if gzip:
            with open(filename, "rb") as f_in:
                filename_gz = f"{f_in.name}.gz"
                with gz.open(filename_gz, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
                    filename = filename_gz
        if acl_policy:
            extra_args["ACL"] = acl_policy

        client = self.client
        client.upload_file(
            filename,
            bucket_name,
            key,
            ExtraArgs=extra_args,
            Config=self.transfer_config,
        )
        logging.info(f"File uploaded to {bucket_name}/{key}")

    def load_string(
        self,
        string_data: str,
        key: str,
        bucket_name: str | None = None,
        replace: bool = False,
        encrypt: bool = False,
        encoding: str | None = None,
        acl_policy: str | None = None,
        compression: str | None = None,
    ) -> None:
        """
        Load a string to S3.

        This is provided as a convenience to drop a string in S3. It uses the
        boto infrastructure to ship a file to s3.

        .. see also::
            - :external+boto3:py:meth:`S3.Client.upload_fileobj`

        :param string_data: str to set as content for the key.
        :param key: S3 key that will point to the file
        :param bucket_name: Name of the bucket in which to store the file
        :param replace: A flag to decide whether or not to overwrite the key
            if it already exists
        :param encrypt: If True, the file will be encrypted on the server-side
            by S3 and will be stored in an encrypted form while at rest in S3.
        :param encoding: The string to byte encoding
        :param acl_policy: The string to specify the canned ACL policy for the
            object to be uploaded
        :param compression: Type of compression to use, currently only gzip is supported.
        """
        encoding = encoding or "utf-8"

        bytes_data = string_data.encode(encoding)

        # Compress string
        available_compressions = ["gzip"]
        if compression is not None and compression not in available_compressions:
            raise NotImplementedError(
                f"Received {compression} compression type. "
                f"String can currently be compressed in {available_compressions} only."
            )
        if compression == "gzip":
            bytes_data = gz.compress(bytes_data)

        with BytesIO(bytes_data) as f:
            self._upload_file_obj(f, key, bucket_name, replace, encrypt, acl_policy)

    def _upload_file_obj(
        self,
        file_obj: BytesIO,
        key: str,
        bucket_name: str | None = None,
        replace: bool = False,
        encrypt: bool = False,
        acl_policy: str | None = None,
    ) -> None:
        if not replace and self.check_for_key(key, bucket_name):
            raise ValueError(f"The key {key} already exists.")

        extra_args = self.extra_args
        if encrypt:
            extra_args["ServerSideEncryption"] = "AES256"
        if acl_policy:
            extra_args["ACL"] = acl_policy

        client = self.client
        client.upload_fileobj(
            file_obj,
            bucket_name,
            key,
            ExtraArgs=extra_args,
            Config=self.transfer_config,
        )

    def check_for_key(self, key: str, bucket_name: str | None = None) -> bool:
        """
        Check if a key exists in a bucket.

        .. see also::
            - :external+boto3:py:meth:`S3.Client.head_object`

        :param key: S3 key that will point to the file
        :param bucket_name: Name of the bucket in which the file is stored
        :return: True if the key exists and False if not.
        """
        obj = self.head_object(key, bucket_name)
        return obj is not None

    def head_object(self, key: str, bucket_name: str | None = None) -> dict | None:
        """
        Retrieve metadata of an object.

        .. see also::
            - :external+boto3:py:meth:`S3.Client.head_object`

        :param key: S3 key that will point to the file
        :param bucket_name: Name of the bucket in which the file is stored
        :return: metadata of an object
        """
        try:
            return self.client.head_object(Bucket=bucket_name, Key=key)
        except ClientError as e:
            if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return None
            else:
                raise e

    def read_key(self, key: str, bucket_name: str | None = None) -> str:
        """
        Read a key from S3.

        .. seealso::
            - :external+boto3:py:meth:`S3.Object.get`

        :param key: S3 key that will point to the file
        :param bucket_name: Name of the bucket in which the file is stored
        :return: the content of the key
        """
        obj = self.get_key(key, bucket_name)
        return obj.get()["Body"].read().decode("utf-8")

    def get_key(self, key: str, bucket_name: str | None = None) -> S3ResourceObject:
        """
        Return a :py:class:`S3.Object`.

        .. seealso::
            - :external+boto3:py:meth:`S3.ServiceResource.Object`

        :param key: the path to the key
        :param bucket_name: the name of the bucket
        :return: the key object from the bucket
        """

        def sanitize_extra_args() -> dict[str, str]:
            """Parse extra_args and return a dict with only the args listed in ALLOWED_DOWNLOAD_ARGS."""
            return {
                arg_name: arg_value
                for (arg_name, arg_value) in self.extra_args.items()
                if arg_name in S3Transfer.ALLOWED_DOWNLOAD_ARGS
            }

        obj = self.resource.Object(bucket_name, key)
        obj.load(**sanitize_extra_args())
        return obj
