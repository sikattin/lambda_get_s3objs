#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
import boto3
import logging
from botocore.exceptions import BotoCoreError
from socket import gethostname

TO_ADDR = 'xxx@xxx.co.jp'

class S3Client(object):
    """Amazon S3 client for operation.
    
    Args:
        object ([type]): [description]
    """

    def __init__(self, bucket: str, logger):
        self._bucket = bucket
        self._logger = logger
        self._s3 = boto3.client('s3')
    
    def list_objs(self, filterByKey: str):
        """list objects on the specified bucket.

        Args:
            filterByKey (str): Objects filtered by the specified key.

        Returns:
            dict

        Raises:
            botocore.exceptions.BotoCoreError
        """
        paginator = self._s3.get_paginator('list_objects_v2')
        resp_iter = paginator.paginate(
            Bucket=self._bucket,
            Prefix=filterByKey
        )
        res = resp_iter.build_full_result()
        del paginator
        del resp_iter
        return res

logger = logging.getLogger()
logger.setLevel(logging.INFO)
result = dict()

def lambda_handler(event, context):
    bucket = event['bucket']
    fileter_by_prefixes = event['filter']
    mail_body = 'Bucket: {}\n\n'.format(bucket)
    try:
        s3client = boto3.client('s3')
        paginator = s3client.get_paginator('list_objects_v2')
    except BotoCoreError as e:
        logger.exception('raised unexpected error while initializing s3 uploader client.')
        logger.error(str(e))
        raise e
    else:
        # get s3 objects
        for prefix in fileter_by_prefixes:
            resp_iter = paginator.paginate(
                Bucket=bucket,
                Prefix=prefix
            )
            result = resp_iter.build_full_result()
            del resp_iter

            for v in result.values():
                for d in v:
                    mail_body += "Key: {0}, Size: {1} bytes\n".format(d["Key"], d["Size"])
        mail_body += "\nend\n"
        subject = 'mail subject'

        result = {
            "Result": {
                "subject": subject,
                "to_addr": TO_ADDR,
                "body": mail_body,
                "status_code": 200
            }
        }
        return result
