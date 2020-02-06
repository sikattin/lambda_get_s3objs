#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
import time
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
        
def check_size(src, dest):
    if not isinstance(src, int) or not isinstance(dest, int):
        raise ValueError('must be int')
        
    if src == dest:
        return 'OK'
    else:
        return 'NG'
        
def get_s3objsiter(bucket, prefix=None):
    if prefix is None:
        prefix = ''
    resp_iter = paginator.paginate(
        Bucket=bucket,
        Prefix=prefix
    )
    return resp_iter
    
def construct_mailbody(iter):
    """construct mailbody
    
    Args:
        iter (botocore.paginate.PageIterator): PageIterator object
    
    Returns:
        str: mail body
    """
    global bucket
    global s3client
    key = str()
    size = int()
    result = str()
    for ele in iter:
        if ele['Contents']:
            for d in ele['Contents']:
                chksize_res = 'Not defined'
                key = d.get('Key')
                size = int(d.get('Size'))
                res = s3client.get_object(Bucket=bucket, Key=key)
                if 'Metadata' in res and 'src_size' in res['Metadata']:
                    chksize_res = check_size(int(res['Metadata']['src_size']), size)
                result += "{0} {1} {2}\n".format(key, size, chksize_res)
        else:
            result += "!!! Not found objects in S3 bucket. " \
            "Check S3 bucket then make sure that backup source has been transfered correctly. !!!"
    return result
    
logger = logging.getLogger()
logger.setLevel(logging.INFO)
bucket = str()
s3client = boto3.client('s3')
paginator = s3client.get_paginator('list_objects_v2')
result = dict()

def lambda_handler(event, context):
    global bucket
    global s3client
    global paginator
    fileter_by_prefixes = list()

    bucket = event['bucket']
    if 'filter' in event:
        # list of filters to get/list objects.
        fileter_by_prefixes = event['filter']
    body_header = "Bucket: {}\n\n".format(bucket)
    body_header += 'Key Size(bytes) Result\n'
    body_header += '-----------------------------'
    try:
        paginator = s3client.get_paginator('list_objects_v2')
    except BotoCoreError as e:
        logger.exception('raised unexpected error while initializing s3 uploader client.')
        logger.error(str(e))
        raise e
    if fileter_by_prefixes:
        # get s3 objects
        for prefix in fileter_by_prefixes:
            s3objsiter = get_s3objsiter(bucket, prefix=prefix)
            mail_body = construct_mailbody(s3objsiter)
    else:
        s3objsiter = get_s3objsiter(bucket)
        mail_body = construct_mailbody(s3objsiter)

    mail_body += "\nend\n"
    body = body_header + mail_body
    subject = 'mail subject'

    result = {
        "Result": {
            "subject": subject,
            "to_addr": TO_ADDR,
            "body": body,
            "status_code": 200
        }
    }
    return result
