import json
import base64
import subprocess
import os
import sys
from datetime import datetime
import time

import boto3

print(f'boto3 version: {boto3.__version__}')

try:
    sm = boto3.Session().client(service_name='sagemaker')
    sm_fs = boto3.Session().client(service_name='sagemaker-featurestore-runtime')
except:
    print(f'Failed while connecting to SageMaker Feature Store')
    print(f'Unexpected error: {sys.exc_info()[0]}')


# Read Environment Vars
CC_AGG_FEATURE_GROUP = os.environ['CC_AGG_FEATURE_GROUP_NAME']


def update_agg(fg_name, cc_num, avg_amt_last_10m, num_trans_last_10m):
    record = [{'FeatureName':'cc_num', 'ValueAsString': str(cc_num)},
              {'FeatureName':'avg_amt_last_10m', 'ValueAsString': str(avg_amt_last_10m)},
              {'FeatureName':'num_trans_last_10m', 'ValueAsString': str(num_trans_last_10m)},
              {'FeatureName':'trans_time', 'ValueAsString': str(int(round(time.time())))} #datetime.now().isoformat()} #
             ]
    sm_fs.put_record(FeatureGroupName=fg_name, Record=record)
    return
        
def lambda_handler(event, context):
    inv_id = event['invocationId']
    app_arn = event['applicationArn']
    records = event['records']
    print(f'Received {len(records)} records, invocation id: {inv_id}, app arn: {app_arn}')
    
    ret_records = []
    for rec in records:
        data = rec['data']
        agg_data_str = base64.b64decode(data) 
        agg_data = json.loads(agg_data_str)
        
        cc_num = agg_data['cc_num']
        num_trans_last_10m = agg_data['num_trans_last_10m']
        avg_amt_last_10m = agg_data['avg_amt_last_10m']

        print(f' updating agg features for card: {cc_num}, avg amt last 10m: {avg_amt_last_10m}, num trans last 10m: {num_trans_last_10m}')
        update_agg(CC_AGG_FEATURE_GROUP, cc_num, avg_amt_last_10m, num_trans_last_10m)
        
        # Flag each record as being "Ok", so that Kinesis won't try to re-send 
        ret_records.append({'recordId': rec['recordId'],
                            'result': 'Ok'})
    return {'records': ret_records}