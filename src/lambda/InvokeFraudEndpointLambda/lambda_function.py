import os
import json
import base64
import logging
import time
from datetime import datetime


# Read environment variables
ENDPOINT_NAME = os.environ['ENDPOINT_NAME']
CC_AGG_FEATURE_GROUP = os.environ['CC_AGG_FEATURE_GROUP_NAME']
CC_AGG_BATCH_FEATURE_GROUP = os.environ['CC_AGG_BATCH_FEATURE_GROUP_NAME']
FRAUD_THRESHOLD = os.environ['FRAUD_THRESHOLD']
LOG_LEVEL = os.environ['LOG_LEVEL']

# Constants
TEN_MINUTES_IN_SEC = 10 * 60

logger = logging.getLogger()
if logging._checkLevel(LOG_LEVEL):
    logger.setLevel(LOG_LEVEL)
else:
    logger.setLevel(logging.INFO)

logging.info(f'Setting Logger Level to {logging.getLevelName(logger.level)}')

import boto3

print(f'boto3 version: {boto3.__version__}')
# Create session via Boto3
session = boto3.session.Session()

try:
    featurestore_runtime = boto3.Session().client(service_name='sagemaker-featurestore-runtime')
except:
    logging.error('Failed to instantiate featurestore-runtime client with install.sh script!')

# Allocate SageMaker runtime
sagemaker_runtime = boto3.client('runtime.sagemaker')

logging.info(f'Lambda will call SageMaker ENDPOINT name: {ENDPOINT_NAME}')


def lambda_handler(event, context):
    """ This handler is triggered by incoming Kinesis events,
    which contain a payload encapsulating the transaction data.
    The Lambda will then lookup corresponding records in the
    aggregate feature groups, assemble a payload for inference,
    and call the inference endpoint to generate a prediction.
    """
    logging.debug('Received event: {}'.format(json.dumps(event, indent=2)))

    records = event['Records']
    logging.debug('Event contains {} records'.format(len(records)))
    
    ret_records = []
    for rec in records:
        # Each record has separate eventID, etc.
        event_id = rec['eventID']
        event_source_arn = rec['eventSourceARN']
        logging.debug(f'eventID: {event_id}, eventSourceARN: {event_source_arn}')

        kinesis = rec['kinesis']
        event_payload = decode_payload(kinesis['data'])

        # Collect fields from event payload
        cc_num = event_payload['cc_num']
        amount = event_payload['amount']
        logging.info(f'Event payload data: cc_num: {cc_num} transaction amount: {amount} ')

        if 'trans_ts' in event_payload:
            trans_ts = event_payload['trans_ts']
            calc_trans_time_delay(trans_ts)

        aggregate_dict, cutoff_condition = lookup_features(cc_num, amount)
        if aggregate_dict is None:
            continue

        feature_string = assemble_features(amount, aggregate_dict)
        prediction = invoke_endpoint(feature_string, cc_num)
        
        dump_stats(prediction, cc_num, amount, aggregate_dict, cutoff_condition)
        
        if prediction is not None:
            sequence_num = kinesis['sequenceNumber']
            ret_records.append({'eventId': event_id,
                            'sequenceNumber': sequence_num,
                            'prediction': prediction,
                            'statusCode': 200})

    return ret_records


def decode_payload(event_data):
    agg_data_bytes = base64.b64decode(event_data)
    decoded_data = agg_data_bytes.decode(encoding="utf-8") 
    event_payload = json.loads(decoded_data) 
    logging.info(f'Decoded data from kinesis record: {event_payload}')
    return event_payload


def calc_trans_time_delay(trans_ts):
    # Let's calculate end-to-end time from transaction time to now
    trans_time = datetime.utcfromtimestamp(int(trans_ts)).strftime('%Y-%m-%d %H:%M:%S')
    now = time.time()
    ts_diff = now - trans_ts
    logging.info(f'Payload timing: trans time: {trans_time} ts_diff: {ts_diff} sec')


def lookup_features(credit_card_number, transaction_amount):
    combined_agg_features = {}

    # Initialize agg feature values
    cc_num = '1111111111111111'
    avg_amt_last_10m = 0
    num_trans_last_10m = 0
    avg_amt_last_1w = 0
    num_trans_last_1w = 0

    # Retrieve first set of aggregated features
    cc_agg_feature_group = CC_AGG_FEATURE_GROUP
    feature_names = ['cc_num', 'avg_amt_last_10m', 'num_trans_last_10m', 'trans_time']
    agg_features = retrieve_aggregated_features(cc_agg_feature_group, credit_card_number, feature_names)

    agg_evt_time = 0.0

    if agg_features is not None:
        cc_num = agg_features['cc_num']
        avg_amt_last_10m = float(agg_features['avg_amt_last_10m'])
        num_trans_last_10m = int(agg_features['num_trans_last_10m'])
        agg_evt_time = float(agg_features['trans_time'])
        # store agg features in dict
        combined_agg_features.update(agg_features)

    logging.debug(f'In> lookup_features: #1 Agg Features: \n {combined_agg_features}')

    # Check cutoff condition before calling second Feature Group
    cutoff_condition = eval_cutoff_window(agg_evt_time)

    # Retrieve second set of aggregated features
    batch_agg_features = None
    if not cutoff_condition:
        # Retrieve data from 'batch' agg feature group
        cc_agg_batch_feature_group = CC_AGG_BATCH_FEATURE_GROUP
        feature_names = ['cc_num', 'num_trans_last_1w', 'avg_amt_last_1w']
        batch_agg_features = retrieve_aggregated_features(cc_agg_batch_feature_group, credit_card_number, feature_names)

    if batch_agg_features is not None:
        num_trans_last_1w = int(batch_agg_features['num_trans_last_1w'])
        avg_amt_last_1w = float(batch_agg_features['avg_amt_last_1w'])
        # store batch agg features in dict
        combined_agg_features.update(batch_agg_features)

    logging.debug(f'In> lookup_features: #2 Agg Features: \n {combined_agg_features}')

    amt_ratio1, amt_ratio2, count_ratio = calc_ratios_for_inference(
        transaction_amount, avg_amt_last_10m, avg_amt_last_1w,
        num_trans_last_10m, num_trans_last_1w, cutoff_condition)

    # Append ratios into dict
    combined_agg_features['amt_ratio1'] = amt_ratio1
    combined_agg_features['amt_ratio2'] = amt_ratio2
    combined_agg_features['count_ratio'] = count_ratio

    logging.debug(f'In> lookup_features: #3 Agg Features: \n {combined_agg_features}')
    return combined_agg_features, cutoff_condition


def calc_ratios_for_inference(
        transaction_amount,
        avg_amt_last_10m,
        avg_amt_last_1w,
        num_trans_last_10m,
        num_trans_last_1w,
        cutoff_condition ):

    # Initialize ratios
    amt_ratio1 = 0.0
    amt_ratio2 = 0.0
    count_ratio = 0.0

    # Check for zero-fill conditions: zero denominator, time cutoff
    if (avg_amt_last_1w > 0) and (num_trans_last_1w > 0) and (not cutoff_condition):
        # Calculate Ratios to be passed to Inference
        amt_ratio1 = avg_amt_last_10m / avg_amt_last_1w
        amt_ratio2 = transaction_amount / avg_amt_last_1w
        count_ratio = num_trans_last_10m / num_trans_last_1w

    logging.debug(f'Ratios: {amt_ratio1} {amt_ratio2} {count_ratio}')
    return amt_ratio1, amt_ratio2, count_ratio


def assemble_features( transaction_amount, combined_agg_features ):
    inference_features = []

    # Pull ratios from dict
    amt_ratio1 = combined_agg_features['amt_ratio1']
    amt_ratio2 = combined_agg_features['amt_ratio2']
    count_ratio = combined_agg_features['count_ratio']

    # Assemble all feature values for inference
    inference_features.append(str(transaction_amount))
    inference_features.append(str(amt_ratio1))
    inference_features.append(str(amt_ratio2))
    inference_features.append(str(count_ratio))

    logging.debug(f'Inference features: {inference_features}')

    # assemble features into CSV-format string
    feature_string = ','.join(inference_features)

    return feature_string


def eval_cutoff_window(agg_evt_time):
    # Test if aggregate event time has been too long ago
    cutoff_condition = False
    now = time.time()
    cutoff_time = now - agg_evt_time
    logging.debug(f'Calculate time diff as: {now} - {agg_evt_time} => {cutoff_time}')

    if cutoff_time > TEN_MINUTES_IN_SEC:
        # assume current agg features are all ZERO
        cutoff_condition = True

    logging.info(f'cutoff_condition: {cutoff_condition} based on agg_evt_time: {agg_evt_time} and cutoff_time: {cutoff_time}')
    return cutoff_condition


def retrieve_aggregated_features(feature_group_name, feature_group_identifier, feature_key_list):
    feature_vector = {}
        
    # Set Feature Group record_identifier which is actually the 'cc_num' field 
    record_identifier = str(int(feature_group_identifier))

    # Call Yavapai Runtime to retrieve records from online feature store
    transaction_record = featurestore_get_record(feature_group_name, record_identifier)
    
    # If no record is returned, no need to extract items
    if transaction_record is None:
        logging.error(f'FeatureStore Group {feature_group_name} did NOT return record for identifier {feature_group_identifier}')
        return None
        
    logging.info('Retrieved Record from FeatureStore: {}'.format(json.dumps(transaction_record)))
    
    for key in feature_key_list:
        val = get_feature_value(transaction_record, key)
        logging.debug(f'Agg Record: key: {key} value: {val}')
        if val is not None:
            feature_vector[key] = val

    logging.info(f'Resulting feature_vector dict: {feature_vector}')
    
    return feature_vector


# Helper to parse the feature value from the record.
def get_feature_value(record, feature_name):
    return str(list(filter(lambda r: r['FeatureName'] == feature_name, record))[0]['ValueAsString'])


def featurestore_get_record(feature_group_name, record_identifier_value):
    response = featurestore_runtime.get_record(
        FeatureGroupName=feature_group_name, RecordIdentifierValueAsString=record_identifier_value)
    logging.debug('FeatureStore: get_record ResponseMetadata: {}'.format(response['ResponseMetadata']))

    return_record = None
    if 'Record' in response:
        return_record = response['Record']
    else:
        logging.error('No Record found in FeatureStore response!!!')
    
    response_status_code = response['ResponseMetadata']['HTTPStatusCode']
    if (response_status_code == 200) and (return_record is not None):
        # record = response['Record']
        logging.info('Record found: {}'.format(return_record))
        return return_record


def invoke_endpoint(request_body, credit_card_number):
    logging.debug('Passing Request Body (CSV-format): {}'.format(request_body))
    
    # Format of inference payload (CSV format)
    # TRANSACTION_AMT_IN_DOLLARS, AMT_RATIO_1, AMT_RATIO_2, COUNT_RATIO
    # payload='68.17,0.1085,0.1085,0.0333'
    
    response = sagemaker_runtime.invoke_endpoint(
        EndpointName=ENDPOINT_NAME,
        ContentType='text/csv',
        Body=request_body)        
    logging.info('Inference Response: {}'.format(response))

    probability = json.loads(response['Body'].read().decode('utf-8'))
    return probability


def dump_stats(probability, credit_card_number, transaction_amount, aggregate_dict, cutoff_condition):
    # extract features from aggregate dict
    avg_amt_last_10m = aggregate_dict['avg_amt_last_10m'] if 'avg_amt_last_10m' in aggregate_dict.keys() else 0.0
    num_trans_last_10m = aggregate_dict['num_trans_last_10m'] if 'num_trans_last_10m' in aggregate_dict.keys() else 0
    agg_evt_time = aggregate_dict['trans_time'] if 'trans_time' in aggregate_dict.keys() else 0.0

    # extract features from batch agg dict
    num_trans_last_1w = aggregate_dict['num_trans_last_1w'] if 'num_trans_last_1w' in aggregate_dict.keys() else 0
    avg_amt_last_1w = aggregate_dict['avg_amt_last_1w'] if 'avg_amt_last_1w' in aggregate_dict.keys() else 0.0

    amt_ratio1 = aggregate_dict['amt_ratio1']
    amt_ratio2 = aggregate_dict['amt_ratio2']
    count_ratio = aggregate_dict['count_ratio']

    if probability > float(FRAUD_THRESHOLD):
        fraud_text = 'FRAUD'
    else:
        fraud_text = 'NOT FRAUD'
    
    if cutoff_condition:
        print(
            f'Prediction: {fraud_text} ({probability:.6f}), ' +
            f'card num: {credit_card_number}, ' +
            f'amount: {transaction_amount:.2f} '
        )
    else:
        print(
            f'Prediction: {fraud_text} ({probability:.6f}), ' +
            f'num last 10m: {num_trans_last_10m}, ' +
            f'avg amt last 10m: {avg_amt_last_10m}, ' +
            f'card num: {credit_card_number}, ' +
            f'amount: {transaction_amount:.2f} '
        )

