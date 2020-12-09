## My Project

### Overview:
In this repository,  we provide artifacts that demonstrate how to leverage the Amazon SageMaker Feature Store using a streaming aggregation use case for Fraud Detection in credit card transactions. We use Amazon SageMaker to train a model (using the built-in XGBoost algorithm) with aggregate features created from historical credit card transactions. We use streaming aggregation with Amazon Kinesis Data Analytics (KDA) SQL, publishing features in near real time to the feature store. Finally, we use inference to detect fraud on new transactions. For a full explanation of SageMaker Feature Store you can read here (https://aws.amazon.com/sagemaker/feature-store/), which describes the capability as:

Amazon SageMaker Feature Store is a purpose-built repository where you can store and access features so it’s much easier to name, organize, and reuse them across teams. SageMaker Feature Store provides a unified store for features during training and real-time inference without the need to write additional code or create manual processes to keep features consistent.


This implementation shows how to do the following:

* Deploy AWS Lambda functions, IAM Roles, and SageMaker notebook instance using CloudFormation template
* Create multiple SageMaker Feature Groups to store aggregate data from a credit card dataset
* Run a SageMaker Processing Spark job to aggregate raw features and derive new features for model training
* Train a SageMaker XGBoost model and deploy it as an endpoint for real time inference
* Generate simulated credit card transactions sending them to Amazon Kinesis 
* Using KDA SQL to aggregate features in near real time, triggering a Lambda function to update feature values in an online-only feature group
* Trigger a Lambda function to invoke the SageMaker endpoint and detect fraudulent transactions

### Prerequisites

Prior to running the steps under Instructions, you will need the following:

1. AWS Account in us-east-2 region where you have full Admin privileges
2. S3 Bucket where you can upload the Lambda code zipfiles (2)

In addition, having basic knowledge of the following services will be valuable: Amazon Kinesis streams, Amazon Kinesis Data Analytics, Amazon SageMaker, AWS Lambda functions, Amazon IAM Roles.

#### Instructions

First you will login to your AWS account with an Admin user or role. This will allow the successful launch of the CloudFormation stack template below.  

You can deploy the stack in us-east-2 region and examine the architectural components by following these steps:

1. Use the following button to launch the AWS CloudFormation stack
2. Once the stack is complete, open the SageMaker Notebook instance and navigate to the various notebooks
3. View the Kinesis Stream that is used to ingest records
4. View the Kinesis Data Analytics SQL query that pulls data from the stream
5. View the Lambda function that receives the initial kinesis events and writes to the FeatureStore
6. View the Lambda function that receives the final kinesis events and triggers the model prediction

You can view the CloudFormation template directly by looking in the cloud-formation directory above and clicking on the file name sagemaker-featurestore-template.yaml   The stack will take a few minutes to launch; it will create a new S3 Bucket, create two new Lambda functions, create several IAM Roles and Policies, and create a SageMaker Notebook instance. When it completes, you can view the items created by clicking on the Resources tab:


#### Running the Notebooks

There are a series of six notebooks which should be run in order (they each begin with a numeral). Each notebook achieves a certain purpose using the SageMaker Python SDK (add link here) to interact with S3 buckets, Amazon Kinesis, Lambda functions, and the SageMaker FeatureStore. We use the simulated credit card fraud dataset, which has certain attributes, like credit_card_number, transaction_amount, etc. We then create aggregate data and store it in the Feature Store, including these attributes that represent aggregate averages and counts over 10-minute and 1-week timeframes: 

* num_trans_last_10m
* num_trans_last_1w
* avg_amt_last_10m
* avg_amt_last_1w


Follow the step-by-step guide by executing the notebooks in the following folders:

* notebooks/0_prepare_transactions_dataset.ipynb
* notebooks/1_setup.ipynb
* notebooks/2_batch_ingestion.ipynb
* notebooks/3_train_and_deploy_model.ipynb
* notebooks/4_streaming_predictions.ipynb
* notebooks/5_cleanup.ipynb

##### Current limitations

This solution does not utilize the SageMaker FeatureStore OFFLINE store. The use case focuses only on the ONLINE store which is leveraged for low-latency consumers.




## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.


#### TODO:
* Check title for correct format 
* Edit repo desc on GitHub