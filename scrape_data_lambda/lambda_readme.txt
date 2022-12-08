# Install the required packages in the same flat folder as the lambda
pip3 install boto3 pendulum requests -t .

# Zip the contents of the folder. The root folder can't be nested and should look like this:
scrape_data_lambda.zip:
* lambda_function.py
* bin
* boto3
* botocore
* requests
...
* urllib

# Upload the zip to the lambda. 
# Set it to fire at whatever rate is appropriate (Every 5 minutes for now)