#!/bin/bash

function_name="cenipa-etl"
aws_region="us-east-1"
s3_bucket="cenipa.etl.com.br"

# Building Local Environment
mkdir venv
python -m pip install --upgrade pip setuptools wheel
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
deactivate

# Creating Zip File
cp -r venv/lib/python3.10/site-packages/. ./
zip -r -9 "lambda_deploy.zip" . -x '*venv*' '*.git*'

# Print local folder and contents
pwd
ls -las

# Creating functions for create or update Lambda Fucntion
function_exists() {
    aws lambda get-function --function-name "$function_name" --region "$aws_region" >/dev/null 2>&1
}

update_function() {
    echo "Updating Lambda $function_name"
    aws lambda update-function-code --function-name "$function_name" --zip-file=fileb://lambda_deploy.zip --region "$aws_region"
}

create_function() {
    echo "Creating Lambda $function_name"
    aws lambda create-function \
        --function-name "$function_name" \
        --runtime python3.10 \
        --handler lambda_function.lambda_handler \
        --role arn:aws:iam::400582553708:role/service-role/cenipa-etl-role-o11blh01 \
        --timeout 600 \
        --memory-size 512 \
        --vpc-config "{"SubnetIds": ["subnet-094736222d17ac479", "subnet-0b2b1239195367b1a"], "SecurityGroupIds": ["sg-0ebd6e8c7c844104d"]}" \
        --environment Variables="{s3_bucket=$s3_bucket}" \
        --zip-file=fileb://lambda_deploy.zip \
        --region "$aws_region"
}

if function_exists; then
    update_function
else
    create_function
fi
