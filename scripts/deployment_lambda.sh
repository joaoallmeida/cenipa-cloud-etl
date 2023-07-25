#!/bin/bash

function_name="cenipa-etl"
aws_region="us-east-1"
s3_bucket="cenipa.etl.com.br"


# Creating functions for create or update Lambda Fucntion
function_exists() {
    aws lambda get-function --function-name "$function_name" --region "$aws_region" >/dev/null 2>&1
}

update_function() {
    echo "Updating Lambda $function_name"
    
    aws lambda update-function-code \
    --function-name "$function_name" \
    --image-uri 400582553708.dkr.ecr.us-east-1.amazonaws.com/cenipa-etl:latest \
    --region "$aws_region"
}

create_function() {
    echo "Creating Lambda $function_name"
    aws lambda create-function \
        --function-name "$function_name" \
        --package-type Image \
        --role arn:aws:iam::400582553708:role/service-role/cenipa-etl-role-o11blh01 \
        --timeout 600 \
        --memory-size 512 \
        --environment Variables="{s3_bucket=$s3_bucket}" \
        --code ImageUri=400582553708.dkr.ecr.us-east-1.amazonaws.com/cenipa-etl:latest \
        --region "$aws_region" \
        --tags '{"Environment":"Dev", "Name":"ETL-Cenipa"}'
}

if function_exists; then
    update_function
else
    create_function
fi
