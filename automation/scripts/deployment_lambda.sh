#!/bin/bash

ACCOUNT_ID=$1
DELETE=$2 
FUNCTION_NAME="cenipa-etl"
BUCKET="cenipa.etl.com.br"


# Creating functions for create or update Lambda Fucntion
function_exists() {
    aws lambda get-function --function-name "$FUNCTION_NAME" >/dev/null 2>&1
}

update_function() {
    echo "Updating Lambda $FUNCTION_NAME"
    
    aws lambda update-function-code \
    --function-name "$FUNCTION_NAME" \
    --image-uri $ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/cenipa-etl:latest 
}

create_function() {
    echo "Creating Lambda $FUNCTION_NAME"
    aws lambda create-function \
        --function-name "$FUNCTION_NAME" \
        --package-type Image \
        --role arn:aws:iam::$ACCOUNT_ID:role/lambda_role \
        --timeout 600 \
        --memory-size 512 \
        --environment Variables="{s3_bucket=$BUCKET}" \
        --code ImageUri=$ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/cenipa-etl:latest \
        --tags '{"Environment":"Dev", "Name":"ETL-Cenipa"}'
}

delete_function() {
    echo "Deleting Lambda $FUNCTION_NAME"
    aws lambda delete-function --function-name "$FUNCTION_NAME"
}

if function_exists; then
    if [ "$DELETE" -eq 1 ]; then
        delete_function
    else
        update_function
    fi
else
    create_function
fi
