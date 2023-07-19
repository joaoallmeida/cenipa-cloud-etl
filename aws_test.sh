function_check=$(aws lambda get-function --function-name=cenipa_etl --region=us-east-1)

if [ ${#function_check} -gt 0 ]
then
    aws lambda update-function-code --function-name=cenipa_etl --zip-file=fileb://lambda_deploy.zip
else
    echo "Inside of else"
fi 