#!/bin/bash

ACCOUNT_ID=$1
STATE_MACHINE_NAME="cenipa-etl-orchestration"

# Creating functions for create or update State Machine
state_machine_exists() {
    aws stepfunctions describe-state-machine --state-machine-arn "arn:aws:states:$REGION:$ACCOUNT_ID:stateMachine:$STATE_MACHINE_NAME" 2>/dev/null
}

update_state_machine() {
    echo "Updating State Machine $STATE_MACHINE_NAME"
    
    aws stepfunctions update-state-machine \
        --state-machine-arn "arn:aws:states:$REGION:$ACCOUNT_ID:stateMachine:$STATE_MACHINE_NAME" \
        --definition "$( cat ./automation/templates/step_function_template.json )"
}

create_state_machine() {
    echo "Creating State Machine $STATE_MACHINE_NAME"

    aws stepfunctions create-state-machine \
        --name $STATE_MACHINE_NAME\
        --role arn:aws:iam::$ACCOUNT_ID:role/step_function_role \
        --definition "$( cat ./automation/templates/step_function_template.json )"
}

if state_machine_exists; then
    update_state_machine
else
    create_state_machine
fi