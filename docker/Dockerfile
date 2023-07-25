FROM public.ecr.aws/lambda/python:3.10

# Copy requirements.txt
COPY requirements.txt .

# Copy function code
COPY etl ./etl
COPY events ./events
COPY lambda_function.py .

# Install the specified packages
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ]