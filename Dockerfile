# Use the AWS Lambda Python runtime as a base image
FROM public.ecr.aws/lambda/python:3.9-arm64

# Copy function code and requirements file
COPY app.py ./ 
COPY requirements.txt ./

# Install the function's dependencies using requirements.txt
RUN pip install -r requirements.txt

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD ["app.lambda_handler"]