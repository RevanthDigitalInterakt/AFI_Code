from fastapi import APIRouter, HTTPException
import boto3
import requests
import json

router = APIRouter()

# Constants
SQS_QUEUE_URL = "https://sqs.eu-north-1.amazonaws.com/062314917923/Payload_Queue"
MOENGAGE_API_URL = "https://api.moengage.com/"
RETRY_LIMIT = 3  # Retry limit for each payload
S3_BUCKET_NAME = "your-s3-bucket-name"  #########################################################
token_moe = "your-token"  # Replace with actual token #############################################


def retry_request(payload, headers, retries=0):
    """
    Retries a request to MoEngage API up to a specified retry limit.

    Args:
    - payload (dict): The payload to send.
    - headers (dict): Headers for the request.
    - retries (int): Current retry attempt.

    Returns:
    - bool: True if the request succeeds, False otherwise.
    """
    try:
        response = requests.post(MOENGAGE_API_URL, json=payload, headers=headers)

        if response.status_code == 200:
            print(f"Successfully retried payload: {payload}")
            return True
        else:
            print(f"Failed to retry payload: {payload}, Error: {response.text}")
            raise Exception(response.text)

    except Exception as e:
        if retries < RETRY_LIMIT - 1:
            print(f"Retry attempt {retries + 1} failed. Retrying...")
            return retry_request(payload, headers, retries + 1)
        else:
            print(f"Max retries reached for payload: {payload}, Error: {str(e)}")
            return False


@router.get("/retry")
async def retry_failed_payloads_from_sqs():
    sqs = boto3.client('sqs')

    try:
        while True:
            # Receive messages from SQS
            response = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=10
            )

            # If no messages are found, break the loop
            if 'Messages' not in response:
                print("No more messages to process.")
                break

            for message in response['Messages']:
                try:
                    # Inspect the raw body before parsing
                    raw_body = message['Body']
                    print(f"Raw message body: {raw_body}")

                    # Attempt to parse the message body
                    try:
                        payload = json.loads(raw_body)
                    except json.JSONDecodeError as e:
                        print(f"Invalid JSON in message body: {raw_body}, Error: {str(e)}")
                        continue

                    # Retry sending the payload to MoEngage
                    headers = {
                        'Authorization': token_moe,
                        'Content-Type': 'application/json',
                        'MOE-APPKEY': '6978DCU8W19J0XQOKS7NEE1C_DEBUG'
                    }

                    success = retry_request(payload, headers)

                    if success:
                        # Delete the message from the queue upon success
                        sqs.delete_message(
                            QueueUrl=SQS_QUEUE_URL,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        print("Message deleted from SQS.")
                    else:
                        print(f"Failed to process message after {RETRY_LIMIT} retries.")

                except Exception as e:
                    print(f"Error processing message: {str(e)}")
                    # Optionally, log the error and leave the message in SQS for another retry

    except Exception as e:
        error_message = f"Error while retrying failed payloads from SQS: {str(e)}"
        print(error_message)
        raise HTTPException(status_code=500, detail=error_message)
