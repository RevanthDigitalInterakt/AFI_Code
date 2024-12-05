import boto3
import json
from botocore.exceptions import ClientError

# Function to fetch secret from AWS Secrets Manager
def get_secret(secret_name: str, region_name: str = "eu-north-1"):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        # Retrieve the secret
        response = client.get_secret_value(SecretId=secret_name)
        
        # Secrets Manager stores the secret in a string format
        if "SecretString" in response:
            secret = response["SecretString"]
            return json.loads(secret)  # Convert it to a Python dictionary if it's a JSON string
        else:
            # If the secret is binary (rare case), decode it
            decoded_secret = response["SecretBinary"]
            return decoded_secret.decode("utf-8")
    except ClientError as e:
        print(f"Error fetching secret: {e}")
        return None

# Example usage
if __name__ == "__main__":
    secret_name = "afi/crm/test"  # Replace with your secret's name
    secret = get_secret(secret_name)
    
    if secret:
        print("Fetched Secret:", secret)
    else:
        print("Failed to fetch secret")
