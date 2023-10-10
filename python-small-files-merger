import boto3
import json

# Replace with your S3 bucket name and prefixes
bucket_name = '<your-bucket>'
source_prefix = '<your-data-folder>/'  # Input prefix for small files
destination_prefix = '<your-destination-data-folder>/'  # Output prefix for merged files
merge_size_limit_bytes = 5 * 1024 * 1024  # Maximum merged file size (e.g., 5MB)

# Initialize the S3 client
s3 = boto3.client('s3')

# Initialize variables for merging files
merged_data = []
current_size_bytes = 0
merged_file_count = 0

# Helper function to write merged data to a new S3 object
def write_merged_data(data, count):
    merged_file_key = f"{destination_prefix}merged_file_{count}.json"
    s3.put_object(Bucket=bucket_name, Key=merged_file_key, Body=json.dumps(data))
    return []
    
while True:
    # List objects in the S3 bucket with pagination
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=source_prefix,
    )
    
    # Check if there are more objects to retrieve
    if 'Contents' in response:
        for obj in response['Contents']:
            obj_key = obj['Key']
            obj_size_bytes = obj['Size']

            # Read the small file's content
            obj_data = s3.get_object(Bucket=bucket_name, Key=obj_key)['Body'].read()

            # Check if adding the current file would exceed the merge size limit
            if current_size_bytes + obj_size_bytes > merge_size_limit_bytes:
                merged_data = write_merged_data(merged_data, merged_file_count)
                current_size_bytes = 0
                merged_file_count += 1

            # Append the current file's content to the merged data
            merged_data.append(json.loads(obj_data))
            current_size_bytes += obj_size_bytes

    # Check if there are more objects to retrieve
    if 'NextContinuationToken' in response:
        next_token = response['NextContinuationToken']
    else:
        break

# Write any remaining merged data to a new S3 object
if merged_data:
    write_merged_data(merged_data, merged_file_count)

print(f"Merged {merged_file_count + 1} files.")
