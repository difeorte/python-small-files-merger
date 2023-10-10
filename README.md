# python-small-files-merger
This Python script will allow you to merge small files in S3 for downstream distributed processing with AWS Glue, Amazon EMR, or Amazon Athena. You need to adjust the following parameters:

bucket_name = '<your-bucket>'
source_prefix = '<your-data-folder>/'  # Input prefix for small files
destination_prefix = '<your-destination-data-folder>/'  # Output prefix for merged files in the same bucket
merge_size_limit_bytes = 5 * 1024 * 1024  # Maximum merged file size (e.g., 5MB)
