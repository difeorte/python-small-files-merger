# python-small-files-merger
This Python script will allow you to merge small files in S3 for downstream distributed processing with AWS Glue, Amazon EMR, or Amazon Athena. You need to adjust the following parameters:

bucket_name
source_prefix # Input prefix for small files
destination_prefix # Output prefix for merged files in the same bucket
merge_size_limit_bytes # Maximum merged file size (e.g., 5MB)
