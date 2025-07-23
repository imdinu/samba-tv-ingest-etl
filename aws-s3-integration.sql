CREATE OR REPLACE STORAGE INTEGRATION aws_prod
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::{account_number}:role/{your_integration_role}'
  STORAGE_ALLOWED_LOCATIONS = ('s3://{s3-aws-bucket-name}/');

DESC INTEGRATION aws_prod;
  
CREATE OR REPLACE FILE FORMAT samba_tv.raw.parquet_auto
  TYPE = PARQUET
  COMPRESSION = AUTO;

CREATE OR REPLACE STAGE samba_tv.raw.samba_s3
  STORAGE_INTEGRATION = aws_prod
  URL = 's3://{s3-aws-bucket-name}/'
  FILE_FORMAT = samba_tv.raw.parquet_auto;
