# Apply against a Moto server (CI): AWS_ENDPOINT_URL points the provider at it.
alert_email       = "alerts@example.com"
s3_use_path_style = true
job_image_tag     = "ci"
# Moto only accepts release labels shaped like emr-7.12.0, not emr-spark-8.1.0.
emr_release_label = "emr-7.12.0"
