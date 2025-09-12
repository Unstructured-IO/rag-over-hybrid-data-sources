# Pipeline execution runner - requires main() and verification functions to be imported
# Note: All imports and functions are defined in other scripts

# Run the pipeline
s3_job_id, es_job_id = main()

# Poll both jobs to make sure they have completed before proceeding
es_job_info = poll_job_status(es_job_id, "Elasticsearch Ingest")
s3_job_info = poll_job_status(s3_job_id, "S3 Ingest")

# Verify the results (run this after workflows have completed)
print("\n🔍 Verifying processed results")
print("-" * 50)
verify_customer_support_results() 