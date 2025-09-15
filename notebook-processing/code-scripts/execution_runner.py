s3_job_id, es_job_id = main()

es_job_info = poll_job_status(es_job_id, "Elasticsearch Ingest")
s3_job_info = poll_job_status(s3_job_id, "S3 Ingest")
print("\n🔍 Verifying processed results")
print("-" * 50)
verify_customer_support_results()
