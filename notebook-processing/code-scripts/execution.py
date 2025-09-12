def run_workflow(workflow_id, workflow_name):
    """Run a workflow and return job information."""
    try:
        with UnstructuredClient(api_key_auth=UNSTRUCTURED_API_KEY) as client:
            response = client.workflows.run_workflow(
                request={"workflow_id": workflow_id}
            )
        
        job_id = response.job_information.id
        print(f"✅ Started {workflow_name} job: {job_id}")
        return job_id
        
    except Exception as e:
        print(f"❌ Error running {workflow_name} workflow: {e}")
        return None

def poll_job_status(job_id, job_name, wait_time=30):
    """Poll job status until completion."""
    print(f"⏳ Monitoring {job_name} job status...")
    
    while True:
        try:
            with UnstructuredClient(api_key_auth=UNSTRUCTURED_API_KEY) as client:
                response = client.jobs.get_job(
                    request={"job_id": job_id}
                )
            
            job = response.job_information
            status = job.status
            
            if status in ["SCHEDULED", "IN_PROGRESS"]:
                print(f"⏳ {job_name} job status: {status}")
                time.sleep(wait_time)
            elif status == "COMPLETED":
                print(f"✅ {job_name} job completed successfully!")
                return job
            elif status == "FAILED":
                print(f"❌ {job_name} job failed!")
                return job
            else:
                print(f"❓ Unknown {job_name} job status: {status}")
                return job
                
        except Exception as e:
            print(f"❌ Error polling {job_name} job status: {e}")
            time.sleep(wait_time)
