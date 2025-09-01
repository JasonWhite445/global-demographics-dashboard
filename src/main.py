from controller.start_job import start_job

if __name__ == "__main__":
    
    result, status = start_job()
    if status == 200:
        print("Job completed successfully.")
    else:
        print(f"Job failed with message: {result}")

