from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
from playwright.async_api import async_playwright, Playwright
import os
import shutil
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

# Initialize FastAPI app
app = FastAPI()

# Initialize the S3 client
s3_client = boto3.client('s3')

# Define request model for input validation
class ScrapeRequest(BaseModel):
    topic: str
    output_dir: Optional[str] = 'output'
    s3_bucket: Optional[str] = None
    scroll_limit: Optional[int] = 10

def create_s3_bucket_if_not_exists(bucket_name: str, region: str = 'us-east-1') -> bool:
    """
    Check if an S3 bucket exists, and if not, create it.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
        return True
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            try:
                if region == 'us-east-1':
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={
                            'LocationConstraint': region
                        }
                    )
                print(f"Bucket '{bucket_name}' created successfully.")
                return True
            except ClientError as create_error:
                print(f"Failed to create bucket '{bucket_name}': {create_error}")
                return False
        else:
            print(f"Error checking bucket '{bucket_name}': {e}")
            return False

def upload_file_to_s3(file_path: str, bucket_name: str, s3_file_key: str) -> bool:
    """
    Uploads a file to the specified S3 bucket.
    """
    try:
        s3_client.upload_file(file_path, bucket_name, s3_file_key)
        print(f"File {file_path} successfully uploaded to S3 bucket {bucket_name} as {s3_file_key}")
        return True
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return False
    except ClientError as e:
        print(f"Failed to upload file to S3: {e}")
        return False

async def scrape_q_box_content(page, output_dir: str, s3_bucket: str = None, s3_prefix: str = "") -> int:
    """
    Scrapes the page for individual content within 'q-box' elements and saves each separately.
    """
    os.makedirs(output_dir, exist_ok=True)

    q_boxes = await page.query_selector_all('span.q-box')
    print(f"{len(q_boxes)} 'q-box' elements found")

    if not q_boxes:
        return 0

    for i, q_box in enumerate(q_boxes, start=1):
        content = await q_box.text_content()

        file_name = f'q_box_{i}.txt'
        file_path = os.path.join(output_dir, file_name)

        with open(file_path, 'w') as f:
            f.write(content.strip())
        print(f'Saved {file_path}')

        if s3_bucket:
            s3_file_key = os.path.join(s3_prefix, file_name) if s3_prefix else file_name
            upload_file_to_s3(file_path, s3_bucket, s3_file_key)

    return len(q_boxes)

def merge_files(output_dir: str, merged_file_name: str) -> str:
    """
    Merge all text files in the output_dir into one merged file.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    merged_file_path = os.path.join(output_dir, merged_file_name)
    with open(merged_file_path, 'w') as merged_file:
        for file_name in os.listdir(output_dir):
            file_path = os.path.join(output_dir, file_name)
            if file_path.endswith('.txt'):
                with open(file_path, 'r') as file:
                    merged_file.write(file.read())
                    merged_file.write("\n")
    print(f"Merged file saved at {merged_file_path}")
    return merged_file_path

async def infinite_scroll(page, scroll_limit: int = 10, scroll_pause_time: float = 2.0):
    """
    Implements infinite scrolling on a page to load more content dynamically.
    """
    previous_height = None
    scroll_count = 0

    while scroll_count < scroll_limit:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(scroll_pause_time * 1000)

        current_height = await page.evaluate("document.body.scrollHeight")
        if previous_height == current_height:
            print("Reached the end of the page or no more new content.")
            break

        previous_height = current_height
        scroll_count += 1
        print(f"Scroll {scroll_count}/{scroll_limit} completed.")

async def run(playwright: Playwright, topic: str, output_dir: str, s3_bucket: str = None, scroll_limit: int = 10) -> dict:
    """
    Run the scraping process using Playwright for the given Quora topic.

    Returns:
        dict: A dictionary containing the S3 bucket and full S3 URL (directory), or None if not uploaded to S3.
    """
    s3_prefix = generate_s3_prefix(topic)
    os.makedirs(output_dir, exist_ok=True)

    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context()
    page = await context.new_page()

    page_url = f'https://www.quora.com/topic/{topic}'
    print(f"Navigating to: {page_url}")
    await page.goto(page_url)

    await infinite_scroll(page, scroll_limit=scroll_limit)

    total_q_boxes = await scrape_q_box_content(page, output_dir, s3_bucket, s3_prefix)
    print(f"Total 'q-box' elements scraped: {total_q_boxes}")

    await context.close()
    await browser.close()

    # Merge the individual files into a single file
    merged_file_name = "merged_file.txt"
    merged_file_path = merge_files(output_dir, merged_file_name)

    # Upload the merged file to S3 and return the S3 bucket and S3 URL
    if s3_bucket:
        s3_file_key = os.path.join(s3_prefix, merged_file_name)
        upload_file_to_s3(merged_file_path, s3_bucket, s3_file_key)

        # Construct the full S3 URL (directory only, without file name)
        s3_url = f"https://{s3_bucket}.s3.amazonaws.com/{s3_prefix}"
        print(f"Files uploaded to S3 at: {s3_url}")

        # Delete the local files after scraping and merging are complete
        delete_local_directory(output_dir)

        return {
            "s3_bucket": s3_bucket,
            "s3_url": s3_url  # Return the full S3 URL (directory)
        }

    # Delete the local files after scraping and merging are complete (if not uploaded to S3)
    delete_local_directory(output_dir)

    return None

def generate_s3_prefix(topic: str) -> str:
    """
    Generate the S3 prefix in the format: 'quora/{topic}/{system_date_time}/'.
    """
    system_date_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    return f"quora/{topic}/{system_date_time}/"

def delete_local_directory(directory: str):
    """
    Deletes the specified local directory and all its contents.
    """
    if os.path.exists(directory):
        try:
            shutil.rmtree(directory)
            print(f"Local directory '{directory}' has been deleted.")
        except Exception as e:
            print(f"Failed to delete directory '{directory}': {e}")
    else:
        print(f"Directory '{directory}' does not exist.")

@app.post("/scrape")
async def scrape_quora(request: ScrapeRequest):
    if request.s3_bucket:
        bucket_created = create_s3_bucket_if_not_exists(request.s3_bucket)
        if not bucket_created:
            raise HTTPException(status_code=500, detail="Failed to create or access the S3 bucket")

    try:
        async with async_playwright() as playwright:
            result = await run(playwright, request.topic, request.output_dir, request.s3_bucket, request.scroll_limit)
            
            if result:
                return {
                    "status": "success",
                    "message": f"Scraping of {request.topic} completed successfully.",
                    "s3_bucket": result["s3_bucket"],
                    "s3_url": result["s3_url"]  # Return the full S3 URL
                }
            else:
                return {
                    "status": "success",
                    "message": f"Scraping of {request.topic} completed successfully. No files were uploaded to S3."
                }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
