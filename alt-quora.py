import argparse
import os
import shutil
import boto3
from botocore.exceptions import ClientError
from playwright.sync_api import Playwright, sync_playwright, expect
import time
from datetime import datetime

# Initialize the S3 client
s3_client = boto3.client('s3')

def create_s3_bucket_if_not_exists(bucket_name: str, region: str = 'us-east-1') -> bool:
    """
    Check if an S3 bucket exists, and if not, create it.

    Args:
        bucket_name (str): The name of the S3 bucket.
        region (str): The AWS region where the bucket will be created if it doesn't exist.

    Returns:
        bool: True if the bucket was created or already exists, False if the bucket could not be created.
    """
    try:
        # Check if the bucket exists by sending a head_bucket request
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
        return True
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            # Bucket does not exist, so let's create it
            try:
                if region == 'us-east-1':
                    # For the 'us-east-1' region, you should not specify the `CreateBucketConfiguration`
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    # For any other region, you must specify the region in `CreateBucketConfiguration`
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
            # Other unexpected errors
            print(f"Error checking bucket '{bucket_name}': {e}")
            return False

def upload_file_to_s3(file_path: str, bucket_name: str, s3_file_key: str) -> bool:
    """
    Uploads a file to the specified S3 bucket.
    
    Args:
        file_path (str): Local path of the file to upload.
        bucket_name (str): Name of the S3 bucket.
        s3_file_key (str): The S3 object key (file name) under which the file will be stored.
    
    Returns:
        bool: True if upload was successful, False otherwise.
    """
    try:
        s3_client.upload_file(file_path, bucket_name, s3_file_key)
        print(f"File {file_path} successfully uploaded to S3 bucket {bucket_name} as {s3_file_key}")
        return True
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return False
    except NoCredentialsError:
        print("Credentials not available for AWS S3.")
        return False

def scrape_q_box_content(page, output_dir: str, s3_bucket: str = None, s3_prefix: str = "") -> int:
    """
    Scrapes the page for content within 'q-box' elements and saves it locally.
    
    Args:
        page (object): Playwright page object to interact with the web page.
        output_dir (str): Directory to save the extracted content.
        s3_bucket (str): Optional S3 bucket to upload the scraped files.
        s3_prefix (str): Prefix to use when uploading files to S3.
    
    Returns:
        int: Number of 'q-box' elements found and processed.
    """
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Find all elements with the 'q-box' class
    q_boxes = page.query_selector_all('span.q-box')
    print(f"{len(q_boxes)} 'q-box' elements found")

    if not q_boxes:
        return 0

    # Loop through each 'q-box' element and extract its content
    for i, q_box in enumerate(q_boxes, start=1):
        # Extract the inner content of the 'q-box' element
        content = q_box.text_content().strip()

        # Save the content to a file
        file_name = f'q_box_{i}.txt'
        file_path = os.path.join(output_dir, file_name)

        with open(file_path, 'w') as f:
            f.write(content)
        print(f'Saved {file_path}')

        # If S3 bucket is provided, upload the file to S3 with the prefix
        if s3_bucket:
            s3_file_key = os.path.join(s3_prefix, file_name) if s3_prefix else file_name
            upload_file_to_s3(file_path, s3_bucket, s3_file_key)

    return len(q_boxes)

def infinite_scroll(page, scroll_limit: int = 10, scroll_pause_time: float = 2.0):
    """
    Implements infinite scrolling on a page to load more content dynamically.

    Args:
        page (object): Playwright page object.
        scroll_limit (int): Maximum number of scrolls to perform.
        scroll_pause_time (float): Time to pause after each scroll to allow content to load.
    """
    previous_height = None
    scroll_count = 0

    while scroll_count < scroll_limit:
        # Scroll down to the bottom of the page
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(scroll_pause_time)

        # Get the current height of the page after scrolling
        current_height = page.evaluate("document.body.scrollHeight")

        # If no new content has loaded, break the loop
        if previous_height == current_height:
            print("Reached the end of the page or no more new content.")
            break

        previous_height = current_height
        scroll_count += 1
        print(f"Scroll {scroll_count}/{scroll_limit} completed.")

def merge_files(output_dir: str, merged_file_name: str) -> str:
    """
    Merge all text files in the output_dir into one merged file.

    Args:
        output_dir (str): The directory where individual files are stored.
        merged_file_name (str): The name of the merged output file.

    Returns:
        str: The path to the merged file.
    """
    # Ensure the output directory exists
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

def delete_local_directory(directory: str):
    """
    Delete the specified local directory and all its contents.

    Args:
        directory (str): The path to the directory to delete.
    """
    try:
        shutil.rmtree(directory)
        print(f"Local directory '{directory}' has been deleted.")
    except Exception as e:
        print(f"Failed to delete directory '{directory}': {e}")

def generate_s3_prefix(topic: str) -> str:
    """
    Generate the S3 prefix in the format: 'quora/{topic}/{system_date_time}/'.

    Args:
        topic (str): The Quora topic being scraped.

    Returns:
        str: The generated S3 prefix with date, time, and topic.
    """
    system_date_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    return f"quora/{topic}/{system_date_time}/"

def run(playwright: Playwright, topic: str, output_dir: str, s3_bucket: str = None, scroll_limit: int = 10) -> None:
    # Generate S3 prefix based on the topic and current system date/time
    s3_prefix = generate_s3_prefix(topic)

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()

    # Construct the page URL dynamically based on the provided topic
    page_url = f'https://www.quora.com/topic/{topic}'
    print(f"Navigating to: {page_url}")
    page.goto(page_url)

    # Implement infinite scrolling
    infinite_scroll(page, scroll_limit=scroll_limit)

    total_q_boxes = scrape_q_box_content(page, output_dir, s3_bucket, s3_prefix)
    print(f"Total 'q-box' elements scraped: {total_q_boxes}")

    context.close()
    browser.close()

    # Once scraping is complete, merge the files and upload the merged file to S3
    merged_file_name = "merged_file.txt"
    merged_file_path = merge_files(output_dir, merged_file_name)

    # Upload the merged file to S3 with the same prefix
    if s3_bucket:
        s3_file_key = os.path.join(s3_prefix, merged_file_name)
        upload_file_to_s3(merged_file_path, s3_bucket, s3_file_key)

    # Delete the local output directory after uploading
#    delete_local_directory(output_dir)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape content from a Quora topic with infinite scroll and upload it to S3.')
    parser.add_argument('-t', '--topic', type=str, required=True, help='Quora topic to scrape (e.g., "politics" or "technology")')
    parser.add_argument('-o', '--output_dir', type=str, default='output', help='Directory to save the scraped files')
    parser.add_argument('--s3_bucket', type=str, help='S3 bucket name to upload scraped files')
    parser.add_argument('--scroll_limit', type=int, default=10, help='Maximum number of scrolls to perform (for infinite scroll)')
    args = parser.parse_args()

    # Ensure the S3 bucket exists
    if args.s3_bucket:
        create_s3_bucket_if_not_exists(args.s3_bucket)

    # Run the scraper
    with sync_playwright() as playwright:
        run(playwright, args.topic, args.output_dir, args.s3_bucket, args.scroll_limit)
