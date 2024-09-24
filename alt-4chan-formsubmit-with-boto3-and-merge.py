import argparse
import os
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

def scrape_page(page, board: str, output_dir: str, s3_bucket: str = None, s3_prefix: str = "") -> int:
    # Scroll to bottom of the page
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")

    # Find all thread links and extract thread ids
    thread_links = page.query_selector_all('.thread')
    thread_ids = [link.get_attribute('id') for link in thread_links]
    print(f"{len(thread_ids)} threads found")

    if not thread_ids:
        return 0

    # Loop through each thread id and extract posts
    for thread_id in thread_ids:
        # Construct thread URL without the 't'
        thread_url = f'https://boards.4channel.org/{board}/thread/{thread_id[1:]}'

        # Go to thread page
        page.goto(thread_url)

        # Extract post information using browser console
        output = page.evaluate('''() => {
            let output = "-----\\n";
            let posts = document.querySelectorAll(".postContainer");
            for (let i = 0; i < posts.length; i++) {
                let post = posts[i];
                let number = post.querySelector(".postInfo .postNum").textContent.replace("No.", "");
                let caption = post.querySelector(".postMessage").innerHTML.trim();
                caption = caption.replace(/&gt;/g, ">");
                caption = caption.replace(/<br>/g, "\\n");
                caption = caption.replace(/<[^>]*>/g, "");
                if (caption) {
                    output += "--- " + number + "\\n" + caption + "\\n";
                }
            }
            return output;
        }''')

        # Ensure the output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Save output to a local file
        file_name = f'{thread_id[1:]}.txt'
        file_path = os.path.join(output_dir, file_name)

        with open(file_path, 'w') as f:
            f.write(output)
        print(f'Saved {file_path}')

        # If S3 bucket is provided, upload the file to S3 with the prefix
        if s3_bucket:
            s3_file_key = os.path.join(s3_prefix, file_name) if s3_prefix else file_name
            upload_file_to_s3(file_path, s3_bucket, s3_file_key)

    return len(thread_ids)

def merge_files(output_dir: str, merged_file_name: str) -> str:
    """
    Merge all text files in the output_dir into one merged file.

    Args:
        output_dir (str): The directory where individual files are stored.
        merged_file_name (str): The name of the merged output file.

    Returns:
        str: The path to the merged file.
    """
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

def click_all_button(page):
    """
    This function clicks the "All" button to enable infinite scrolling on the first page if it exists.
    """
    all_button = page.query_selector('a.depagelink[href][data-cmd="depage"]')
    if all_button:
        print("Clicking 'All' button to enable infinite scroll.")
        all_button.click()
        time.sleep(2)  # Wait for the page to reflect the change after clicking "All"
        return True
    else:
        print("No 'All' button found.")
    return False

def get_next_page_number(page):
    """
    Extracts the action value from the pageSwitcherForm to get the next page number.
    """
    next_page_form = page.query_selector('.pagelist.desktop .next form.pageSwitcherForm')
    if next_page_form:
        # Get the action value (which is the next page number, e.g., "2")
        action_value = next_page_form.get_attribute('action')
        return int(action_value) if action_value.isdigit() else None
    return None

def generate_s3_prefix(board: str) -> str:
    """
    Generate the S3 prefix in the format: '4chan/{board}/{system date_time}/'.
    
    Args:
        board (str): The name of the board (e.g., 'pol', 'g').
    
    Returns:
        str: The generated S3 prefix with date and time.
    """
    system_date_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    return f"4chan/{board}/{system_date_time}/"

def run(playwright: Playwright, board: str, max_scrolls: int, output_dir: str, s3_bucket: str = None) -> None:
    # Generate S3 prefix based on board and current system date/time
    s3_prefix = generate_s3_prefix(board)
    
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()

    # Navigate to the board's first page (no page number for the first page)
    first_page_url = f'https://boards.4channel.org/{board}/'
    page.goto(first_page_url)

    # Click the "All" button on the first page to enable infinite scroll, if it exists
    click_all_button(page)

    total_threads = 0
    scroll_count = 0
    last_thread_count = 0
    current_page_number = 2  # Start numbering from 2 for the next pages

    # Loop over pages as long as there are pages to navigate
    while True:
        while scroll_count < max_scrolls:
            # Scrape threads on the current page
            print(f"Scroll {scroll_count + 1}: Scraping visible threads...")
            current_threads_found = scrape_page(page, board, output_dir, s3_bucket, s3_prefix)

            if current_threads_found == last_thread_count:
                # If no new threads are found after scrolling, stop scrolling and look for the next page
                print("No new threads found after scrolling. Checking for next page...")
                break

            last_thread_count = current_threads_found
            total_threads += current_threads_found
            scroll_count += 1

            # Scroll down by one page (window.innerHeight)
            print("Scrolling down by one page...")
            page.evaluate("window.scrollBy(0, window.innerHeight)")

            # Wait for new content to load (adjust sleep time as needed)
            time.sleep(2)

            # Optionally, wait for new threads to load with a selector
            page.wait_for_selector('.thread', timeout=10000)  # Wait up to 10 seconds for new threads

        # Move to the next page (start numbering from page 2)
        next_page_url = f'https://boards.4channel.org/{board}/{current_page_number}/'
        print(f"Moving to page {current_page_number}: {next_page_url}")
        page.goto(next_page_url)
        current_page_number += 1  # Increment the page number for the next iteration
        scroll_count = 0  # Reset scroll count for the new page
        page.wait_for_load_state('load')  # Wait for the page to load fully

        # If no "Next" page or no new threads, break the loop
        if not page.query_selector('.pagelist.desktop .next form.pageSwitcherForm'):
            print("No more pages. Scraping complete.")
            break

    print(f"Total threads scraped: {total_threads}")
    context.close()
    browser.close()

    # Once scraping is complete, merge the files and upload the merged file to S3
    merged_file_name = "merged_file.txt"
    merged_file_path = merge_files(output_dir, merged_file_name)

    # Upload the merged file to S3 with the same prefix
    if s3_bucket:
        s3_file_key = os.path.join(s3_prefix, merged_file_name)
        upload_file_to_s3(merged_file_path, s3_bucket, s3_file_key)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape posts from 4chan with infinite scroll by page.')
    parser.add_argument('-b', '--board', type=str, default='pol', help='The 4chan board to scrape')
    parser.add_argument('-s', '--max_scrolls', type=int, default=10, help='Maximum number of scrolls per page')
    parser.add_argument('-o', '--output_dir', type=str, default='output', help='Directory to save the scraped files')
    parser.add_argument('--s3_bucket', type=str, help='S3 bucket name to upload scraped files')
    parser.add_argument('--region', type=str, default='us-east-1', help='AWS region for S3 bucket')
    args = parser.parse_args()

    # Ensure the S3 bucket exists
    if args.s3_bucket:
        create_s3_bucket_if_not_exists(args.s3_bucket, args.region)

    # Run the scraper
    with sync_playwright() as playwright:
        run(playwright, args.board, args.max_scrolls, args.output_dir, args.s3_bucket)
