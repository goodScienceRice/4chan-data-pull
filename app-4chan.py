import os
import shutil
import boto3
import aiofiles  # Import aiofiles for asynchronous file operations
from botocore.exceptions import ClientError
from playwright.async_api import async_playwright
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
import asyncio

# Initialize FastAPI app
app = FastAPI()

# Initialize the S3 client
s3_client = boto3.client('s3')

# Define request model
class ScrapeRequest(BaseModel):
    board: str
    max_scrolls: int = 10
    output_dir: str = 'output'
    s3_bucket: str = 'nyc-ccc'
    region: str = 'us-east-1'


@app.post("/scrape")
async def scrape_4chan(request: ScrapeRequest):
    try:
        # Ensure the S3 bucket exists
        create_s3_bucket_if_not_exists(request.s3_bucket, request.region)

        # Run the scraper asynchronously
        async with async_playwright() as playwright:
            await run(playwright, request.board, request.max_scrolls, request.output_dir, request.s3_bucket)
        
        return {"message": "Scraping completed successfully."}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def create_s3_bucket_if_not_exists(bucket_name: str, region: str = 'us-east-1') -> bool:
    """Check if an S3 bucket exists, and if not, create it."""
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
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                print(f"Bucket '{bucket_name}' created successfully.")
                return True
            except ClientError as create_error:
                print(f"Failed to create bucket '{bucket_name}': {create_error}")
                return False
        else:
            print(f"Error checking bucket '{bucket_name}': {e}")
            return False


async def run(playwright, board: str, max_scrolls: int, output_dir: str, s3_bucket: str = None) -> None:
    # Generate S3 prefix based on board and current system date/time
    s3_prefix = generate_s3_prefix(board)
    
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context()
    page = await context.new_page()

    # Navigate to the board's first page (no page number for the first page)
    first_page_url = f'https://boards.4channel.org/{board}/'
    await page.goto(first_page_url)

    # Attempt to click the "All" button or similar
    await click_all_button(page)

    total_threads = 0
    scroll_count = 0
    last_thread_count = 0
    current_page_number = 2  # Start numbering from 2 for the next pages

    # Loop over pages as long as there are pages to navigate
    while True:
        while scroll_count < max_scrolls:
            # Scrape threads on the current page
            print(f"Scroll {scroll_count + 1}: Scraping visible threads...")
            current_threads_found = await scrape_page(page, board, output_dir, s3_bucket, s3_prefix)

            if current_threads_found == last_thread_count:
                # If no new threads are found after scrolling, stop scrolling and look for the next page
                print("No new threads found after scrolling. Checking for next page...")
                break

            last_thread_count = current_threads_found
            total_threads += current_threads_found
            scroll_count += 1

            # Scroll down by one page (window.innerHeight)
            print("Scrolling down by one page...")
            await page.evaluate("window.scrollBy(0, window.innerHeight)")

            # Wait for new content to load (adjust sleep time as needed)
            await asyncio.sleep(2)

            # Optionally, wait for new threads to load with a selector
            await page.wait_for_selector('.thread', timeout=10000)  # Wait up to 10 seconds for new threads

        # Move to the next page (start numbering from page 2)
        next_page_url = f'https://boards.4channel.org/{board}/{current_page_number}/'
        print(f"Moving to page {current_page_number}: {next_page_url}")
        await page.goto(next_page_url)
        current_page_number += 1  # Increment the page number for the next iteration
        scroll_count = 0  # Reset scroll count for the new page
        await page.wait_for_load_state('load')  # Wait for the page to load fully

        # If no "Next" page or no new threads, break the loop
        if not await page.query_selector('.pagelist.desktop .next form.pageSwitcherForm'):
            print("No more pages. Scraping complete.")
            break

    print(f"Total threads scraped: {total_threads}")
    await context.close()
    await browser.close()

    # Once scraping is complete, merge the files and upload the merged file to S3
    merged_file_name = "merged_file.txt"
    merged_file_path = await merge_files(output_dir, merged_file_name)

    # Upload the merged file to S3 with the same prefix
    if s3_bucket:
        s3_file_key = os.path.join(s3_prefix, merged_file_name)
        await upload_file_to_s3(merged_file_path, s3_bucket, s3_file_key)

    # Delete the local output directory after uploading
    await delete_local_directory(output_dir)


async def scrape_page(page, board: str, output_dir: str, s3_bucket: str = None, s3_prefix: str = "") -> int:
    """Scrapes the current page for threads, saves them locally, and uploads to S3."""
    # Scrape all thread links on the current page
    thread_links = await page.query_selector_all('.thread')
    thread_ids = [await link.get_attribute('id') for link in thread_links]
    print(f"{len(thread_ids)} threads found")

    if not thread_ids:
        return 0

    # Convert the output_dir to an absolute path
    absolute_output_dir = os.path.abspath(output_dir)
    print(f"Absolute output directory: {absolute_output_dir}")

    # Ensure the output directory exists before saving
    output_path = os.path.join(absolute_output_dir, board)
    print(f"Output path for board '{board}': {output_path}")
    
    os.makedirs(output_path, exist_ok=True)  # Creates the directory if it doesn't exist
    print(f"Directory {output_path} created or already exists.")

    # Loop through each thread and extract the content
    for thread_id in thread_ids:
        thread_url = f'https://boards.4channel.org/{board}/thread/{thread_id[1:]}'  # [1:] removes the 't' prefix
        await page.goto(thread_url)

        # Extract thread content (e.g., post messages)
        output = await page.evaluate('''() => {
            let output = "-----\\n";
            let posts = document.querySelectorAll(".postContainer");
            for (let i = 0; i < posts.length; i++) {
                let post = posts[i];
                let number = post.querySelector(".postInfo .postNum").textContent.replace("No.", "");
                let caption = post.querySelector(".postMessage").innerHTML.trim();
                caption = caption.replace(/&gt;/g, ">");
                caption = caption.replace(/<br>/g, "\\n");
                caption = caption.replace(/<[^>]*>/g, "");  // Remove HTML tags
                if (caption) {
                    output += "--- " + number + "\\n" + caption + "\\n";
                }
            }
            return output;
        }''')

        # Save the thread content to a local .txt file
        file_name = f'{thread_id[1:]}.txt'
        file_path = os.path.join(output_path, file_name)
        print(f"Saving thread {thread_id[1:]} to {file_path}")

        # Write the content to the file asynchronously
        try:
            async with aiofiles.open(file_path, 'w') as f:
                await f.write(output)
            print(f"Thread {thread_id[1:]} saved to {file_path}")
        except Exception as e:
            print(f"Error saving thread {thread_id[1:]}: {e}")
            return 0

        # Optionally upload the file to S3
        if s3_bucket:
            s3_file_key = os.path.join(s3_prefix, file_name) if s3_prefix else file_name
            if await upload_file_to_s3(file_path, s3_bucket, s3_file_key):
                print(f"Thread {thread_id[1:]} uploaded to S3 as {s3_file_key}")
            else:
                print(f"Failed to upload thread {thread_id[1:]} to S3.")

    return len(thread_ids)


async def click_all_button(page):
    """Attempts to click the 'All' button or any other button that loads more threads."""
    try:
        # Replace '.some-button-class' with the actual class or id of the button to click
        button_selector = '.some-button-class'
        if await page.query_selector(button_selector):
            await page.click(button_selector)
            print("Clicked the 'All' button to load more threads.")
        else:
            print("No 'All' button found, continuing without clicking.")
    except Exception as e:
        print(f"Failed to click the 'All' button: {e}")


def generate_s3_prefix(board: str) -> str:
    """Generates an S3 prefix based on the board name and current timestamp."""
    system_date_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    return f"4chan/{board}/{system_date_time}/"


async def upload_file_to_s3(file_path: str, bucket_name: str, s3_file_key: str) -> bool:
    """Uploads a file to S3."""
    try:
        s3_client.upload_file(file_path, bucket_name, s3_file_key)
        print(f"Uploaded {file_path} to S3 bucket {bucket_name} as {s3_file_key}")
        return True
    except ClientError as e:
        print(f"Error uploading {file_path} to S3: {e}")
        return False


async def merge_files(output_dir: str, merged_file_name: str) -> str:
    """Merges all .txt files into one file."""
    merged_file_path = os.path.join(output_dir, merged_file_name)
    async with aiofiles.open(merged_file_path, 'w') as merged_file:
        for file_name in os.listdir(output_dir):
            file_path = os.path.join(output_dir, file_name)
            if file_path.endswith('.txt'):
                async with aiofiles.open(file_path, 'r') as file:
                    content = await file.read()
                    await merged_file.write(content)
                    await merged_file.write("\n")
    return merged_file_path


async def delete_local_directory(directory: str):
    """Deletes the local output directory and its contents."""
    try:
        shutil.rmtree(directory)
        print(f"Directory {directory} deleted successfully.")
    except Exception as e:
        print(f"Failed to delete directory '{directory}': {e}")
