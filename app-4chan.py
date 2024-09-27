import os
import shutil
import boto3
import aiofiles
from botocore.exceptions import ClientError
from playwright.async_api import async_playwright
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
import asyncio

app = FastAPI()

s3_client = boto3.client('s3')

class ScrapeRequest(BaseModel):
    board: str
    max_scrolls: int = 10
    output_dir: str = './data/pol'
    s3_bucket: str = 'epigen-nycc-data'
    region: str = 'us-east-1'


def create_s3_bucket_if_not_exists(bucket_name: str, region: str = 'us-east-1'):
    """Check if an S3 bucket exists, and if not, create it."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            print(f"Bucket '{bucket_name}' not found. Creating bucket...")
            try:
                if region == 'us-east-1':
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                print(f"Bucket '{bucket_name}' created successfully.")
            except ClientError as create_error:
                print(f"Error creating bucket '{bucket_name}': {create_error}")
                raise
        else:
            print(f"Error checking bucket '{bucket_name}': {e}")
            raise


@app.post("/scrape")
async def scrape_4chan(request: ScrapeRequest):
    try:
        create_s3_bucket_if_not_exists(request.s3_bucket, request.region)

        print(f"Using specified output directory: {request.output_dir}")

        async with async_playwright() as playwright:
            s3_prefix = generate_s3_prefix(request.board)
            await run(playwright, request.board, request.max_scrolls, request.output_dir, request.s3_bucket, s3_prefix)
        
        merged_file_name = "merged_file.txt"
        merged_file_path = await merge_files(request.output_dir, merged_file_name)

        await upload_files_to_s3(request.output_dir, request.s3_bucket, s3_prefix, exclude=[merged_file_name])

        s3_file_key = os.path.join(s3_prefix, merged_file_name)
        await upload_file_to_s3(merged_file_path, request.s3_bucket, s3_file_key)

        await delete_files_in_directory(request.output_dir, exclude=[merged_file_name])

        s3_directory_url = f"https://{request.s3_bucket}.s3.amazonaws.com/{s3_prefix}"
        
        return {
            "message": "Scraping completed successfully.",
            "s3_directory_url": s3_directory_url
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def run(playwright, board: str, max_scrolls: int, output_dir: str, s3_bucket: str, s3_prefix: str) -> None:
    """Scrapes threads from a 4chan board, paginates through the board, and saves the data to the output directory."""

    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context()
    page = await context.new_page()

    base_url = f'https://boards.4channel.org/{board}/'
    current_page_number = 1
    await page.goto(base_url)

    total_threads = 0
    scroll_count = 0
    last_thread_count = 0

    while True:
        while scroll_count < max_scrolls:
            print(f"Scroll {scroll_count + 1} on page {current_page_number}: Scraping visible threads...")
            current_threads_found = await scrape_page(page, board, output_dir, s3_bucket, s3_prefix)

            if current_threads_found == last_thread_count:
                print("No new threads found after scrolling. Checking for the next page...")
                break

            last_thread_count = current_threads_found
            total_threads += current_threads_found
            scroll_count += 1

            await page.evaluate("window.scrollBy(0, window.innerHeight)")
            await asyncio.sleep(2)
            await page.wait_for_selector('.thread', timeout=10000)

        current_page_number += 1
        next_page_url = f'{base_url}{current_page_number}/'
        print(f"Moving to page {current_page_number}: {next_page_url}")
        
        await page.goto(next_page_url)
        scroll_count = 0
        await page.wait_for_load_state('load')

        if not await page.query_selector('.pagelist.desktop .next form.pageSwitcherForm'):
            print("No more pages. Scraping complete.")
            break

    print(f"Total threads scraped: {total_threads}")
    await context.close()
    await browser.close()


async def scrape_page(page, board: str, output_dir: str, s3_bucket: str, s3_prefix: str) -> int:
    """Scrapes all threads on the current page and saves them to local .txt files."""
    
    thread_links = await page.query_selector_all('.thread')
    thread_ids = [await link.get_attribute('id') for link in thread_links]
    print(f"Found {len(thread_ids)} threads.")

    if not thread_ids:
        return 0

    os.makedirs(output_dir, exist_ok=True)

    for thread_id in thread_ids:
        thread_url = f'https://boards.4channel.org/{board}/thread/{thread_id[1:]}'  
        await page.goto(thread_url)

        output = await page.evaluate('''() => {
            let output = "-----\\n";
            let posts = document.querySelectorAll(".postContainer");

            if (!posts) {
                return "No posts found";
            }

            for (let post of posts) {
                let number = post.querySelector(".postInfo .postNum")?.textContent.replace("No.", "") || "Unknown";
                let message = post.querySelector(".postMessage")?.innerHTML.trim().replace(/<br>/g, "\\n").replace(/<[^>]*>/g, "") || "No message found";
                output += "--- " + number + "\\n" + message + "\\n";
            }
            return output;
        }''')

        if output is None or not output.strip():
            print(f"No content found for thread {thread_id[1:]}. Skipping...")
            continue

        file_name = f"{thread_id[1:]}.txt"
        file_path = os.path.join(output_dir, file_name)
        print(f"Saving thread {thread_id[1:]} to {file_path}")

        try:
            async with aiofiles.open(file_path, 'w') as f:
                await f.write(output)
            print(f"Thread {thread_id[1:]} saved.")

            if s3_bucket:
                s3_file_key = os.path.join(s3_prefix, file_name)
                await upload_file_to_s3(file_path, s3_bucket, s3_file_key)

        except Exception as e:
            print(f"Error saving thread {thread_id[1:]}: {e}")

    return len(thread_ids)


async def upload_files_to_s3(directory: str, s3_bucket: str, s3_prefix: str, exclude: list = None):
    """Uploads individual files from the board directory to S3."""
    try:
        print(f"Uploading files from directory: {directory}")
        for file_name in os.listdir(directory):
            if exclude and file_name in exclude:
                continue
            file_path = os.path.join(directory, file_name)
            if os.path.isfile(file_path):
                print(f"Uploading file: {file_name}")
                s3_file_key = os.path.join(s3_prefix, file_name)
                await upload_file_to_s3(file_path, s3_bucket, s3_file_key)
    except Exception as e:
        print(f"Error uploading files in directory '{directory}': {e}")


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


async def delete_files_in_directory(directory: str, exclude: list = []):
    """Deletes all files in a directory, excluding any files specified in the 'exclude' list."""
    try:
        for file_name in os.listdir(directory):
            if file_name in exclude:
                continue
            file_path = os.path.join(directory, file_name)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Deleted file: {file_path}")
        print(f"All files in {directory} deleted except for {exclude}.")
    except Exception as e:
        print(f"Error deleting files in directory '{directory}': {e}")


async def merge_files(output_dir: str, merged_file_name: str) -> str:
    """Merges all .txt files in the specified board directory into a single file."""
    merged_file_path = os.path.join(output_dir, merged_file_name)

    try:
        if not os.path.exists(output_dir):
            raise Exception(f"Output directory {output_dir} does not exist.")
        
        txt_files = [f for f in os.listdir(output_dir) if f.endswith('.txt') and f != merged_file_name]
        
        if not txt_files:
            raise Exception(f"No .txt files found in {output_dir}.")

        async with aiofiles.open(merged_file_path, 'w') as outfile:
            for file in txt_files:
                file_path = os.path.join(output_dir, file)
                async with aiofiles.open(file_path, 'r') as infile:
                    content = await infile.read()
                    if content.strip():
                        await outfile.write(content)
                        await outfile.write('\n\n')
        
        print(f"All files merged into {merged_file_path}.")
        return merged_file_path

    except Exception as e:
        print(f"Error while merging files: {e}")
        raise
