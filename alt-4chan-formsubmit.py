import argparse   
import os
from playwright.sync_api import Playwright, sync_playwright, expect
import time

def scrape_page(page, board: str, output_dir: str) -> int:
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
        # Construct thread url without the 't'
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

        # Save output to file in the specified directory
        file_name = f'{thread_id[1:]}.txt'
        file_path = os.path.join(output_dir, file_name)

        # Overwrite existing files with the same name
        with open(file_path, 'w') as f:
            f.write(output)
        print(f'Saved {file_path}')

    return len(thread_ids)

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

def run(playwright: Playwright, board: str, max_scrolls: int, output_dir: str) -> None:
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
            current_threads_found = scrape_page(page, board, output_dir)

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

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape posts from 4chan with infinite scroll by page.')
    parser.add_argument('-b', '--board', type=str, default='pol', help='The 4chan board to scrape')
    parser.add_argument('-s', '--max_scrolls', type=int, default=10, help='Maximum number of scrolls per page')
    parser.add_argument('-o', '--output_dir', type=str, default='output', help='Directory to save the scraped files')
    args = parser.parse_args()

    with sync_playwright() as playwright:
        run(playwright, args.board, args.max_scrolls, args.output_dir)
