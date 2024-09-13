import argparse   
import re
from playwright.sync_api import Playwright, sync_playwright

def run(playwright: Playwright, board: str, page_num: int) -> None:
    browser = playwright.firefox.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()
    website = 'https://archive.4plebs.org/'
    
    # Navigate to the first page
    page.goto(f"{website}{board}/page/{page_num}")

    # Function to process a single page and extract thread links
    def process_page():
        # Scroll to bottom of the page to ensure all content is loaded
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")

        # Find all thread links and extract thread ids
        thread_links = page.query_selector_all('.thread a.btnr.parent')
        thread_ids = set()
        for link in thread_links:
            href = link.get_attribute('href')
            match = re.search(r'/thread/(\d+)/', href)
            if match:
                thread_ids.add(match.group(1))

        print(f"{len(thread_ids)} threads found on current page")

        # Loop through each thread id and extract posts
        for thread_id in thread_ids:
            # Construct thread URL
            thread_url = f'{website}{board}/thread/{thread_id}'
            page.goto(thread_url)

            # Extract post information using browser console
            output = page.evaluate('''() => {
                let output = "-----\\n";
                let posts = document.querySelectorAll('article[data-doc-id]');
                for (let i = 0; i < posts.length; i++) {
                    let post = posts[i];
                    let number = post.querySelector('a[data-post]').getAttribute('data-post');
                    let caption = post.querySelector(".text").innerHTML.trim();
                    caption = caption.replace(/&gt;/g, ">");
                    caption = caption.replace(/<br>/g, "\\n");
                    caption = caption.replace(/<[^>]*>/g, "");
                    if (caption) {
                        output += "--- " + number + "\\n" + caption + "\\n";
                    }
                }
                return output;
            }''')

            # Save output to file
            file_name = f'{thread_id[1:]}.txt'

            # Overwrite existing files with the same name
            with open(file_name, 'w') as f:
                f.write(output)
            print(f'Saved {file_name}')

    # Function to get the "Next" page link from the list within the .paginate class
    def get_next_page_link():
        # Scroll to bottom to ensure pagination is loaded
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")

        # Find the "Next" link inside the <ul> or <ol> within the .paginate class
        next_link = page.query_selector('.paginate li a.next')  # Adjusted selector for ul/ol
        if next_link:
            return next_link.get_attribute('href')  # Get the href attribute for the next page link
        return None

    # Start processing from the initial page
    while True:
        print(f"Processing page {page_num}")
        
        # Process the current page
        process_page()

        # Find the next page link
        next_page_link = get_next_page_link()
        
        if next_page_link:
            print(f"Navigating to next page: {next_page_link}")
            page.goto(next_page_link)  # Navigate to the next page
            page_num += 1  # Increment the page number counter (for logging or debugging)
        else:
            print("No more pages to process.")
            break  # Break out of the loop if there's no next page link

    context.close()
    browser.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape posts from 4chan.')
    parser.add_argument('-b', '--board', type=str, default='pol', help='The 4chan board to scrape')
    parser.add_argument('-p', '--page_num', type=int, default=1, help='The starting page number')
    args = parser.parse_args()

    with sync_playwright() as playwright:
        run(playwright, args.board, args.page_num)
