import os
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
# from fake_useragent import UserAgent

# Set up logging
logger = logging.getLogger("etl.extract")
# ua = UserAgent()


headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent':  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}


def extract_data(num_books, config=None):
    base_url = "https://www.amazon.com/s?k=self+improvement+books"

    results = []
    seen_titles = set()
    page = 1

    while len(results) < num_books:
        url = f"{base_url}&page={page}"
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            logger.error(f"Failed to retrieve page {page} — Status Code: {response.status_code}")
            break
        
        logger.info("Starting to parse books from amazon!")
        soup = BeautifulSoup(response.content, "html.parser")
        book_items = soup.select("div.s-result-item[data-component-type='s-search-result']")

        logger.info(f"Parsing books from page {page}") 
        for book in book_items:

            title_tag = book.select_one("h2 span")
            author_tag = book.select_one(".a-color-secondary .a-size-base+ .a-size-base")
            price_tag = book.select_one(".a-price .a-offscreen")
            reviews_num = book.select_one("span.a-size-base.s-underline-text")
            rating_tag = book.select_one(".a-icon-alt")

            
            if title_tag and price_tag:
                title = title_tag.get_text(strip=True)
                if title not in seen_titles:
                    seen_titles.add(title)
                    rating_book = rating_tag.get_text(strip=True) if rating_tag else "N/A"
                    if rating_book != "N/A":
                        # Get books that have 4.5 rating and above
                        if float(rating_book.split()[0]) >= 4.5:
                            results.append({
                                "Title": title,
                                "Author": author_tag.get_text(strip=True) if author_tag else "N/A",
                                "Price": price_tag.get_text(strip=True),
                                "Reviews": reviews_num.get_text(strip=True) if author_tag else "N/A",
                                "Rating": rating_tag.get_text(strip=True) if rating_tag else "N/A"
                            })

                if len(results) >= num_books:
                    break

        page += 1

    # creating dataframe
    df = pd.DataFrame(results)

    # Create raw data directory if it doesn't exist
    raw_dir = config['raw_data_path']
    os.makedirs(raw_dir, exist_ok=True)
    output_file = os.path.join(raw_dir, f"Books.csv")

    logger.info(f"Creating Books.csv file") 

    df.to_csv(output_file, index=False)

    logger.info(f"Books.csv file created in {raw_dir}") 


