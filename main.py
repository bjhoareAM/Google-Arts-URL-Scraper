import asyncio
import pandas as pd
from playwright.async_api import async_playwright
from rapidfuzz import process

COLLECTION_URL = "https://artsandculture.google.com/explore/collections/auckland-war-memorial-museum?c=assets"

async def scrape_titles():
    async with async_playwright() as p:
        print("Launching browser...")
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(COLLECTION_URL)
        print("Page loaded, scrolling...")

        # Scroll until we stop loading new content
        previous_height = 0
        for _ in range(30):  # scroll 30 times max
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(2000)
            current_height = await page.evaluate("document.body.scrollHeight")
            if current_height == previous_height:
                break
            previous_height = current_height

        print("Scrolling complete. Extracting items...")

        # Grab all /asset/ links and their inner text
        links = await page.query_selector_all("a[href^='/asset/']")
        results = []
        for link in links:
            href = await link.get_attribute("href")
            title = await link.inner_text()
            if href and title:
                results.append((title.strip(), f"https://artsandculture.google.com{href}"))

        await browser.close()
        return results

def match_titles_to_metadata(metadata_path, output_path):
    df = pd.read_csv(metadata_path)
    scraped_titles_urls = asyncio.run(scrape_titles())

    print(f"Scraped {len(scraped_titles_urls)} items from the collection page.")

    if not scraped_titles_urls:
        print("No items were scraped. Check the page or scroll logic.")
        df["google_arts_url"] = None
        df.to_csv(output_path, index=False)
        print(f"Output CSV saved but no matches: {output_path}")
        return

    # Match using fuzzy logic
    matched_urls = []
    for title in df["title/en"]:
        if pd.isna(title):
            matched_urls.append(None)
            continue
        match = process.extractOne(title, [t for t, _ in scraped_titles_urls])
        if match:
            matched_title, score, _ = match
            url = next((u for t, u in scraped_titles_urls if t == matched_title), None)
            matched_urls.append(url if score > 75 else None)
        else:
            matched_urls.append(None)

    df["google_arts_url"] = matched_urls
    df.to_csv(output_path, index=False)
    print(f"Matched CSV saved to: {output_path}")

# Run script
match_titles_to_metadata("Google Arts for URLs - Sheet1.csv", "Google_Arts_Matched.csv")
print("Script terminated successfully.")
