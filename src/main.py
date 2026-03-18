"""Apify Actor: Revolut UAE Careers Scraper.

Scrapes Revolut career pages for UAE-Remote jobs across specified teams.
Handles React SPA with Playwright, clicks "Show more" to load all positions,
extracts job titles, detail URLs, and generates apply URLs.

Structure:
  Listing: /careers/?city=UAE+-+Remote&team=Executive
    → Job cards with links to /careers/position/{slug}-{uuid}/
    → "Show more" button loads remaining jobs
  Apply:   /careers/apply/{uuid}/
"""

import re
from urllib.parse import quote_plus

from apify import Actor
from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext

# ─── Team name → URL parameter mapping ──────────────────────
TEAM_URL_PARAMS = {
    "Executive": "Executive",
    "Risk, Compliance & Audit": "Risk%2C+Compliance+%26+Audit",
    "Product & Design": "Product+%26+Design",
    "Credit": "Credit",
    "Finance": "Finance",
    "Legal": "Legal",
    "Marketing & Comms": "Marketing+%26+Comms",
    "Operations": "Operations",
    "Business Development": "Business+Development",
    "Support & FinCrime": "Support+%26+FinCrime",
}

BASE_URL = "https://www.revolut.com/careers/"


def extract_uuid(url: str) -> str:
    """Extract UUID from a Revolut position URL."""
    m = re.search(
        r"([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})", url
    )
    return m.group(1) if m else ""


def should_skip(title: str, patterns: list[str]) -> bool:
    """Check if a job title matches any skip pattern."""
    return any(re.search(p, title, re.IGNORECASE) for p in patterns)


async def main() -> None:
    async with Actor:
        actor_input = await Actor.get_input() or {}

        city = actor_input.get("city", "UAE+-+Remote")
        teams = actor_input.get("teams", list(TEAM_URL_PARAMS.keys()))
        skip_patterns = actor_input.get(
            "skipPatterns",
            [
                r"\b(intern|internship)\b",
                r"\b(student|graduate)\b",
                r"\b(junior|jr\.?)\b",
                r"\b(entry.level)\b",
                r"\b(apprentice)\b",
            ],
        )
        max_show_more = actor_input.get("maxShowMoreClicks", 20)

        # Build start URLs with team metadata
        start_requests = []
        for team_name in teams:
            team_param = TEAM_URL_PARAMS.get(team_name)
            if not team_param:
                # Try URL-encoding the team name as fallback
                team_param = quote_plus(team_name)
            url = f"{BASE_URL}?city={city}&team={team_param}"
            start_requests.append(
                {
                    "url": url,
                    "userData": {
                        "team": team_name,
                        "teamIndex": teams.index(team_name),
                    },
                }
            )

        # Track seen UUIDs globally for deduplication
        seen_uuids: set[str] = set()

        crawler = PlaywrightCrawler(
            max_requests_per_crawl=len(start_requests) + 5,
            headless=True,
            browser_type="chromium",
            max_concurrency=1,  # Sequential to avoid rate limiting
        )

        @crawler.router.default_handler
        async def handle_team_page(context: PlaywrightCrawlingContext) -> None:
            page = context.page
            team_name = context.request.user_data.get("team", "Unknown")
            team_index = context.request.user_data.get("teamIndex", 99)

            Actor.log.info(f"Scraping team: {team_name} → {context.request.url}")

            # Wait for page content to load
            await page.wait_for_timeout(3000)

            # Click "Show more" until all jobs are visible
            show_more_clicks = 0
            while show_more_clicks < max_show_more:
                try:
                    show_more = page.locator("text=Show more").first
                    if await show_more.is_visible(timeout=2000):
                        await show_more.click()
                        show_more_clicks += 1
                        await page.wait_for_timeout(1500)
                    else:
                        break
                except Exception:
                    break

            if show_more_clicks:
                Actor.log.info(
                    f"  [{team_name}] Clicked 'Show more' {show_more_clicks}x"
                )

            # Extract all job position links from the page
            job_links = await page.evaluate(
                """
                () => {
                    const jobs = [];
                    document.querySelectorAll('a[href*="/careers/position/"]').forEach(a => {
                        const title = a.textContent.trim().split('\\n')[0].trim();
                        const href = a.getAttribute('href');
                        if (title && href && title.length > 3 && title.length < 200) {
                            // Get location info from nearby elements
                            const parent = a.closest(
                                '[class*="card"], [class*="Card"], [class*="item"], ' +
                                '[class*="Item"], [class*="role"], [class*="Role"]'
                            ) || a.parentElement;
                            let location = '';
                            if (parent) {
                                const locEls = parent.querySelectorAll('span, p, div');
                                for (const el of locEls) {
                                    const text = el.textContent.trim();
                                    if (text.includes('Remote:') || text.includes('Office:')) {
                                        location += text + ' ';
                                    }
                                }
                            }
                            jobs.push({ title, href, location: location.trim() });
                        }
                    });
                    return jobs;
                }
            """
            )

            Actor.log.info(
                f"  [{team_name}] Found {len(job_links)} raw job links on page"
            )

            # Process and deduplicate
            team_seen_titles: set[str] = set()
            jobs_added = 0
            jobs_skipped = 0

            for link in job_links:
                title = link.get("title", "").strip()
                href = link.get("href", "")
                location_details = link.get("location", "")

                if not title or not href:
                    continue

                if title in team_seen_titles:
                    continue
                team_seen_titles.add(title)

                if should_skip(title, skip_patterns):
                    Actor.log.debug(f"  SKIP (pattern match): {title}")
                    jobs_skipped += 1
                    continue

                # Build URLs
                detail_url = (
                    href
                    if href.startswith("http")
                    else f"https://www.revolut.com{href}"
                )
                uuid = extract_uuid(detail_url)

                if not uuid:
                    Actor.log.warning(f"  No UUID found in URL: {detail_url}")
                    continue

                # Global dedup by UUID
                if uuid in seen_uuids:
                    continue
                seen_uuids.add(uuid)

                apply_url = f"https://www.revolut.com/careers/apply/{uuid}/"

                await context.push_data(
                    {
                        "title": title,
                        "company": "Revolut",
                        "team": team_name,
                        "teamPreferenceIndex": team_index,
                        "location": city.replace("+", " ").replace("-", "- "),
                        "location_details": location_details,
                        "apply_url": apply_url,
                        "detail_url": detail_url,
                        "uuid": uuid,
                        "platform": "revolut_custom",
                        "score": round(
                            max(0.5, 0.9 - team_index * 0.04), 2
                        ),  # Higher score for preferred teams
                    }
                )
                jobs_added += 1

            Actor.log.info(
                f"  [{team_name}] Added {jobs_added} jobs, "
                f"skipped {jobs_skipped} (pattern), "
                f"total unique so far: {len(seen_uuids)}"
            )

        await crawler.run(start_requests)

        Actor.log.info(f"Done! Total unique jobs scraped: {len(seen_uuids)}")
