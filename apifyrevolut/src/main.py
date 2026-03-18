"""Apify Actor: Revolut UAE Careers Scraper.

Scrapes Revolut career pages for UAE-Remote jobs across specified teams.
Three-phase scraping:
  1. Listing pages: extract job titles, URLs, teams
  2. Detail pages: extract full job descriptions
  3. Apply pages: extract application form fields

Structure:
  Listing: /careers/?city=UAE+-+Remote&team=Executive
  Detail:  /careers/position/{slug}-{uuid}/
  Apply:   /careers/apply/{uuid}/
"""

import asyncio
import re
from urllib.parse import quote_plus

from apify import Actor
from crawlee import Request, ConcurrencySettings
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext

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

# Stop markers for truncating boilerplate
DETAIL_STOP_MARKERS = [
    "Building a global financial super app",
    "Our Revoluters are a priority",
    "we encourage applications from people with diverse backgrounds",
    "Important notice for candidates",
    "Job scams are on the rise",
    "By submitting this application",
]

APPLY_STOP_MARKERS = [
    'Tick \u201cI consent\u201d above',
    "By submitting this application, I confirm",
    "For information on how we will handle your personal data",
    "Data Privacy Notice for Candidates",
]


def extract_uuid(url: str) -> str:
    m = re.search(
        r"([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})", url
    )
    return m.group(1) if m else ""


def should_skip(title: str, patterns: list[str]) -> bool:
    return any(re.search(p, title, re.IGNORECASE) for p in patterns)


def truncate_at_markers(text: str, markers: list[str]) -> str:
    """Truncate text at the first occurrence of any marker."""
    earliest = len(text)
    for marker in markers:
        idx = text.find(marker)
        if idx != -1 and idx < earliest:
            earliest = idx
    return text[:earliest].strip()


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
        scrape_details = actor_input.get("scrapeDetails", True)
        scrape_apply_form = actor_input.get("scrapeApplyForm", True)

        start_requests = []
        for team_name in teams:
            team_param = TEAM_URL_PARAMS.get(team_name)
            if not team_param:
                team_param = quote_plus(team_name)
            url = f"{BASE_URL}?city={city}&team={team_param}"
            start_requests.append(
                Request.from_url(
                    url,
                    user_data={"team": team_name, "teamIndex": teams.index(team_name)},
                )
            )

        # Shared storage for multi-phase scraping
        collected_jobs: dict[str, dict] = {}
        seen_uuids: set[str] = set()

        crawler = PlaywrightCrawler(
            max_requests_per_crawl=500,
            headless=True,
            browser_type="chromium",
            concurrency_settings=ConcurrencySettings(
                max_concurrency=1, desired_concurrency=1
            ),
        )

        @crawler.router.default_handler
        async def handle_team_page(context: PlaywrightCrawlingContext) -> None:
            """Phase 1: Extract job listings from team pages."""
            page = context.page
            team_name = context.request.user_data.get("team", "Unknown")
            team_index = context.request.user_data.get("teamIndex", 99)

            Actor.log.info(f"Scraping team: {team_name}")
            await page.wait_for_timeout(3000)

            # Click "Show more" until all jobs visible
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
                Actor.log.info(f"  [{team_name}] Clicked Show more {show_more_clicks}x")

            job_links = await page.evaluate(
                """
                () => {
                    const jobs = [];
                    document.querySelectorAll('a[href*="/careers/position/"]').forEach(a => {
                        const title = a.textContent.trim().split('\n')[0].trim();
                        const href = a.getAttribute('href');
                        if (title && href && title.length > 3 && title.length < 200) {
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

            Actor.log.info(f"  [{team_name}] Found {len(job_links)} raw links")

            team_seen_titles: set[str] = set()
            jobs_added = 0
            enqueue_requests = []

            for link in job_links:
                title = link.get("title", "").strip()
                href = link.get("href", "")
                location_details = link.get("location", "")

                if not title or not href or title in team_seen_titles:
                    continue
                team_seen_titles.add(title)

                if should_skip(title, skip_patterns):
                    continue

                detail_url = (
                    href if href.startswith("http") else f"https://www.revolut.com{href}"
                )
                uuid = extract_uuid(detail_url)
                if not uuid or uuid in seen_uuids:
                    continue
                seen_uuids.add(uuid)

                apply_url = f"https://www.revolut.com/careers/apply/{uuid}/"

                collected_jobs[uuid] = {
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
                    "score": round(max(0.5, 0.9 - team_index * 0.04), 2),
                    "description": None,
                    "apply_form_fields": None,
                    "apply_form_text": None,
                }

                if scrape_details:
                    enqueue_requests.append(
                        Request.from_url(
                            detail_url,
                            label="detail",
                            user_data={"uuid": uuid},
                        )
                    )
                if scrape_apply_form:
                    enqueue_requests.append(
                        Request.from_url(
                            apply_url,
                            label="apply",
                            user_data={"uuid": uuid},
                        )
                    )
                jobs_added += 1

            if enqueue_requests:
                await context.add_requests(enqueue_requests)

            Actor.log.info(
                f"  [{team_name}] Added {jobs_added} jobs, total unique: {len(seen_uuids)}"
            )

        @crawler.router.handler("detail")
        async def handle_detail(context: PlaywrightCrawlingContext) -> None:
            """Phase 2: Extract job description from detail pages."""
            page = context.page
            uuid = context.request.user_data.get("uuid", "")
            job_title = collected_jobs.get(uuid, {}).get("title", "unknown")

            Actor.log.info(f"  [Detail] {job_title}")
            await page.wait_for_timeout(3000)

            try:
                raw_text = await page.evaluate("() => document.body.innerText")

                # Try to start from meaningful section headers
                start_markers = [
                    "About the role",
                    "About the job",
                    "What you\u2019ll be doing",
                    "What you'll be doing",
                    "Description",
                    "The Role",
                ]
                start_idx = 0
                for marker in start_markers:
                    idx = raw_text.find(marker)
                    if idx != -1:
                        start_idx = idx
                        break

                description = raw_text[start_idx:]
                description = truncate_at_markers(description, DETAIL_STOP_MARKERS)

                if uuid in collected_jobs:
                    collected_jobs[uuid]["description"] = description.strip()

                Actor.log.info(f"  [Detail] {uuid}: {len(description)} chars")
            except Exception as e:
                Actor.log.warning(f"  [Detail] Failed {uuid}: {e}")

        @crawler.router.handler("apply")
        async def handle_apply(context: PlaywrightCrawlingContext) -> None:
            """Phase 3: Extract application form fields from apply pages."""
            page = context.page
            uuid = context.request.user_data.get("uuid", "")
            job_title = collected_jobs.get(uuid, {}).get("title", "unknown")

            Actor.log.info(f"  [Apply] {job_title}")
            await page.wait_for_timeout(5000)

            try:
                form_data = await page.evaluate(
                    """
                    () => {
                        const fields = [];

                        // Method 1: Standard label elements
                        document.querySelectorAll('label').forEach(label => {
                            const text = label.textContent.trim().replace(/\s+/g, ' ');
                            const forAttr = label.getAttribute('for');
                            let input = forAttr ? document.getElementById(forAttr) : null;
                            if (!input) input = label.querySelector('input, select, textarea');

                            let fieldType = 'unknown';
                            let options = [];
                            let required = false;

                            if (input) {
                                fieldType = input.tagName.toLowerCase();
                                if (fieldType === 'input') fieldType = input.type || 'text';
                                if (fieldType === 'select') {
                                    input.querySelectorAll('option').forEach(opt => {
                                        if (opt.value) options.push(opt.textContent.trim());
                                    });
                                }
                                required = input.required ||
                                           input.getAttribute('aria-required') === 'true';
                            }

                            if (text && text.length > 1 && text.length < 300) {
                                const field = { label: text, type: fieldType, required };
                                if (options.length) field.options = options;
                                fields.push(field);
                            }
                        });

                        // Method 2: aria-label inputs not covered above
                        document.querySelectorAll(
                            'input[aria-label], select[aria-label], textarea[aria-label]'
                        ).forEach(input => {
                            const al = input.getAttribute('aria-label');
                            if (al && !fields.some(f => f.label.includes(al))) {
                                let ft = input.tagName.toLowerCase();
                                if (ft === 'input') ft = input.type || 'text';
                                fields.push({
                                    label: al,
                                    type: ft,
                                    required: input.required ||
                                              input.getAttribute('aria-required') === 'true',
                                });
                            }
                        });

                        // Method 3: placeholder-only inputs
                        document.querySelectorAll(
                            'input[placeholder], textarea[placeholder]'
                        ).forEach(input => {
                            const ph = input.getAttribute('placeholder');
                            if (ph && !fields.some(f =>
                                f.label.includes(ph) || f.label === ph
                            )) {
                                let ft = input.tagName.toLowerCase();
                                if (ft === 'input') ft = input.type || 'text';
                                fields.push({
                                    label: ph,
                                    type: ft,
                                    required: input.required ||
                                              input.getAttribute('aria-required') === 'true',
                                });
                            }
                        });

                        return { fields, pageText: document.body.innerText };
                    }
                """
                )

                page_text = form_data.get("pageText", "")

                # Extract section between "You are applying" and consent boilerplate
                start_idx = page_text.find("You are applying")
                if start_idx == -1:
                    start_idx = 0
                relevant_text = page_text[start_idx:]
                relevant_text = truncate_at_markers(relevant_text, APPLY_STOP_MARKERS)

                if uuid in collected_jobs:
                    collected_jobs[uuid]["apply_form_fields"] = form_data.get(
                        "fields", []
                    )
                    collected_jobs[uuid]["apply_form_text"] = relevant_text.strip()

                fields_count = len(form_data.get("fields", []))
                Actor.log.info(
                    f"  [Apply] {uuid}: {fields_count} fields, {len(relevant_text)} chars"
                )
            except Exception as e:
                Actor.log.warning(f"  [Apply] Failed {uuid}: {e}")

        Actor.log.info(f"Starting with {len(start_requests)} listing requests")
        for r in start_requests:
            Actor.log.info(f"  {r.url}")

        try:
            await crawler.run(start_requests)
        except Exception as e:
            Actor.log.exception(f"Crawler failed: {e}")
            raise

        # Push all collected data to Apify dataset
        for job in collected_jobs.values():
            await Actor.push_data(job)

        Actor.log.info(f"Done! Total unique jobs: {len(collected_jobs)}")


if __name__ == "__main__":
    asyncio.run(main())
