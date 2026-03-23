"""Apify Actor: Revolut Multi-Region Careers Scraper.

Strategy:
  Revolut uses Next.js with SSG. ALL positions (648+) are embedded in the
  __NEXT_DATA__ JSON on the careers listing page. No "Show more" clicking needed.

  Phase 1: Fetch listing page, parse __NEXT_DATA__ for all positions
  Phase 2: Fetch detail pages for job descriptions (also from __NEXT_DATA__)
  Phase 3: Scrape apply form fields from /careers/apply/{uuid}/

Regions scraped: UAE, Poland, Europe (UK/Ireland/Spain/Portugal/etc.)

Structure:
  Listing: /careers/  (contains ALL positions in __NEXT_DATA__)
  Detail:  /careers/position/{slug}-{uuid}/  (has description in __NEXT_DATA__)
  Apply:   /careers/apply/{uuid}/
"""

import asyncio
import json
import re
from html import unescape
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
    "Engineering": "Engineering",
    "Data": "Data",
    "People & Recruitment": "People+%26+Recruitment",
    "Sales": "Sales",
    "Other": "Other",
}

BASE_URL = "https://www.revolut.com/careers/"

UAE_LOCATION_PATTERNS = [
    r"\bUAE\b",
    r"\bDubai\b",
    r"\bAbu Dhabi\b",
    r"\bUnited Arab Emirates\b",
]

POLAND_LOCATION_PATTERNS = [
    r"\bPoland\b",
    r"\bWarsaw\b",
    r"\bWarszawa\b",
    r"\bKrak[oó]w\b",
    r"\bCracow\b",
    r"\bWroc[lł]aw\b",
    r"\bGda[nń]sk\b",
    r"\bPozna[nń]\b",
    r"\bSilesia\b",
    r"\b[LŁ][oó]d[zź]\b",
    r"\bKatowice\b",
    r"\bLublin\b",
]

EUROPE_LOCATION_PATTERNS = [
    r"\bUK\b",
    r"\bUnited Kingdom\b",
    r"\bLondon\b",
    r"\bManchester\b",
    r"\bEdinburgh\b",
    r"\bBirmingham\b",
    r"\bIreland\b",
    r"\bDublin\b",
    r"\bSpain\b",
    r"\bMadrid\b",
    r"\bBarcelona\b",
    r"\bPortugal\b",
    r"\bLisbon\b",
    r"\bLisboa\b",
    r"\bPorto\b",
    r"\bGermany\b",
    r"\bBerlin\b",
    r"\bFrankfurt\b",
    r"\bMunich\b",
    r"\bFrance\b",
    r"\bParis\b",
    r"\bItaly\b",
    r"\bMilan\b",
    r"\bRome\b",
    r"\bNetherlands\b",
    r"\bAmsterdam\b",
    r"\bBelgium\b",
    r"\bBrussels\b",
    r"\bSwitzerland\b",
    r"\bZurich\b",
    r"\bAustria\b",
    r"\bVienna\b",
    r"\bSweden\b",
    r"\bStockholm\b",
    r"\bDenmark\b",
    r"\bCopenhagen\b",
    r"\bNorway\b",
    r"\bOslo\b",
    r"\bFinland\b",
    r"\bHelsinki\b",
    r"\bCzech\b",
    r"\bPrague\b",
    r"\bRomania\b",
    r"\bBucharest\b",
    r"\bHungary\b",
    r"\bBudapest\b",
    r"\bGreece\b",
    r"\bAthens\b",
    r"\bCroatia\b",
    r"\bZagreb\b",
    r"\bLithuania\b",
    r"\bVilnius\b",
    r"\bLatvia\b",
    r"\bRiga\b",
    r"\bEstonia\b",
    r"\bTallinn\b",
    r"\bBulgaria\b",
    r"\bSofia\b",
    r"\bSlovakia\b",
    r"\bBratislava\b",
    r"\bSlovenia\b",
    r"\bLjubljana\b",
    r"\bLuxembourg\b",
    r"\bCyprus\b",
    r"\bMalta\b",
    r"\bEurope\b",
    r"\bEMEA\b",
    r"\bEEA\b",
    r"\bEU\b",
]

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

TEAM_PREFERENCE_ORDER = [
    "Executive",
    "Risk, Compliance & Audit",
    "Product & Design",
    "Credit",
    "Finance",
    "Legal",
    "Marketing & Comms",
    "Operations",
    "Business Development",
    "Support & FinCrime",
    "Engineering",
    "Data",
    "People & Recruitment",
    "Sales",
    "Other",
]


def extract_uuid(url: str) -> str:
    m = re.search(
        r"([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})", url
    )
    return m.group(1) if m else ""


def should_skip(title: str, patterns: list[str]) -> bool:
    return any(re.search(p, title, re.IGNORECASE) for p in patterns)


def _match_patterns(combined: str, patterns: list[str]) -> bool:
    return any(re.search(p, combined, re.IGNORECASE) for p in patterns)


def get_job_region(locations: list[dict]) -> str | None:
    """Return the region for a job based on its locations.

    Priority: UAE > POLAND > EUROPE (checked in order, first match wins).
    Returns None if no region matches.
    """
    combined_parts = []
    for loc in locations:
        name = loc.get("name", "")
        country = loc.get("country", "")
        combined_parts.append(f"{name} {country}")
    combined = " | ".join(combined_parts)

    if _match_patterns(combined, UAE_LOCATION_PATTERNS):
        return "UAE"
    if _match_patterns(combined, POLAND_LOCATION_PATTERNS):
        return "POLAND"
    if _match_patterns(combined, EUROPE_LOCATION_PATTERNS):
        return "EUROPE"
    return None


def format_locations(locations: list[dict]) -> str:
    offices = []
    remotes = []
    for loc in locations:
        name = loc.get("name", "")
        loc_type = loc.get("type", "")
        if loc_type == "office":
            offices.append(name)
        elif loc_type == "remote":
            remotes.append(name)
    parts = []
    if offices:
        parts.append(f"Office: {', '.join(offices)}")
    if remotes:
        parts.append(f"Remote: {', '.join(remotes)}")
    return " | ".join(parts)


def html_to_text(html_str: str) -> str:
    """Convert HTML job description to clean text."""
    if not html_str:
        return ""
    text = unescape(html_str)
    text = re.sub(r"<h[1-6][^>]*>", "\n\n## ", text)
    text = re.sub(r"</h[1-6]>", "\n", text)
    text = re.sub(r"<li[^>]*>", "\n- ", text)
    text = re.sub(r"<br\s*/?>", "\n", text)
    text = re.sub(r"<p[^>]*>", "\n", text)
    text = re.sub(r"</p>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def truncate_at_markers(text: str, markers: list[str]) -> str:
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
        scrape_details = actor_input.get("scrapeDetails", True)
        scrape_apply_form = actor_input.get("scrapeApplyForm", True)

        # ─── Phase 1: Extract ALL positions from __NEXT_DATA__ ──────

        collected_jobs: dict[str, dict] = {}
        seen_uuids: set[str] = set()
        team_counts: dict[str, int] = {}

        # We use PlaywrightCrawler for the listing page too, to handle
        # any potential JS-rendered content, but extract from __NEXT_DATA__
        listing_crawler = PlaywrightCrawler(
            max_requests_per_crawl=5,
            headless=True,
            browser_type="chromium",
            concurrency_settings=ConcurrencySettings(
                max_concurrency=1, desired_concurrency=1
            ),
        )

        @listing_crawler.router.default_handler
        async def handle_listing(context: PlaywrightCrawlingContext) -> None:
            page = context.page
            Actor.log.info("Extracting positions from __NEXT_DATA__")
            await page.wait_for_timeout(3000)

            # Extract __NEXT_DATA__ JSON
            next_data = await page.evaluate(
                "() => {"
                "  var el = document.getElementById('__NEXT_DATA__');"
                "  if (el) return JSON.parse(el.textContent);"
                "  return null;"
                "}"
            )

            if not next_data:
                Actor.log.warning("__NEXT_DATA__ not found!")
                return

            positions = (
                next_data.get("props", {}).get("pageProps", {}).get("positions", [])
            )
            Actor.log.info(f"Found {len(positions)} total positions in __NEXT_DATA__")

            # Get team counts from widget data
            widget = (
                next_data.get("props", {})
                .get("pageProps", {})
                .get("widgetData", {})
                .get("careers-teams-widget", {})
            )
            total_count = widget.get("total", len(positions))
            functions_raw = widget.get("functions", {})
            for func_name, count in functions_raw.items():
                team_counts[func_name] = count
            team_counts["_total"] = total_count

            Actor.log.info(f"Total open positions (widget): {total_count}")

            # Filter positions
            skipped_no_region = 0
            skipped_junior = 0
            skipped_team = 0
            region_counts: dict[str, int] = {}

            teams_set = set(teams)

            for pos in positions:
                title = pos.get("text", "").strip()
                pos_id = pos.get("id", "")
                team = pos.get("team", "Unknown")
                locations = pos.get("locations", [])

                if not title or not pos_id:
                    continue

                if team not in teams_set:
                    skipped_team += 1
                    continue

                region = get_job_region(locations)
                if region is None:
                    skipped_no_region += 1
                    continue

                if should_skip(title, skip_patterns):
                    skipped_junior += 1
                    continue

                if pos_id in seen_uuids:
                    continue
                seen_uuids.add(pos_id)

                region_counts[region] = region_counts.get(region, 0) + 1

                team_index = (
                    TEAM_PREFERENCE_ORDER.index(team)
                    if team in TEAM_PREFERENCE_ORDER
                    else 99
                )
                apply_url = f"https://www.revolut.com/careers/apply/{pos_id}/"
                detail_url = f"https://www.revolut.com/careers/position/{pos_id}/"

                collected_jobs[pos_id] = {
                    "title": title,
                    "company": "Revolut",
                    "team": team,
                    "teamPreferenceIndex": team_index,
                    "region": region,
                    "location": format_locations(locations),
                    "location_details": format_locations(locations),
                    "apply_url": apply_url,
                    "detail_url": detail_url,
                    "uuid": pos_id,
                    "platform": "revolut_custom",
                    "score": round(max(0.5, 0.9 - team_index * 0.04), 2),
                    "description": None,
                    "description_html": None,
                    "apply_form_fields": None,
                    "apply_form_text": None,
                }

            region_summary = ", ".join(
                f"{r}: {c}" for r, c in sorted(region_counts.items())
            )
            Actor.log.info(
                f"Filtered: {len(collected_jobs)} jobs across regions ({region_summary}) "
                f"(skipped {skipped_no_region} outside target regions, "
                f"{skipped_junior} junior/student, "
                f"{skipped_team} wrong team)"
            )

        listing_request = Request.from_url(BASE_URL)
        Actor.log.info(f"Phase 1: Fetching listing page {BASE_URL}")
        await listing_crawler.run([listing_request])

        if not collected_jobs:
            Actor.log.warning("No jobs found! Exiting.")
            return

        # ─── Phase 2 & 3: Detail pages + Apply form scraping ────────

        detail_apply_requests = []
        for uuid, job in collected_jobs.items():
            if scrape_details:
                detail_apply_requests.append(
                    Request.from_url(
                        job["detail_url"],
                        label="detail",
                        user_data={"uuid": uuid},
                    )
                )
            if scrape_apply_form:
                detail_apply_requests.append(
                    Request.from_url(
                        job["apply_url"],
                        label="apply",
                        user_data={"uuid": uuid},
                    )
                )

        if detail_apply_requests:
            detail_crawler = PlaywrightCrawler(
                max_requests_per_crawl=len(detail_apply_requests) + 10,
                headless=True,
                browser_type="chromium",
                concurrency_settings=ConcurrencySettings(
                    max_concurrency=3, desired_concurrency=3
                ),
            )

            @detail_crawler.router.handler("detail")
            async def handle_detail(context: PlaywrightCrawlingContext) -> None:
                """Phase 2: Extract job description from __NEXT_DATA__."""
                page = context.page
                uuid = context.request.user_data.get("uuid", "")
                job_title = collected_jobs.get(uuid, {}).get("title", "unknown")

                Actor.log.info(f"  [Detail] {job_title}")
                await page.wait_for_timeout(2000)

                try:
                    # Extract description from __NEXT_DATA__
                    next_data = await page.evaluate(
                        "() => {"
                        "  var el = document.getElementById('__NEXT_DATA__');"
                        "  if (el) return JSON.parse(el.textContent);"
                        "  return null;"
                        "}"
                    )

                    if next_data:
                        pos_data = (
                            next_data.get("props", {})
                            .get("pageProps", {})
                            .get("position", {})
                        )
                        desc_html = pos_data.get("description", "")
                        if desc_html and uuid in collected_jobs:
                            desc_text = html_to_text(desc_html)
                            desc_text = truncate_at_markers(
                                desc_text, DETAIL_STOP_MARKERS
                            )
                            collected_jobs[uuid]["description"] = desc_text
                            collected_jobs[uuid]["description_html"] = desc_html
                            Actor.log.info(
                                f"  [Detail] {uuid}: {len(desc_text)} chars from __NEXT_DATA__"
                            )
                            return

                    # Fallback: extract from visible text
                    raw_text = await page.evaluate(
                        "() => { return document.body.innerText; }"
                    )
                    start_markers = [
                        "About the role",
                        "About the job",
                        "What you\u2019ll be doing",
                        "Description",
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

                    Actor.log.info(
                        f"  [Detail] {uuid}: {len(description)} chars from DOM"
                    )
                except Exception as e:
                    Actor.log.warning(f"  [Detail] Failed {uuid}: {e}")

            @detail_crawler.router.handler("apply")
            async def handle_apply(context: PlaywrightCrawlingContext) -> None:
                """Phase 3: Extract application form fields."""
                page = context.page
                uuid = context.request.user_data.get("uuid", "")
                job_title = collected_jobs.get(uuid, {}).get("title", "unknown")

                Actor.log.info(f"  [Apply] {job_title}")
                await page.wait_for_timeout(5000)

                try:
                    # Use string concat to avoid \n escaping issues in evaluate
                    form_data = await page.evaluate(
                        "() => {"
                        "  var fields = [];"
                        "  document.querySelectorAll('label').forEach(function(label) {"
                        "    var text = label.textContent.trim().replace(/\\s+/g, ' ');"
                        "    var forAttr = label.getAttribute('for');"
                        "    var input = forAttr ? document.getElementById(forAttr) : null;"
                        "    if (!input) input = label.querySelector('input, select, textarea');"
                        "    var fieldType = 'unknown';"
                        "    var options = [];"
                        "    var required = false;"
                        "    if (input) {"
                        "      fieldType = input.tagName.toLowerCase();"
                        "      if (fieldType === 'input') fieldType = input.type || 'text';"
                        "      if (fieldType === 'select') {"
                        "        input.querySelectorAll('option').forEach(function(opt) {"
                        "          if (opt.value) options.push(opt.textContent.trim());"
                        "        });"
                        "      }"
                        "      required = input.required || input.getAttribute('aria-required') === 'true';"
                        "    }"
                        "    if (text && text.length > 1 && text.length < 300) {"
                        "      var field = { label: text, type: fieldType, required: required };"
                        "      if (options.length) field.options = options;"
                        "      fields.push(field);"
                        "    }"
                        "  });"
                        "  document.querySelectorAll('input[aria-label], select[aria-label], textarea[aria-label]').forEach(function(input) {"
                        "    var al = input.getAttribute('aria-label');"
                        "    if (al && !fields.some(function(f) { return f.label.indexOf(al) !== -1; })) {"
                        "      var ft = input.tagName.toLowerCase();"
                        "      if (ft === 'input') ft = input.type || 'text';"
                        "      fields.push({ label: al, type: ft, required: input.required || input.getAttribute('aria-required') === 'true' });"
                        "    }"
                        "  });"
                        "  return { fields: fields, pageText: document.body.innerText };"
                        "}"
                    )

                    page_text = form_data.get("pageText", "")
                    start_idx = page_text.find("You are applying")
                    if start_idx == -1:
                        start_idx = 0
                    relevant_text = page_text[start_idx:]
                    relevant_text = truncate_at_markers(
                        relevant_text, APPLY_STOP_MARKERS
                    )

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

            Actor.log.info(
                f"Phase 2+3: {len(detail_apply_requests)} requests "
                f"({len(collected_jobs)} detail + apply pages)"
            )
            await detail_crawler.run(detail_apply_requests)

        # ─── Push results ────────────────────────────────────────────

        # Sort by team preference
        sorted_jobs = sorted(
            collected_jobs.values(),
            key=lambda j: j.get("teamPreferenceIndex", 99),
        )

        for job in sorted_jobs:
            await Actor.push_data(job)

        # Log summary
        teams_found = {}
        regions_found = {}
        for job in sorted_jobs:
            t = job["team"]
            r = job.get("region", "UNKNOWN")
            teams_found[t] = teams_found.get(t, 0) + 1
            regions_found[r] = regions_found.get(r, 0) + 1

        Actor.log.info(f"Done! Total unique jobs across all regions: {len(sorted_jobs)}")
        Actor.log.info(f"Total open positions at Revolut: {team_counts.get('_total', '?')}")
        Actor.log.info("Jobs by region:")
        for region, count in sorted(regions_found.items()):
            Actor.log.info(f"  {region}: {count} jobs")
        Actor.log.info("Jobs by team:")
        for team, count in teams_found.items():
            total = team_counts.get(team, "?")
            Actor.log.info(f"  {team}: {count} jobs (of {total} total)")

        desc_count = sum(1 for j in sorted_jobs if j.get("description"))
        Actor.log.info(f"Job descriptions fetched: {desc_count}/{len(sorted_jobs)}")


if __name__ == "__main__":
    asyncio.run(main())
