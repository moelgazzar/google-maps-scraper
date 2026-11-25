"""Read only the rendered Maps results in a single, disposable Chrome session."""
import asyncio
import re
from datetime import datetime, timezone
from itertools import count
from pathlib import Path
import time
from urllib.parse import quote_plus, urlsplit

from playwright.async_api import Error, TimeoutError as PlaywrightTimeout, async_playwright

from nord_proxy import (NordProxyError, launch_nord_browser, ordered_hosts, proxy_hosts,
                        route_state, save_route_state, service_credentials)
from prospect import phone_key, place_id

SIGN_IN_TITLE = re.compile(r"^Sign[- ]in to get the best of Google Maps$", re.I)

READ_CARDS = """
feed => [...feed.querySelectorAll('a[href*="/maps/place/"]')].flatMap(link => {
  const name = link.getAttribute('aria-label');
  if (!name || !link.getClientRects().length) return [];
  let card = link.parentElement;
  while (card.parentElement !== feed &&
         card.parentElement.querySelectorAll('a[href*="/maps/place/"]').length === 1) {
    card = card.parentElement;
  }
  const lines = card.innerText.split('\\n').map(line => line.trim()).filter(Boolean);
  if (lines.some(line => /^(Sponsored|Annuncio)$/i.test(line))) return [];
  const site = card.querySelector('a[data-value="Website"], a[aria-label^="Visit "][aria-label$="website"]');
  const stars = [...card.querySelectorAll('[role="img"],[aria-label]')]
    .map(node => node.getAttribute('aria-label') || '').find(text => /\\bstars?\\b/i.test(text)) || '';
  const rating = stars.match(/\\d+\\.\\d+|\\d+/)?.[0] || '';
  const reviews = stars.match(/([\\d,]+)\\s+reviews?/i)?.[1] ||
    card.innerText.match(/\\(([\\d,]+)\\)/)?.[1] || '';
  const reputationLine = lines.findIndex(line => /\\d+(?:\\.\\d+)?\\s*\\([\\d,]+\\)/.test(line));
  const details = reputationLine >= 0 ? (lines[reputationLine + 1] || '').split('·')
    .map(text => text.trim()) : [];
  return [{name, maps_url: link.href.split('?')[0], rating, reviews,
    category: details[0] || '', address: details.find(text => /^\\d+\\s/.test(text)) || '',
    phone: card.innerText.match(/(?:\\+1\\s*)?\\(?\\d{3}\\)?[- .]\\d{3}[- .]\\d{4}/)?.[0] || '',
    website_url: site?.href || '',
    website_button: site || lines.includes('Website') ? 'yes' : 'no'}];
})
"""

READ_DIRECT_PROFILE_CARD = r"""
main => {
  const shown = el => el && el.getClientRects().length > 0;
  const name = [...main.querySelectorAll('h1')].find(shown)?.innerText?.trim() || '';
  const stars = [...main.querySelectorAll('[role="img"][aria-label]')]
    .map(node => node.getAttribute('aria-label') || '').find(text => /\bstars?\b/i.test(text)) || '';
  const reviewText = [...main.querySelectorAll('[aria-label]')]
    .map(node => node.getAttribute('aria-label') || '').find(text => /[\d,]+\s+reviews?\b/i.test(text)) || '';
  const rating = stars.match(/\d+\.\d+|\d+/)?.[0] || '';
  const reviews = reviewText.match(/([\d,]+)\s+reviews?/i)?.[1] || '';
  const category = [...main.querySelectorAll('button')].filter(shown)
    .map(node => node.innerText.trim()).find(text => /(?:detail|car wash|auto|vehicle|ceramic)/i.test(text)) || '';
  const address = [...main.querySelectorAll('[data-item-id="address"],[aria-label^="Address:"]')]
    .find(shown);
  const phone = [...main.querySelectorAll('[data-item-id^="phone:tel:"],[aria-label^="Phone:"]')]
    .find(shown);
  const website = [...main.querySelectorAll('a[data-item-id="authority"],a[aria-label^="Website:"]')]
    .find(shown);
  return {name, maps_url: location.href.split('?')[0], rating, reviews, category,
    address: address?.getAttribute('aria-label')?.replace(/^Address:\s*/i, '') || address?.innerText || '',
    phone: phone?.getAttribute('aria-label')?.replace(/^Phone:\s*/i, '') || phone?.innerText || '',
    website_url: website?.href || '', website_button: website ? 'yes' : 'no'};
}
"""


def sign_in_prompt(page):
    return page.get_by_text(SIGN_IN_TITLE).filter(visible=True).first


async def dismiss_maps_sign_in(prompt):
    if not await prompt.is_visible():
        return False
    # The sign-in card can be exposed as a group. Keep Close scoped to that card.
    container = prompt.locator("xpath=ancestor::*[self::dialog or @role='dialog' or @role='alertdialog' or @role='group'][1]")
    close = container.get_by_role("button", name=re.compile(r"^(Dismiss|Close)$", re.I)).filter(visible=True).first
    if not await close.is_visible():
        return False
    await close.click(timeout=4000)
    await prompt.wait_for(state="hidden", timeout=4000)
    return True


async def dismiss_consent(page, expected):
    decline = page.get_by_role("button", name=re.compile(r"^(Reject all|Rifiuta tutto)$", re.I)).first
    prompt = sign_in_prompt(page)
    await expected.or_(decline).or_(prompt).filter(visible=True).first.wait_for(timeout=18000)
    if await decline.is_visible():
        await decline.click(timeout=4000)
    await expected.or_(prompt).filter(visible=True).first.wait_for(timeout=18000)
    await dismiss_maps_sign_in(prompt)
    await expected.wait_for(timeout=18000)


def maps_restriction(page, response=None):
    if response is not None and response.status in {403, 429, 503}:
        return f"Maps returned HTTP {response.status}."
    url = urlsplit(page.url)
    if url.hostname and (url.hostname == "google.com" or url.hostname.endswith(".google.com")) and url.path.startswith("/sorry/"):
        return "Maps displayed an unusual-traffic challenge."
    return ""


async def capture_on_page(page, query, limit, saved_rows=(), checkpoint=None):
    started = time.monotonic()
    started_utc = datetime.now(timezone.utc).isoformat()
    url = f"https://www.google.com/maps/search/{quote_plus(query)}/?hl=en"
    records = {place_id(row["maps_url"]): row for row in saved_rows}
    stop = "scroll_limit"
    error = ""
    def sample(stop_reason):
        return {
            "query": query, "rows": list(records.values())[:limit],
            "started_utc": started_utc, "finished_utc": datetime.now(timezone.utc).isoformat(),
            "capture_seconds": round(time.monotonic() - started, 3),
            "stop": stop_reason, "error": error,
        }

    if limit is not None and len(records) >= limit:
        return sample("requested_limit")
    try:
        response = await page.goto(url, wait_until="domcontentloaded", timeout=22000)
        if error := maps_restriction(page, response):
            return sample("maps_access_restricted")
        feed = page.get_by_role("feed", name=f"Results for {query}", exact=True)
        direct_heading = page.get_by_role("main").locator("h1").filter(visible=True).first
        await dismiss_consent(page, feed.or_(direct_heading).filter(visible=True).first)
        if not await feed.is_visible() and await direct_heading.is_visible():
            main = direct_heading.locator("xpath=ancestor::*[@role='main'][1]")
            row = await main.evaluate(READ_DIRECT_PROFILE_CARD)
            if row["name"] and place_id(row["maps_url"]):
                row["phone"] = row["phone"] if phone_key(row["phone"]) else ""
                records.setdefault(place_id(row["maps_url"]), row)
                if checkpoint:
                    checkpoint(sample("direct_profile"))
                return sample("direct_profile")
        unchanged = 0
        seen_this_visit = set()
        more = feed.get_by_role("button", name=re.compile(r"^See more(?: results)?$", re.I))
        limited_view = feed.get_by_text("You're seeing a limited view of Google Maps.", exact=False).first
        prompt = sign_in_prompt(page)
        settle_attempts = 6 if limit is None else 4
        previous_position = None
        for _ in count() if limit is None else range(35):
            if limit is None and time.monotonic() - started >= 120:
                stop = "scroll_timeout"
                break
            saved_count = len(records)
            seen_count = len(seen_this_visit)
            for row in await feed.evaluate(READ_CARDS):
                row["phone"] = row["phone"] if phone_key(row["phone"]) else ""
                listing_id = place_id(row["maps_url"])
                seen_this_visit.add(listing_id)
                records.setdefault(listing_id, row)
            if len(records) > saved_count and checkpoint and not checkpoint(sample("capturing")):
                stop = "paused"
                break
            if limit is not None and len(records) >= limit:
                stop = "requested_limit"
                break
            position = await feed.evaluate("e => [e.scrollTop, e.scrollHeight, e.clientHeight]")
            top, height, viewport = position
            at_bottom = height - viewport - top < 3
            end = await page.get_by_text("You've reached the end of the list.", exact=True).count()
            if end and (limit is not None or at_bottom):
                stop = "maps_end_of_list"
                break
            moving = position != previous_position or len(seen_this_visit) > seen_count
            unchanged = 0 if moving else unchanged + 1
            previous_position = position
            if unchanged and await dismiss_maps_sign_in(prompt):
                unchanged = 0
                continue
            limited = unchanged and await limited_view.is_visible()
            if limited and at_bottom and (limit is not None or unchanged >= settle_attempts):
                stop = "maps_limited_view"
                break
            if unchanged >= settle_attempts:
                stop = "no_growth"
                break
            if unchanged and not limited and await more.count() == 1 and await more.is_visible():
                await more.click(timeout=3000)
            elif limit is None and at_bottom and (bounds := await feed.bounding_box()):
                # The banner can already be present on a feed that is still loading.
                # Scroll its actual panel again and give Maps time to append cards.
                await page.mouse.move(bounds["x"] + bounds["width"] / 2, bounds["y"] + bounds["height"] / 2)
                await page.mouse.wheel(0, int(bounds["height"]))
            else:
                await feed.evaluate("e => e.scrollBy(0, e.clientHeight * 0.85)")
            # The visible feed is lazy-loaded after scrolling.
            await asyncio.sleep(0.42)
    except Error as exc:
        restriction = maps_restriction(page)
        stop = "maps_access_restricted" if restriction else "maps_timeout" if isinstance(exc, PlaywrightTimeout) else "maps_error"
        error = restriction or str(exc).splitlines()[0][:300]
    return sample(stop)


async def capture_window(browser, queries, tabs, limit):
    started = time.monotonic()
    pending = iter(enumerate(queries))
    results = [None] * len(queries)
    context = await browser.new_context(locale="en-US", viewport={"width": 1280, "height": 800})

    async def worker(number):
        page = await context.new_page()
        for index, query in pending:
            result = await capture_on_page(page, query, limit)
            results[index] = {**result, "tab": number}
            print(f"  {query} | {len(result['rows'])} cards | {result['stop']}", flush=True)

    try:
        await asyncio.gather(*(worker(i + 1) for i in range(min(tabs, len(queries)))))
    finally:
        await context.close()
    return {"tabs": tabs, "wall_seconds": round(time.monotonic() - started, 3), "searches": results}


READ_PROFILE = """
main => {
  const info = main.querySelector('[aria-label^="Information for "]') || main;
  const shown = el => el.getClientRects().length > 0;
  const address = [...info.querySelectorAll('[data-item-id="address"],[aria-label^="Address:"]')]
    .find(shown);
  const website = [...info.querySelectorAll('a[data-item-id="authority"], a[aria-label^="Website:"]')]
    .find(shown);
  const links = [...info.querySelectorAll('a[href]')].filter(shown)
    .filter(el => /booking|book online|appointment|reservation/i.test(
      [el.getAttribute('aria-label'), el.getAttribute('data-item-id'), el.innerText].join(' ')))
    .map(el => el.href);
  const service = info.innerText.split('\\n').map(line => line.trim())
    .filter(line => /^(serves\\b|service area\\b)/i.test(line));
  return {address: address?.getAttribute('aria-label') || address?.innerText || '',
    website: website?.href || '', booking_links: [...new Set(links)],
    service_area: service.join(' ')};
}
"""


async def inspect_profile(page, candidate):
    try:
        url = candidate["maps_url"].split("?")[0] + "?hl=en"
        response = await page.goto(url, wait_until="domcontentloaded", timeout=16000)
        if restriction := maps_restriction(page, response):
            return {"error": restriction, "access_restricted": True}
        main = page.get_by_role("main", name=candidate["name"], exact=True)
        await dismiss_consent(page, main)
        info = main.get_by_role("region", name=f"Information for {candidate['name']}", exact=True)
        prompt = sign_in_prompt(page)
        await info.or_(prompt).filter(visible=True).first.wait_for(timeout=5000)
        await dismiss_maps_sign_in(prompt)
        await info.wait_for(timeout=5000)
        evidence = await main.evaluate(READ_PROFILE)
        evidence["add_website"] = bool(await main.get_by_role(
            "button", name="Add website", exact=True).count())
        return evidence
    except Error as exc:
        restriction = maps_restriction(page)
        return {"error": restriction or str(exc).splitlines()[0][:300], "access_restricted": bool(restriction)}


def capture_query(query, limit):
    async def capture():
        async with async_playwright() as playwright:
            username, password = service_credentials()
            state_path = Path(__file__).resolve().parent / ".runtime" / "nord-proxy-state.json"
            saved = route_state(state_path)
            browser = bridge = None
            errors = []
            for host in ordered_hosts(proxy_hosts(), saved["host"]):
                try:
                    browser, bridge, public_ip = await launch_nord_browser(
                        playwright, host, username, password, saved["public_ip"]
                    )
                    save_route_state(state_path, host, public_ip)
                    print(f"NordVPN browser route: {host} | public IP {public_ip}", flush=True)
                    break
                except NordProxyError as error:
                    errors.append(str(error))
            if not browser:
                raise NordProxyError("No fresh NordVPN browser route was available: " + " | ".join(errors))
            try:
                return await capture_window(browser, [query], 1, limit)
            finally:
                await browser.close()
                await bridge.close()

    started = time.monotonic()
    try:
        result = asyncio.run(capture())["searches"][0]
    except Error as error:
        raise RuntimeError(f"Chrome/Maps could not finish the query: {error}") from error
    if not result["rows"]:
        raise RuntimeError(result["error"] or "No rendered place cards were captured.")
    return result["rows"], round(time.monotonic() - started, 2), result["stop"]
