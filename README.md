# Google Maps Scraper

Terminal-based business discovery with resumable state campaigns, a Census place and ZIP-area planner for all 50 states and DC, deduplication, and CSV exports. Live discovery requires separately authorized access to the source. Check the applicable provider terms before running it.

## Setup on macOS or Linux

1. Install Python 3.12 or newer, Google Chrome, and tmux. On Linux, launch from a graphical session or configure an X display before starting a campaign.
2. Create a virtual environment with `python3 -m venv .venv`, activate it with `source .venv/bin/activate`, and run `python -m pip install -r requirements.txt`.
3. Run `python google_maps_scraper.py --once` for an offline status check. Use `python google_maps_scraper.py` for the terminal dashboard. Press `M` to create or resume a named campaign.

The existing live runner expects authorized NordVPN manual service credentials in the process environment as `NORDVPN_SERVICE_USERNAME` and `NORDVPN_SERVICE_PASSWORD`. Use a secure way to populate the environment. Keep credentials out of terminal history and Git. The browser pauses on a genuine source restriction.

## How leads qualify

A listing needs at least 4.0 stars, at least 10 reviews, and no website visible on its Maps card to enter the profile-check queue. Check the full profile for a listed website, public phone and closure status before niche review. Historical observations are kept for deduplication and audit. They do not qualify a listing that falls below the current threshold.

Discovery writes its checkpoint and exports under `runs/` and `Campaigns/`. The source distribution contains only code, tests and Census geography. The original deployment's paid phone verification and CRM integrations require separate configuration and are excluded from this distribution.

Run the qualification tests with `python -m unittest test_campaign_candidates test_ai_review_pipeline test_profile_recheck`.

Geography is derived from the [U.S. Census Gazetteer and geographic relationships](https://www.census.gov/geographies/reference-files.html). Planned searches describe coverage of the local search plan.
