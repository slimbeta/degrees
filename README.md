# Degrees

An iPhone-installable climate reader, laid out like a newspaper front page, built on the same
static site + hourly GitHub Action pattern as The Commonplace.

Sources and filters live in `scripts/update.py` (`SOURCES`):

| Desk | Sources |
|---|---|
| Science | NASA (climate-related only), NOAA Climate.gov, Copernicus, Berkeley Earth, Yale Climate Connections |
| Politics | Carbon Brief, Climate Home News (Politics / UNFCCC / COP / Article 6 only), Climate Diplomacy Brief (Ed King) |
| Reporting | NYT (Lisa Friedman only), Bloomberg Green (via Google News), Inside Climate News, The Guardian |
| Opinion | NYT David Wallace-Wells (climate columns), The Atlantic (science feed, climate stories), The Economist (via Google News), Watts Up With That |

The warming stripes under the masthead are redrawn each run from NASA GISTEMP v4.

Run locally: `python3 scripts/update.py`, then `python3 -m http.server`.
