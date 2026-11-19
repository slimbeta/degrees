#!/usr/bin/env python3
"""Gather climate coverage from many feeds into data/articles.json, and NASA's
global temperature record into data/stripes.json.

Each source is an RSS/Atom feed, optionally filtered (by author, category or
keyword). New stories get the publisher page's photo, description and opening
paragraphs when the site allows it. Standard library only.
"""
import html
import json
import re
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import quote_plus, urlparse

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "articles.json"
STRIPES = ROOT / "data" / "stripes.json"
MAX_ITEMS = 500
MAX_AGE_DAYS = 45
PER_SOURCE_PER_RUN = 8
UA = ("Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 "
      "(KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1")

CLIMATE_WORDS = re.compile(
    r"climat|warming|carbon|emission|greenhouse|co2|methane|fossil|sea.level|sea ice|glacier|ice sheet|"
    r"heat ?wave|wildfire|drought|flood|hurricane|el ni|la ni|temperature|net.zero|renewable|solar|wind power|"
    r"\bcop\d|unfccc|paris agreement|ocean|arctic|antarctic|permafrost", re.I)


def gnews(query):
    return "https://news.google.com/rss/search?q=" + quote_plus(query) + "&hl=en-US&gl=US&ceid=US:en"


# desk: science | politics | reporting | opinion
SOURCES = [
    # Science
    {"name": "NASA", "desk": "science", "feed": "https://science.nasa.gov/feed/", "keywords": CLIMATE_WORDS,
     "keyword_fields": "title+categories"},
    {"name": "NOAA Climate.gov", "desk": "science", "feed": "https://www.climate.gov/feeds/news-features/highlights.rss"},
    {"name": "Copernicus", "desk": "science", "feed": "https://climate.copernicus.eu/rss.xml"},
    {"name": "Berkeley Earth", "desk": "science", "feed": "https://berkeleyearth.org/feed/"},
    {"name": "Yale Climate Connections", "desk": "science", "feed": "https://yaleclimateconnections.org/feed/"},
    # Politics & diplomacy
    {"name": "Carbon Brief", "desk": "politics", "feed": "https://www.carbonbrief.org/feed/"},
    {"name": "Climate Home News", "desk": "politics", "feed": "https://www.climatechangenews.com/feed/",
     "categories": re.compile(r"^(politics|unfccc|cop\d+|article 6|paris agreement)$", re.I),
     "exclude_categories": re.compile(r"^sponsored$", re.I)},
    {"name": "Climate Diplomacy Brief", "byline": "Ed King", "desk": "politics",
     "feed": "https://climatediplobrief.substack.com/feed"},
    # Reporting
    {"name": "The New York Times", "byline": "Lisa Friedman", "desk": "reporting",
     "feed": "https://rss.nytimes.com/services/xml/rss/nyt/Climate.xml", "author": re.compile(r"Lisa Friedman", re.I)},
    {"name": "Bloomberg Green", "desk": "reporting", "feed": gnews("site:bloomberg.com/news climate when:14d"),
     "via_google": True},
    {"name": "Inside Climate News", "desk": "reporting", "feed": "https://insideclimatenews.org/feed/"},
    {"name": "The Guardian", "desk": "reporting", "feed": "https://www.theguardian.com/environment/climate-crisis/rss"},
    # Analysis & opinion
    {"name": "The New York Times", "byline": "David Wallace-Wells", "desk": "opinion",
     "feed": "https://www.nytimes.com/svc/collections/v1/publish/www.nytimes.com/column/david-wallace-wells/rss.xml",
     "keywords": CLIMATE_WORDS},
    {"name": "The Atlantic", "desk": "opinion", "feed": "https://www.theatlantic.com/feed/channel/science/",
     "keywords": CLIMATE_WORDS},
    {"name": "The Economist", "desk": "opinion", "feed": gnews("site:economist.com climate OR emissions OR warming when:21d"),
     "via_google": True},
    {"name": "Watts Up With That", "desk": "opinion", "note": "Skeptic blog",
     "feed": "https://wattsupwiththat.com/feed/"},
]

NS = {"dc": "http://purl.org/dc/elements/1.1/", "atom": "http://www.w3.org/2005/Atom",
      "media": "http://search.yahoo.com/mrss/", "content": "http://purl.org/rss/1.0/modules/content/"}


def get(url, limit=1_500_000, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "text/html,application/xml,*/*"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read(limit).decode("utf-8", "replace")


def tidy_desc(t):
    t = re.sub(r"\s*The post .{3,300}? appeared first on .{2,80}?\.?\s*$", "", t)
    t = re.sub(r"\s*(Continue reading|Read more|Keep reading)\.*\s*$", "", t, flags=re.I)
    t = re.sub(r"\s*\[(\u2026|\.\.\.|&hellip;)\]\s*$", "\u2026", t)
    return t.strip()


def text_of(node):
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", node or ""))).strip()


def parse_date(s):
    if not s:
        return None
    try:
        d = parsedate_to_datetime(s)
    except Exception:
        try:
            d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc)


def fetch_feed(url):
    for attempt in range(3):
        try:
            return get(url, limit=4_000_000)
        except urllib.error.HTTPError as e:
            if e.code != 429 or attempt == 2:
                raise
            time.sleep(10 * (attempt + 1))  # Google News rate limit


def parse_xml(raw):
    try:
        return ET.fromstring(raw.encode("utf-8"))
    except ET.ParseError:
        # Repair common feed slips: attributes run together, bare ampersands, control characters.
        raw = re.sub(r'"(?=[A-Za-z_:][\w:.-]*=")', '" ', raw)
        raw = re.sub(r"&(?!#?\w+;)", "&amp;", raw)
        raw = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", raw)
        return ET.fromstring(raw.encode("utf-8"))


def feed_items(src):
    if src.get("via_google"):
        time.sleep(3)
    root = parse_xml(fetch_feed(src["feed"]))
    items = root.findall(".//item") or root.findall(".//atom:entry", NS)
    for it in items:
        title = text_of(it.findtext("title") or it.findtext("atom:title", namespaces=NS))
        link = (it.findtext("link") or "").strip()
        if not link:
            el = it.find("atom:link", NS)
            link = el.get("href") if el is not None else ""
        desc = text_of(it.findtext("description") or it.findtext("atom:summary", namespaces=NS) or "")
        author = it.findtext("dc:creator", namespaces=NS) or it.findtext("author") or ""
        cats = [text_of(c.text) for c in it.findall("category")]
        when = parse_date(it.findtext("pubDate") or it.findtext("dc:date", namespaces=NS)
                          or it.findtext("atom:published", namespaces=NS) or it.findtext("atom:updated", namespaces=NS))
        image = None
        for tag in ("media:content", "media:thumbnail"):
            m = it.find(tag, NS)
            if m is not None and (m.get("url") or "").startswith("http") and m.get("medium", "image") == "image":
                image = m.get("url")
                break
        enc = it.find("enclosure")
        if not image and enc is not None and (enc.get("type") or "").startswith("image"):
            image = enc.get("url")
        yield {"title": title, "url": link, "desc": desc, "author": author.strip(), "cats": cats, "when": when, "image": image}


HOUSEKEEPING = re.compile(r"help us|donat|fundrais|open thread|sponsored|subscribe to|weekly roundup of comments|"
                          r"\bjobs?\b board|webinar|podcast:", re.I)


def keep(src, it):
    if not it["title"] or not it["url"] or HOUSEKEEPING.search(it["title"]):
        return False
    if "author" in src and not src["author"].search(it["author"]):
        return False
    if "categories" in src and not any(src["categories"].search(c) for c in it["cats"]):
        return False
    if "exclude_categories" in src and any(src["exclude_categories"].search(c) for c in it["cats"]):
        return False
    hay = f"{it['title']} {' '.join(it['cats'])}" if src.get("keyword_fields") == "title+categories" \
        else f"{it['title']} {it['desc']} {' '.join(it['cats'])}"
    if "keywords" in src and not src["keywords"].search(hay):
        return False
    return True


def meta(page, prop):
    for tag in re.findall(r"<meta\b[^>]*>", page, re.I):
        if not re.search(rf'(?:property|name)\s*=\s*["\']{re.escape(prop)}["\']', tag, re.I):
            continue
        m = re.search(r'content\s*=\s*(?:"([^"]*)"|\'([^\']*)\')', tag, re.I)
        if m:
            return html.unescape(m.group(1) if m.group(1) is not None else m.group(2)).strip()
    return None


BOILERPLATE = re.compile(
    r"subscribe|sign up|sign in|log in|newsletter|cookie|advertisement|all rights reserved|"
    r"support our|donate|your inbox|this article|share this|read more|click here|free trial|"
    r"\bpodcast\b|listen to|photo(graph)? by|illustration by|getty images|\(c\)|©|"
    r"originally appeared|originally published|translated by|own translation|"
    r"\b\d+ (pp|pages)\b|\bpp\.|\$\d|related:|you may also like|reprint", re.I)


def opening_paragraphs(page, limit=3000, most=10):
    body = page
    m = re.search(r"<article\b.*?</article>", page, re.S | re.I)
    if m and len(m.group(0)) > 2000:
        body = m.group(0)
    body = re.sub(r"<(script|style|aside|nav|header|footer|figure)\b.*?</\1>", " ", body, flags=re.S | re.I)
    out, total = [], 0
    for raw in re.findall(r"<p\b[^>]*>(.*?)</p>", body, re.S | re.I):
        t = text_of(raw)
        if len(t) < 90 or BOILERPLATE.search(t) or t.count(" ") < 12 or (out and t == out[-1]):
            continue
        out.append(t)
        total += len(t)
        if total >= limit or len(out) >= most:
            break
    return out


def clean_title(title):
    title = re.sub(r"^(Opinion|Review|Essay|Analysis)\s*[|:]\s*", "", title)
    return re.sub(r"\s+[|–—-]\s+[^|–—-]{2,40}$", "", title).strip()


def enrich(item):
    """Photo, description and opening paragraphs from the publisher page (best effort)."""
    item["ftries"] = item.get("ftries", 0) + 1
    if item.get("via_google"):
        item["fetched"] = True  # Google News links are redirects; nothing to read
        return item
    try:
        page = get(item["url"])
    except Exception as e:
        print(f"  no preview ({item['source']}): {e}", file=sys.stderr)
        return item
    item["fetched"] = True
    if item.get("undated"):
        found = meta(page, "article:published_time") or meta(page, "date") or meta(page, "dc.date")
        m = None if found else re.search(r'"datePublished"\s*:\s*"([^"]+)"', page)
        d = parse_date(found or (m.group(1) if m else None))
        if not d:
            m = re.search(r"Published:?\s+([A-Z][a-z]+ \d{1,2}, \d{4})", page)
            if m:
                try:
                    d = datetime.strptime(m.group(1), "%B %d, %Y").replace(tzinfo=timezone.utc)
                except ValueError:
                    d = None
        if d:
            item["published"] = d.isoformat(timespec="seconds")
            item.pop("undated", None)
    img = meta(page, "og:image") or meta(page, "twitter:image")
    if img and img.startswith("http") and not item.get("image"):
        item["image"] = img
    desc = meta(page, "og:description") or meta(page, "description")
    if desc and len(desc) > 40:  # the publisher's own standfirst beats a feed excerpt
        item["description"] = tidy_desc(re.sub(r"\s+", " ", desc).strip())
    paras = opening_paragraphs(page)
    d = item.get("description") or ""
    if paras and d and paras[0][:60] in d and len(paras[0]) <= len(d) + 20:
        paras = paras[1:]
    if paras:
        item["paragraphs"] = paras
    return item


def update_stripes():
    try:
        csv = get("https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv")
    except Exception as e:
        print(f"stripes: {e}", file=sys.stderr)
        return
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    years, latest = [], None
    for line in csv.splitlines()[2:]:
        cells = line.split(",")
        if not cells[0].isdigit():
            continue
        y = int(cells[0])
        if len(cells) > 13 and cells[13] not in ("***", ""):
            years.append([y, float(cells[13])])
        for i in range(12):
            if i + 1 < len(cells) and cells[i + 1] not in ("***", ""):
                latest = {"year": y, "month": months[i], "anomaly": float(cells[i + 1])}
    if years:
        STRIPES.write_text(json.dumps({"source": "NASA GISTEMP v4", "baseline": "1951–1980",
                                       "years": years, "latest": latest}))
        print(f"stripes: {years[0][0]}–{years[-1][0]}, latest {latest}")


def main():
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=MAX_AGE_DAYS)
    existing = json.loads(OUT.read_text()) if OUT.exists() else {"articles": []}
    known = {a["url"] for a in existing["articles"]}
    known_titles = {a["title"].lower() for a in existing["articles"]}
    fresh = []
    for src in SOURCES:
        try:
            items = [it for it in feed_items(src) if keep(src, it)]
        except Exception as e:
            print(f"feed failed: {src['name']} {src.get('byline', '')}: {e}", file=sys.stderr)
            continue
        added = 0
        for it in items:
            if added >= PER_SOURCE_PER_RUN:
                break
            when = it["when"] or now
            if when < cutoff or it["url"] in known or it["title"].lower() in known_titles:
                continue
            title = it["title"]
            if src.get("via_google"):
                title = re.sub(r"\s+-\s+[^-]+$", "", title)  # "Headline - Bloomberg.com"
            item = {"url": it["url"], "title": clean_title(title), "source": src["name"], "desk": src["desk"],
                    "published": when.isoformat(timespec="seconds"), "seen": now.isoformat(timespec="seconds")}
            if not it["when"]:
                item["undated"] = True
            for k in ("byline", "note", "via_google"):
                if src.get(k):
                    item[k] = src[k]
            if it["desc"] and not src.get("via_google") and len(it["desc"]) > 30:
                item["description"] = tidy_desc(it["desc"])[:600]
            if it["image"]:
                item["image"] = it["image"]
            known.add(item["url"])
            known_titles.add(item["title"].lower())
            fresh.append(item)
            added += 1
        print(f"{src['name']:<26} {src.get('byline', ''):<20} {len(items):>3} matching, {added} new")

    retry = [a for a in existing["articles"][:120] if not a.get("fetched") and a.get("ftries", 0) < 3]
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(enrich, fresh + retry))

    articles = [a for a in fresh + existing["articles"] if (parse_date(a["published"]) or now) >= cutoff]
    articles.sort(key=lambda a: a["published"], reverse=True)
    articles = articles[:MAX_ITEMS]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({"updated": now.isoformat(timespec="seconds"), "articles": articles},
                              ensure_ascii=False, indent=1))
    print(f"wrote {len(articles)} articles ({len(fresh)} new)")
    update_stripes()


if __name__ == "__main__":
    main()
