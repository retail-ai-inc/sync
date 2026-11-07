"""Turns govulncheck SARIF output into the reports used by the Code Vulnerability Scan workflow.

Writes govulncheck-report.html (full report artifact) and govulncheck-summary.md (PR comment and job summary),
prints error annotations on the lines that call vulnerable code, and sets the `called` step output.

Usage: python3 .github/scripts/govulncheck_report.py [vulns.sarif]
"""
import datetime
import html
import json
import os
import sys
import urllib.request

HTML_OUT = "govulncheck-report.html"
MD_OUT = "govulncheck-summary.md"
MARKER = "<!-- govulncheck-report -->"  # Lets the workflow find and update its own PR comment

# govulncheck levels: error = vulnerable code is called, warning = package imported only, note = module required only
LEVELS = {
    "error": ("Called", "Your code calls the vulnerable functions. These need to be fixed."),
    "warning": ("Imported, not called", "Your code imports a vulnerable package but does not call the vulnerable functions."),
    "note": ("Required, not called", "A required module is vulnerable, but your code does not import the vulnerable package."),
}
ORDER = list(LEVELS)
MAX_SITES = 5  # Call sites per vulnerability in the PR comment; the HTML report lists all of them
MAX_FUNCS = 3  # Vulnerable functions per call site in the PR comment
MAX_ANNOTATIONS = 10  # GitHub shows at most 10 error annotations per step
MAX_COMMENT = 60000  # GitHub rejects comments over 65536 characters

server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
repo = os.environ.get("GITHUB_REPOSITORY", "")
sha = os.environ.get("GITHUB_SHA", "")
blob_url = f"{server}/{repo}/blob/{sha}/"
run_url = f"{server}/{repo}/actions/runs/{os.environ.get('GITHUB_RUN_ID', '')}"


def load_sarif(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def esc(text):
    return html.escape(str(text))


def md(text):
    return str(text).replace("|", "\\|").replace("\n", " ")


def short(name):
    # "lollipop-core/packages/helpers.*conn.Upload" -> "helpers.*conn.Upload"
    return name.rsplit("/", 1)[-1]


def frame_info(frame):
    loc = frame.get("location", {})
    phys = loc.get("physicalLocation", {})
    region = phys.get("region", {})
    return {
        "uri": phys.get("artifactLocation", {}).get("uri", ""),
        "own": phys.get("artifactLocation", {}).get("uriBaseId") == "%SRCROOT%",
        "line": region.get("startLine") or 0,
        "col": region.get("startColumn") or 0,
        "name": short(loc.get("message", {}).get("text", "")),
    }


def call_sites(res):
    # Groups the call paths by the line in our code where they leave our module
    sites = {}
    for flow in res.get("codeFlows", []):
        for thread in flow.get("threadFlows", []):
            frames = [frame_info(f) for f in thread.get("locations", [])]
            own = [i for i, f in enumerate(frames) if f["own"] and f["uri"] and f["line"]]
            if not own:
                continue
            i = own[-1]
            f = frames[i]
            site = sites.setdefault((f["uri"], f["line"], f["col"]), {"func": f["name"], "calls": set(), "vulns": set()})
            if i + 1 < len(frames):
                site["calls"].add(frames[i + 1]["name"])
            site["vulns"].add(frames[-1]["name"])
    return sorted(sites.items())


def found_versions(res):
    # "stdlib@v1.26.0" -> {"stdlib": "v1.26.0"}; our own module has no version and is skipped
    found = {}
    for flow in res.get("codeFlows", []):
        for thread in flow.get("threadFlows", []):
            for frame in thread.get("locations", []):
                module, _, version = frame.get("module", "").partition("@")
                if module and version:
                    found[module] = version
    return found


def version_key(version):
    return tuple(int(n) if n.isdigit() else 0 for n in version.lstrip("v").split("-")[0].split("."))


def fixed_versions(rule_id, found):
    # SARIF has no fixed versions, so they come from the vulnerability's OSV entry
    if not found:
        return []
    try:
        with urllib.request.urlopen(f"https://vuln.go.dev/ID/{rule_id}.json", timeout=10) as resp:
            osv = json.load(resp)
    except Exception:
        return []
    fixes = {}
    for affected in osv.get("affected", []):
        module = affected.get("package", {}).get("name")
        if module not in found:
            continue
        current = version_key(found[module])
        for rng in affected.get("ranges", []):
            for event in rng.get("events", []):
                fixed = event.get("fixed")
                if fixed and version_key(fixed) > current and (module not in fixes or version_key(fixed) < version_key(fixes[module])):
                    fixes[module] = fixed
    return [(module, found[module], "v" + fixed.lstrip("v")) for module, fixed in sorted(fixes.items())]


def level_of(res):
    return res.get("level") if res.get("level") in LEVELS else "note"


def build_items(results, rules):
    items = []
    for res in results:
        level = level_of(res)
        rule_id = res.get("ruleId", "Unknown Rule")
        rule = rules.get(rule_id, {})
        called = level == "error"
        items.append({
            "level": level,
            "id": rule_id,
            "title": rule.get("fullDescription", {}).get("text", ""),
            "url": rule.get("helpUri", "https://pkg.go.dev/vuln/" + rule_id),
            "aliases": ", ".join(rule.get("properties", {}).get("tags", [])),
            "message": res.get("message", {}).get("text", "No details provided"),
            "sites": call_sites(res) if called else [],
            "fixes": fixed_versions(rule_id, found_versions(res)) if called else [],
        })
    items.sort(key=lambda item: (ORDER.index(item["level"]), item["id"]))
    return items


def fix_text(fixes):
    return ", ".join(f"{module} {found} → {fixed}" for module, found, fixed in fixes)


def render_html(items, counts, props, driver):
    def render_sites(sites):
        rows = ""
        for (uri, line, col), site in sites:
            rows += f"""
                <tr>
                    <td><a href="{esc(blob_url + uri)}#L{line}"><code>{esc(uri)}:{line}:{col}</code></a></td>
                    <td><code>{esc(site["func"])}</code> calls <code>{esc(", ".join(sorted(site["calls"])))}</code></td>
                    <td><code>{"<br>".join(esc(v) for v in sorted(site["vulns"]))}</code></td>
                </tr>"""
        return f"""
            <details{" open" if len(sites) <= 10 else ""}>
                <summary>{len(sites)} call site(s) in your code</summary>
                <table>
                    <tr><th>Location</th><th>Call</th><th>Vulnerable function</th></tr>{rows}
                </table>
            </details>"""

    def render_card(item):
        level = item["level"]
        fix = f'<div class="fix">Fix: upgrade {esc(fix_text(item["fixes"]))}</div>' if item["fixes"] else ""
        return f"""
            <div class="card {level}">
                <div><span class="badge {level}">{esc(LEVELS[level][0])}</span> <a href="{esc(item["url"])}"><strong>{esc(item["id"])}</strong></a> <span class="meta">{esc(item["aliases"])}</span></div>
                <div class="title">{esc(item["title"])}</div>
                <div class="desc">{esc(item["message"])}</div>
                {fix}
                {render_sites(item["sites"]) if item["sites"] else ""}
            </div>"""

    if counts["error"] == 0:
        status = '<div class="clean">✓ No vulnerabilities found in code execution pathways!'
        if items:
            status += f'<div class="sub">{len(items)} finding(s) below are not called by your code.</div>'
        status += "</div>"
    else:
        status = f'<div class="alert">✗ Your code calls {counts["error"]} vulnerability(ies). See the call sites below.</div>'

    chips = "".join(f'<span class="chip {level}">{esc(LEVELS[level][0])}: {counts[level]}</span>' for level in ORDER)

    sections = ""
    for level in ORDER:
        cards = "".join(render_card(item) for item in items if item["level"] == level)
        if cards:
            sections += f'<h2>{esc(LEVELS[level][0])} ({counts[level]})</h2><p class="meta">{esc(LEVELS[level][1])}</p>{cards}'

    return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Go Vulncheck Security Report</title>
    <style>
        body {{ font-family: system-ui, sans-serif; margin: 40px; background: #fafafa; color: #222; }}
        .container {{ max-width: 1100px; margin: 0 auto; }}
        h1 {{ border-bottom: 2px solid #ddd; padding-bottom: 10px; }}
        h2 {{ margin-top: 32px; margin-bottom: 4px; }}
        a {{ color: #0969da; text-decoration: none; }}
        code {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.85em; }}
        .meta {{ color: #666; font-size: 0.9rem; }}
        .card {{ background: #fff; border: 1px solid #e0e0e0; border-left-width: 4px; border-radius: 6px; padding: 20px; margin-bottom: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.02); }}
        .card.error {{ border-left-color: #cf222e; }}
        .card.warning {{ border-left-color: #bc4c00; }}
        .card.note {{ border-left-color: #8c959f; }}
        .badge {{ color: white; padding: 4px 8px; font-size: 0.75rem; font-weight: bold; border-radius: 4px; text-transform: uppercase; }}
        .badge.error {{ background: #cf222e; }}
        .badge.warning {{ background: #bc4c00; }}
        .badge.note {{ background: #6e7781; }}
        .title {{ font-size: 1.15rem; font-weight: bold; margin-top: 10px; }}
        .desc {{ margin: 10px 0; color: #444; line-height: 1.5; }}
        .fix {{ margin: 10px 0; font-weight: bold; color: #1a7f37; }}
        .chip {{ display: inline-block; margin: 0 8px 8px 0; padding: 4px 10px; border-radius: 12px; font-size: 0.85rem; font-weight: bold; color: white; }}
        .chip.error {{ background: #cf222e; }}
        .chip.warning {{ background: #bc4c00; }}
        .chip.note {{ background: #6e7781; }}
        .clean {{ background: #dafbe1; color: #1a7f37; padding: 20px; border-radius: 6px; text-align: center; border: 1px solid #4ac26b; font-weight: bold; }}
        .alert {{ background: #ffebe9; color: #cf222e; padding: 20px; border-radius: 6px; text-align: center; border: 1px solid #ff8182; font-weight: bold; }}
        .sub {{ margin-top: 6px; font-weight: normal; color: #444; }}
        summary {{ cursor: pointer; font-weight: bold; margin-top: 10px; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 8px; font-size: 0.85rem; }}
        th, td {{ text-align: left; vertical-align: top; padding: 6px 8px; border-top: 1px solid #eee; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Go Vulnerability Scan Report</h1>
        <p class="meta">Generated on: {datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")}
            · Go: {esc(props.get("go_version", "unknown"))}
            · govulncheck: {esc(driver.get("semanticVersion", "unknown"))}
            · Vuln DB updated: {esc(props.get("db_last_modified", "unknown"))}
            · Commit: {esc(sha[:8])}</p>
        {status}
        <p>{chips}</p>
        {sections}
    </div>
</body>
</html>
"""


def render_markdown(items, counts, props, driver):
    called = [item for item in items if item["level"] == "error"]
    not_called = [item for item in items if item["level"] != "error"]

    head = [MARKER, "### 🛡️ Go Vulnerability Scan", ""]
    if called:
        head.append(f"❌ **Your code calls {len(called)} vulnerability(ies).** Fix the call sites below, usually by upgrading to the version in **Fix**.")
    else:
        head.append("✅ **No vulnerabilities found in code execution pathways.**" + (f" {len(not_called)} finding(s) are not called by your code." if not_called else ""))
    head += [
        "",
        "| " + " | ".join(LEVELS[level][0] for level in ORDER) + " |",
        "|" + ":-:|" * len(ORDER),
        "| " + " | ".join(str(counts[level]) for level in ORDER) + " |",
    ]

    tail = []
    if not_called:
        tail += ["", f"<details><summary>Not called by your code ({len(not_called)}), no fix required</summary>", ""]
        tail += [f"- [{item['id']}]({item['url']}) {md(item['title'])} *({LEVELS[item['level']][0].lower()})*" for item in not_called]
        tail += ["", "</details>"]
    tail += [
        "",
        f"<sub>Go {props.get('go_version', '?')} · govulncheck {driver.get('semanticVersion', '?')} · Vuln DB {props.get('db_last_modified', '?')}"
        f" · commit `{sha[:8]}` · [Full HTML report]({run_url}#artifacts)</sub>",
        "",
    ]

    body = []
    size = len("\n".join(head + tail))
    for n, item in enumerate(called):
        section = ["", f"#### ❌ [{item['id']}]({item['url']}) {md(item['title'])}"]
        if item["aliases"]:
            section.append(f"{item['aliases']}")
        if item["fixes"]:
            section.append(f"**Fix:** upgrade {md(fix_text(item['fixes']))}")
        section += ["", "| Location | Call | Vulnerable function |", "|---|---|---|"]
        for (uri, line, col), site in item["sites"][:MAX_SITES]:
            vulns = sorted(site["vulns"])
            more = f" +{len(vulns) - MAX_FUNCS} more" if len(vulns) > MAX_FUNCS else ""
            section.append(
                f"| [`{uri}:{line}`]({blob_url}{uri}#L{line}) | `{site['func']}` calls `{', '.join(sorted(site['calls']))}`"
                f" | `{', '.join(vulns[:MAX_FUNCS])}`{more} |"
            )
        if len(item["sites"]) > MAX_SITES:
            section += ["", f"…and {len(item['sites']) - MAX_SITES} more call site(s) in the full HTML report."]
        if size + len("\n".join(section)) > MAX_COMMENT:
            body += ["", f"…and {len(called) - n} more called vulnerability(ies) in the full HTML report."]
            break
        body += section
        size += len("\n".join(section))

    return "\n".join(head + body + tail)


def escape_data(value):
    # Workflow command escaping, see "Workflow commands for GitHub Actions"
    return str(value).replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def escape_property(value):
    return escape_data(value).replace(":", "%3A").replace(",", "%2C")


def annotate(items):
    # Shows the calling lines in the PR checks, and inline in "Files changed" when the line is part of the diff
    count = 0
    for item in items:
        for (uri, line, col), site in item["sites"]:
            if count == MAX_ANNOTATIONS:
                return
            message = (
                f"{site['func']} calls {', '.join(sorted(site['calls']))}, which reaches vulnerable "
                f"{', '.join(sorted(site['vulns']))}. {item['title']}"
                + (f" Fix: upgrade {fix_text(item['fixes'])}." if item["fixes"] else "")
                + f" {item['url']}"
            )
            print(f"::error file={escape_property(uri)},line={line},col={col},title={escape_property(item['id'] + ' is called')}::{escape_data(message)}")
            count += 1


def main():
    sarif = load_sarif(sys.argv[1] if len(sys.argv) > 1 else "vulns.sarif")
    run = (sarif.get("runs") or [{}])[0]
    driver = run.get("tool", {}).get("driver", {})
    props = driver.get("properties", {})
    rules = {r.get("id"): r for r in driver.get("rules", [])}
    items = build_items(run.get("results", []), rules)
    counts = {level: sum(1 for item in items if item["level"] == level) for level in ORDER}

    with open(HTML_OUT, "w", encoding="utf-8") as out:
        out.write(render_html(items, counts, props, driver))

    markdown = render_markdown(items, counts, props, driver)
    with open(MD_OUT, "w", encoding="utf-8") as out:
        out.write(markdown)

    if os.environ.get("GITHUB_ACTIONS") == "true":
        annotate(items)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as out:
            out.write(markdown)
    # The last workflow step fails the job when our code calls a vulnerable function
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
            out.write(f"called={counts['error']}\n")

    print(f"Called: {counts['error']}, imported: {counts['warning']}, required: {counts['note']}")


if __name__ == "__main__":
    main()
