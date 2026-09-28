"""Render the rowcounts API answer as a table."""
import json
import sys

d = json.load(sys.stdin)
if not d.get("success"):
    print("failed:", d.get("errorMessage", d))
    raise SystemExit(1)

x = d["data"]
print("engine=%s  discovered=%s  objects=%d  differing=%d\n"
      % (x["engine"], x["discovered"], len(x["objects"]), x["differing"]))
print("%-44s%12s%12s" % ("object", "source", "target"))

for o in x["objects"]:
    source = o.get("sourceRows", "?")
    target = o.get("targetRows", "?")
    name = o["source"] if o["source"] == o["target"] else "%s -> %s" % (o["source"], o["target"])
    mark = "" if o["agrees"] else "   <-- DIFFERS"
    print("%-44s%12s%12s%s" % (name, source, target, mark))
    if o.get("note"):
        print("    note: %s" % o["note"])
