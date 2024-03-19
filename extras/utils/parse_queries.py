import re
def get_queries_from_file_format_orig(f):
    with open(f, "r") as of:
        raw_queries = of.read()
        file_headers, file_queries = _parse_queries(raw_queries)
    queries = [e for e in zip(file_queries, file_headers)]
    return queries

def get_queries_from_file_format_semi(f, filter_comment_lines=False):
    fc = None
    queries = []
    with open(f, "r") as of:
        fc = of.read()
    for idx, q in enumerate(fc.split(";")):
        q = q.strip()
        if not q:
            continue
        # Keep non-empty lines.
        # Also keep or remove comments depending on the flag.
        rq = [
            l
            for l in q.split("\n")
            if l.strip() and not (filter_comment_lines and l.startswith("--"))
        ]
        if rq:
            queries.append(
                (
                    "\n".join(rq),
                    f"query{idx}",
                )
            )
    return queries

def _parse_queries(raw_queries):
    split_raw = re.split(r"(Q\d+\n+)", raw_queries)[1:]
    split_clean = list(map(str.strip, split_raw))
    headers = split_clean[::2]
    queries = split_clean[1::2]
    return headers, queries

if __name__ == "__main__":
    queries = get_queries_from_file_format_orig("queries/benchmark_core_monthly.sql")
    print(queries)