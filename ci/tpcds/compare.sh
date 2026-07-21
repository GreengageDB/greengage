#!/usr/bin/env bash
# Build a side-by-side markdown comparison across versions from
# ci/tpcds/results/<LABEL>/queries.tsv (per-query single-user seconds) and the
# headline metrics in ci/tpcds/results/tpcds_<LABEL>.md. Emits, into
# ci/tpcds/results/comparison.md:
#   1. metric definitions,
#   2. a headline-metrics summary table (rows/load/analyze/single/multi/score),
#   3. the per-query single-user table.
# For each label after the first it adds a successive Δ% column (label_i vs
# label_i-1) so a 3-version run shows the version-over-version progression.
#
#   ./compare.sh                       # all labels found under results/
#   ./compare.sh gg7 gg8 gg9           # order/subset the labels explicitly
set -euo pipefail
cd "$(dirname "$0")/results"

# Capture the host / Docker / cluster environment for the report (best-effort).
export ENV_DOCKER=$(docker --version 2>/dev/null || sudo -n docker --version 2>/dev/null || echo 'n/a')
export ENV_CPU=$(lscpu 2>/dev/null | awk -F: '/^Model name/{gsub(/^ +/,"",$2);print $2;exit}')
export ENV_CPU_LOGICAL=$(nproc 2>/dev/null || echo '?')
export ENV_CPU_CORES=$(lscpu 2>/dev/null | awk -F: '/^Core\(s\) per socket/{gsub(/ /,"",$2);c=$2}/^Socket\(s\)/{gsub(/ /,"",$2);s=$2}END{if(c&&s)print c*s}')
export ENV_RAM=$(free -h 2>/dev/null | awk '/^Mem:/{print $2}')
export ENV_OS=$(. /etc/os-release 2>/dev/null; echo "$PRETTY_NAME")
export ENV_KERNEL=$(uname -sr 2>/dev/null)
export ENV_DISK=$(lsblk -d -o ROTA,SIZE,MODEL 2>/dev/null | awk 'NR==2{rota=$1;size=$2;$1="";$2="";sub(/^ +/,"");printf "%s (%s, %s)", $0, size, (rota=="0"?"SSD":"HDD")}')
export ENV_TPCDS_REF=$(grep -h 'harness pinned' ../logs/*run*.log 2>/dev/null | tail -1 | awk '{print $NF}')

python3 - "$@" <<'PY'
import os, sys, glob, re

want = sys.argv[1:]
labels = want or sorted(d.split('/')[0] for d in glob.glob('*/queries.tsv'))
labels = [l for l in labels if os.path.isfile(f'{l}/queries.tsv')]
if not labels:
    sys.exit("no results/<LABEL>/queries.tsv found — run ./run.sh first")

# --- per-query single-user timings from queries.tsv ---
data, queries = {}, []
for l in labels:
    data[l] = {}
    for line in open(f'{l}/queries.tsv'):
        parts = line.rstrip('\n').split('\t')
        if len(parts) < 2 or not parts[0]:
            continue
        k, v = parts[0], parts[1]
        data[l][k] = v
        if k not in queries:
            queries.append(k)
queries.sort()

# --- headline metrics + metadata from tpcds_<label>.md ---
def read_report(label):
    p = f'tpcds_{label}.md'
    t = open(p).read() if os.path.isfile(p) else ''
    def g(pat):
        m = re.search(pat, t)
        return m.group(1).strip() if m else None
    return {
        'version':   g(r'\*\*Version:\*\*\s*(.+)'),
        'scale':     g(r'\*\*Scale factor:\*\*\s*(.+)'),
        'segs':      g(r'\*\*Primary segments:\*\*\s*(.+)'),
        'optimizer': g(r'\*\*Optimizer:\*\*\s*(.+)'),
        'rows':      g(r'\|\s*Rows loaded\s*\|\s*([\d.]+|n/a)\s*\|'),
        'load':      g(r'\|\s*Load time \(s\)\s*\|\s*([\d.]+|n/a)\s*\|'),
        'analyze':   g(r'\|\s*Analyze time \(s\)\s*\|\s*([\d.]+|n/a)\s*\|'),
        'single':    g(r'\|\s*Single-user query total \(s\)\s*\|\s*([\d.]+|n/a)\s*\|'),
        'multi':     g(r'\|\s*Multi-user query total \(s\)\s*\|\s*([\d.]+|n/a)\s*\|'),
        'score':     g(r'\*\*TPC-DS Score\*\*\s*\|\s*\*\*(.+?)\*\*'),
    }
rep = {l: read_report(l) for l in labels}

def num(x):
    try: return float(x)
    except: return None

delta_pairs = [(labels[i-1], labels[i]) for i in range(1, len(labels))]

def short_ver(v):
    if not v: return 'n/a'
    m = re.match(r'(PostgreSQL \S+).*?\(([^)]*build)', v)
    return m.group(1) if m else v[:40]

out = []
out.append('# TPC-DS comparison')
out.append('')
out.append('Versions (in order):')
for l in labels:
    out.append(f'- **`{l}`** — {rep[l]["version"] or "n/a"}')
r0 = rep[labels[0]]
out.append('')
out.append(f'Scale factor **{r0["scale"] or "?"}** · **{r0["segs"] or "?"}** primary segments · '
           f'optimizer **{r0["optimizer"] or "?"}** (ORCA) · 3 concurrent streams (multi-user).')
out.append('')
out.append('Δ% columns are **successive** (each version vs the one to its left). '
           'For time metrics, positive = slower (worse); for **Score**, higher = better.')
out.append('')

# --- test environment ---
e = os.environ
cpu = e.get('ENV_CPU') or 'n/a'
cores, logical = e.get('ENV_CPU_CORES'), e.get('ENV_CPU_LOGICAL')
if cores and logical:
    cpu += f' ({cores} cores / {logical} threads)'
elif logical:
    cpu += f' ({logical} threads)'
out.append('## Test environment')
out.append('')
out.append(f'- **Host CPU:** {cpu}')
out.append(f'- **RAM:** {e.get("ENV_RAM") or "n/a"}')
out.append(f'- **Storage:** {e.get("ENV_DISK") or "n/a"}')
out.append(f'- **OS / kernel:** {e.get("ENV_OS") or "n/a"} · {e.get("ENV_KERNEL") or "n/a"}')
out.append(f'- **Docker:** {e.get("ENV_DOCKER") or "n/a"}')
out.append('- **Cluster:** one privileged Docker container per version = coordinator + 3 primary '
           'segments, no mirrors/standby; production (no-cassert, `-O3`, ORCA+LLVM) Greengage build; '
           'ORCA optimizer on.')
out.append('- **Container limits:** no CPU/memory cgroup caps (full host); `--shm-size=2g`; '
           '`kernel.sem=500 1024000 200 4096`; `nofile=65535`.')
out.append('- **Method:** versions run **strictly sequentially**, one container at a time, with a '
           'full teardown between runs, so every version sees identical resources. Data is generated '
           'deterministically by `dsdgen`; multi-user = 3 concurrent streams.')
if e.get('ENV_TPCDS_REF'):
    out.append(f'- **TPC-DS harness:** dimoffon/TPC-DS pinned @ `{e["ENV_TPCDS_REF"]}` (identical across versions).')
out.append('')

# --- metric definitions ---
out.append('## Metric definitions')
out.append('')
out.append('- **Rows loaded** — total rows loaded across all TPC-DS tables (7 fact + 17 dimension) '
           'at this scale factor. Generated deterministically by `dsdgen`, so it is identical across '
           'versions and confirms a fair comparison.')
out.append('- **Load time (s)** — cumulative time to bulk-load the generated flat data into the '
           'tables via `gpfdist` external-table `INSERT … SELECT` (sum of per-table load durations). '
           'Excludes data generation and ANALYZE.')
out.append('- **Analyze time (s)** — cumulative time running `ANALYZE` (optimizer statistics '
           'collection) over the loaded tables. Feeds the planner’s cardinality estimates.')
out.append('- **Single-user query total (s)** — sum of the execution times of the 99 TPC-DS queries '
           'run **one at a time** (the *power* run), taking the fastest time per query across '
           'iterations. Measures per-query latency with no concurrency.')
out.append('- **Multi-user query total (s)** — sum of query execution times across the **3 concurrent '
           'query streams** (the *throughput* run), each stream running the full 99-query set at the '
           'same time. Measures behaviour under concurrency; a higher total reflects more '
           'contention / disk spill.')
out.append('- **TPC-DS Score** — the harness’s headline score from its scoring step (`09_score`), '
           'derived from the power and throughput results at this scale and stream count; **higher is '
           'better**. This is the pivotalguru-style harness score, **not** an audited TPC-DS '
           '`QphDS@SF` metric.')
out.append('')

# --- headline summary table ---
out.append('## Summary (headline metrics)')
out.append('')
hdr = ['Metric'] + labels + [f'Δ% ({b} vs {a})' for a, b in delta_pairs]
out.append('| ' + ' | '.join(hdr) + ' |')
out.append('|' + '|'.join(['---'] + ['---:'] * (len(hdr) - 1)) + '|')

def metric_row(name, key, fmt='{:.2f}', pct=True):
    cells = [name]
    vals = {l: num(rep[l].get(key)) for l in labels}
    for l in labels:
        v = vals[l]
        cells.append('n/a' if v is None else (fmt.format(v) if key != 'rows' else f'{int(v):,}'))
    for a, b in delta_pairs:
        va, vb = vals[a], vals[b]
        if pct and va and vb:
            cells.append(f'{(vb-va)/va*100:+.1f}%')
        elif not pct and va is not None and vb is not None:
            cells.append(f'{vb-va:+g}')
        else:
            cells.append('')
    return '| ' + ' | '.join(cells) + ' |'

out.append(metric_row('Rows loaded', 'rows', pct=False))
out.append(metric_row('Load time (s)', 'load'))
out.append(metric_row('Analyze time (s)', 'analyze'))
out.append(metric_row('Single-user total (s)', 'single'))
out.append(metric_row('Multi-user total (s)', 'multi'))
out.append(metric_row('TPC-DS Score', 'score', fmt='{:g}', pct=False))
out.append('')

# --- per-query single-user table ---
out.append('## Per-query single-user timings (s)')
out.append('')
out.append('Fastest time per query (across iterations). Per-query deltas can be noisy at '
           'qualification scale / single iteration — trust the totals and Score.')
out.append('')
qhdr = ['Query'] + labels + [f'Δ% ({b} vs {a})' for a, b in delta_pairs]
out.append('| ' + ' | '.join(qhdr) + ' |')
out.append('|' + '|'.join(['---'] + ['---:'] * (len(qhdr) - 1)) + '|')

def qrow(name, getter):
    cells = [name]
    vals = {l: getter(l) for l in labels}
    cells += [('' if vals[l] is None else f'{vals[l]:.2f}') for l in labels]
    for a, b in delta_pairs:
        va, vb = vals[a], vals[b]
        cells.append(f'{(vb-va)/va*100:+.1f}%' if (va and vb) else '')
    return '| ' + ' | '.join(cells) + ' |'

for qn in queries:
    out.append(qrow(qn, lambda l, qn=qn: num(data[l].get(qn))))
out.append(qrow('**TOTAL**', lambda l: sum(num(v) or 0 for v in data[l].values()) if data[l] else None))

open('comparison.md', 'w').write('\n'.join(out) + '\n')
print('wrote comparison.md for labels:', ', '.join(labels))
PY
