#!/usr/bin/env bash
# Merge every ci/tpcds/results/<LABEL>/queries.tsv into a side-by-side markdown
# comparison (per-query seconds + total). With exactly two labels it also adds a
# delta% column (positive = the second label is slower). Writes
# ci/tpcds/results/comparison.md.
#
#   ./compare.sh                 # compare all labels found under results/
#   ./compare.sh adb-8.x claude-merge-7   # order/subset the labels explicitly
set -euo pipefail
cd "$(dirname "$0")/results"

python3 - "$@" <<'PY'
import os, sys, glob

want = sys.argv[1:]
labels = want or sorted(d.split('/')[0] for d in glob.glob('*/queries.tsv'))
labels = [l for l in labels if os.path.isfile(f'{l}/queries.tsv')]
if not labels:
    sys.exit("no results/<LABEL>/queries.tsv found — run ./run.sh first")

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

def num(x):
    try: return float(x)
    except: return None

out = ['# TPC-DS comparison', '', 'Labels: ' + ', '.join(f'`{l}`' for l in labels), '']
header = ['Query'] + labels + (['Δ%% (%s vs %s)' % (labels[1], labels[0])] if len(labels) == 2 else [])
out.append('| ' + ' | '.join(header) + ' |')
out.append('|' + '|'.join(['---'] + ['---:'] * (len(header) - 1)) + '|')

def row(name, getter):
    cells = [name]
    vals = [getter(l) for l in labels]
    cells += [('' if v is None else f'{v:.2f}') for v in vals]
    if len(labels) == 2 and vals[0] and vals[1]:
        cells.append(f'{(vals[1]-vals[0])/vals[0]*100:+.1f}%')
    elif len(labels) == 2:
        cells.append('')
    return '| ' + ' | '.join(cells) + ' |'

for qn in queries:
    out.append(row(qn, lambda l, qn=qn: num(data[l].get(qn))))

# totals
def total(l):
    s = sum(num(v) or 0 for v in data[l].values())
    return s if data[l] else None
out.append(row('**TOTAL**', total))

open('comparison.md', 'w').write('\n'.join(out) + '\n')
print('wrote comparison.md for labels:', ', '.join(labels))
PY
