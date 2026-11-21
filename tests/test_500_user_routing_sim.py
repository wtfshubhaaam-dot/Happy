"""In-memory routing simulation, NOT a real 500-user load benchmark."""
from pathlib import Path
import time
source = Path(__file__).with_name('test_multiserver.py').read_text()
exec(compile(source.split('# ================= config')[0], 'test_multiserver.py', 'exec'))
now = time.time()
C.router.users.clear()
C.router.down_until.clear()
C.router.nodes = {f'n{i}': {'hb': now, 'cap': 10, 'max_tasks': 10, 'max_streams': 8,
                                'tasks': 0, 'streams': 0, 'cpu': 0, 'ram': 0}
                  for i in range(1, 51)}
assigned = [C.router.route(i, now) for i in range(1, 501)]
counts = {nid: assigned.count(nid) for nid in C.router.nodes}
assert len(counts) == 50 and all(c == 10 for c in counts.values()), counts
print('PASS: simulation assigns 500 users across 50 nodes, 10 per node')
try:
    C.router.route(501, now)
except C.CapacityUnavailable:
    print('PASS: 501st user rejected when all 50 nodes have 10 assignments')
else:
    raise AssertionError('501st user unexpectedly admitted')
