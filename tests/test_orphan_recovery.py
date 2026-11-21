"""Isolated dead-node selection; integration with real MongoDB remains untested."""
from pathlib import Path
src = Path(__file__).with_name('test_multiserver.py').read_text()
exec(compile(src.split('# ================= config')[0], 'test_multiserver.py', 'exec'))
now=1000
nodes=[{'_id':'n1','hb':900},{'_id':'n2','hb':990},{'_id':'n3','hb':950},{'_id':'n4'}]
assert C.orphaned_nodes(nodes,now)==['n3'] if C.config.NODE_ID=='n1' else True
assert C.orphaned_nodes([{'_id':'n2','hb':990}],now)==[]
print('PASS: stale known node selection and healthy node exclusion')
