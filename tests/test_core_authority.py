"""Authoritative counters stay on core before/after secondary failures."""
from pathlib import Path
source = Path(__file__).with_name('test_mongo_router.py').read_text()
exec(compile(source.split('# ================= single cluster')[0], 'mongo_stubs', 'exec'))
import asyncio
config, mongo, db = fresh({'MONGO_URI_2':'mongodb://hot','MONGO_URI_3':'mongodb://logs','MONGO_ROUTES':'daily_stats=hot,daily_usage=hot,ad_sessions=hot,pending_deletes=hot'})
critical = ('daily_usage','ad_sessions','pending_deletes','daily_stats')
assert all(mongo.group_of(c) == 'core' for c in critical)
assert all(mongo.pick(c).name == 'core' for c in critical)
async def test():
    for name in critical:
        await mongo.col(name).update_one({'_id':'test'}, {'$inc': {'count':1}})
    assert all(c in [e[2] for e in FakeClient.instances['mongodb://core'].log] for c in critical)
    assert not FakeClient.instances['mongodb://hot'].log
    mongo.clusters['hot'].mark_down(RuntimeError('down'))
    for name in critical:
        assert mongo.pick(name).name == 'core'
    mongo.clusters['hot'].mark_up()
    for name in critical:
        assert mongo.pick(name).name == 'core'
    print('PASS: critical counters and sessions stay on core before, during, after hot outage')
asyncio.run(test())
