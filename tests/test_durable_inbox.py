"""Targeted isolated tests; no live MongoDB or Telegram API."""
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock
src = Path(__file__).with_name('test_multiserver.py').read_text()
exec(compile(src.split('# ================= config')[0], 'test_multiserver.py', 'exec'))

async def main():
    col = C.dbm.cl_inbox_col
    col.update_one = AsyncMock()
    await C._enqueue_durable({'update_id': 777, 'message': {}}, 'n1')
    args, kwargs = col.update_one.call_args
    assert args[0] == {'_id': 777}
    assert kwargs['upsert'] is True
    assert args[1]['$setOnInsert']['node'] == 'n1'
    assert args[1]['$setOnInsert']['state'] == 'queued'
    try:
        await C._enqueue_durable({'update_id': 'bad'}, 'n1')
    except ValueError:
        pass
    else:
        raise AssertionError('bad update id accepted')
    C.router.target = lambda _: C.config.NODE_ID
    await C.dispatch({'update_id': 778})
    assert col.update_one.await_count == 2
    print('PASS: durable write-before-ack, global idempotency key, input validation, local dispatch')

asyncio.run(main())
