"""No live Telegram required: admission accounting and rollback tests."""
from pathlib import Path
source = Path(__file__).with_name("test_multiserver.py").read_text()
exec(compile(source.split("# ================= config")[0], "test_multiserver.py", "exec"))

C._draining = False
C.live.reserved = 0
C._active_user_slots.clear()
C._admitted_ids.clear()
C._seen_set.clear()
C._processing_ids.clear()
C._spawn = lambda coro: coro.close()  # keep reservations, don't run update
def update(user, seq):
    return {"update_id": seq, "message": {"from": {"id": user}, "chat": {"type": "private"}}}

for user in range(1, 11):
    assert C._admit_local(update(user, user))
assert len(C._active_user_slots) == 10
assert C.live.reserved == 10
assert not C._admit_local(update(11, 11))
assert not C._admit_local(update(1, 1))
assert len(C._active_user_slots) == 10
print("PASS: 10 distinct in-flight users admitted; 11th denied; duplicates denied")
C.live.reserved = 0
C._active_user_slots.clear()
C._admitted_ids.clear()
def fail_spawn(coro):
    coro.close()
    raise RuntimeError("spawn failed")
C._spawn = fail_spawn
try:
    C._admit_local(update(123, 123))
except RuntimeError:
    pass
else:
    raise AssertionError("expected spawn failure")
assert C.live.reserved == 0 and not C._active_user_slots and not C._admitted_ids
print("PASS: failed spawn rolls back user reservation")
assert C.config.NODE_HARD_TASKS <= 10
print("PASS: hard task limit <=10")
