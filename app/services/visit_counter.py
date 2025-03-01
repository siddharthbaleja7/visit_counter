import redis
import time
import hashlib
import threading
from ..core.config import settings

class VisitCounterService:
    """Visit Counter Service with Redis Sharding & Batching"""

    def __init__(self):
        """Initialize Redis shards and batch buffer"""
        redis_nodes = settings.REDIS_NODES.split(",")  # ✅ Get all Redis instances
        self.redis_shards = {
            f"redis_{node.split('//')[1].split(':')[0]}": redis.StrictRedis.from_url(node, decode_responses=True)
            for node in redis_nodes
        }
        self.batch_buffer = {}  # ✅ Store pending updates before flushing
        self.batch_interval = 30  # ✅ Flush every 30 seconds

        # ✅ Start background thread to flush batches
        self.flush_thread = threading.Thread(target=self.flush_to_redis, daemon=True)
        self.flush_thread.start()

    def _get_shard(self, page_id: str):
        """Get the correct Redis shard using consistent hashing"""
        shard_keys = list(self.redis_shards.keys())  # ✅ ["redis_7070", "redis_7071"]
        shard_index = int(hashlib.md5(page_id.encode()).hexdigest(), 16) % len(shard_keys)
        shard_name = shard_keys[shard_index]
        return self.redis_shards[shard_name], shard_name  # ✅ Return the correct Redis shard

    def increment_visit(self, page_id: str) -> None:
        """Increment visit count in batch buffer"""
        if page_id in self.batch_buffer:
            self.batch_buffer[page_id]["count"] += 1
        else:
            self.batch_buffer[page_id] = {"count": 1, "shard": self._get_shard(page_id)[1]}  # ✅ Store shard info

    def get_visit_count_with_source(self, page_id: str):
        """Get visit count from the correct shard + batch buffer"""
        redis_shard, shard_name = self._get_shard(page_id)

        # ✅ Check if value is in batch buffer
        batch_count = self.batch_buffer.get(page_id, {}).get("count", 0)

        # ✅ Fetch from correct Redis shard
        redis_count = redis_shard.get(page_id)
        redis_count = int(redis_count) if redis_count else 0

        total_count = redis_count + batch_count  # ✅ Correct total count

        # ✅ If batch has pending visits, serve from memory
        if batch_count > 0:
            return total_count, "in_memory"

        return total_count, shard_name  # ✅ Return the correct Redis shard

    def flush_to_redis(self):
        """Periodically flush batch buffer to Redis every 30 seconds"""
        while True:
            time.sleep(self.batch_interval)  # ✅ Wait for batch interval
            for page_id, data in self.batch_buffer.items():
                redis_shard, _ = self._get_shard(page_id)
                redis_shard.incrby(page_id, data["count"])  # ✅ Bulk update to Redis
            self.batch_buffer.clear()  # ✅ Clear the buffer after flushing
