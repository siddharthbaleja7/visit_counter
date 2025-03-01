import redis
from .consistent_hash import ConsistentHash
from .config import settings

class RedisManager:
    def __init__(self):
        redis_nodes = [node.strip() for node in settings.REDIS_NODES.split(",") if node.strip()]
        self.consistent_hash = ConsistentHash(redis_nodes, settings.VIRTUAL_NODES)
        self.redis_clients = {node: redis.StrictRedis.from_url(node, decode_responses=True) for node in redis_nodes}

    def get_connection(self, key: str) -> redis.Redis:
        node = self.consistent_hash.get_node(key)
        return self.redis_clients[node]

    def increment(self, key: str, amount: int = 1) -> int:
        redis_client = self.get_connection(key)
        return redis_client.incrby(key, amount)

    def get(self, key: str) -> int:
        redis_client = self.get_connection(key)
        value = redis_client.get(key)
        return int(value) if value else 0
