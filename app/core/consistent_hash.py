import hashlib
from typing import List, Dict
from bisect import bisect

class ConsistentHash:
    def __init__(self, nodes: List[str], virtual_nodes: int = 100):
        self.hash_ring: Dict[int, str] = {}
        self.sorted_keys = []
        self.virtual_nodes = virtual_nodes

        for node in nodes:
            for i in range(virtual_nodes):
                virtual_node_id = f"{node}-{i}"
                hash_value = self._hash(virtual_node_id)
                self.hash_ring[hash_value] = node
                self.sorted_keys.append(hash_value)

        self.sorted_keys.sort()

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def get_node(self, key: str) -> str:
        if not self.hash_ring:
            raise ValueError("No Redis nodes available")

        hash_value = self._hash(key)
        index = bisect(self.sorted_keys, hash_value)

        if index == len(self.sorted_keys):
            index = 0

        return self.hash_ring[self.sorted_keys[index]]
