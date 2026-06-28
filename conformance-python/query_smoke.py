"""Compact serialization + predicate queries via a stock client.

Puts Compact `Person` records into a map, then queries with equal/greater/and
predicates over `values`, `key_set`, and `entry_set`. The server replicates the
schema (ClientSendSchema), reads the big-endian Compact records, decodes the
IdentifiedDataSerializable predicates, full-scans, and returns matching Data.
"""
import sys

import hazelcast
from hazelcast import predicate
from hazelcast.serialization.api import CompactSerializer


class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def __repr__(self):
        return f"Person({self.name}, {self.age})"


class PersonSerializer(CompactSerializer):
    def read(self, reader):
        return Person(reader.read_string("name"), reader.read_int32("age"))

    def write(self, writer, obj):
        writer.write_string("name", obj.name)
        writer.write_int32("age", obj.age)

    def get_type_name(self):
        return "person"

    def get_class(self):
        return Person


def main() -> int:
    c = hazelcast.HazelcastClient(
        cluster_name="dev",
        cluster_members=["127.0.0.1:5701"],
        cluster_connect_timeout=10.0,
        compact_serializers=[PersonSerializer()],
    )

    m = c.get_map("people").blocking()
    m.put("a", Person("alice", 35))
    m.put("b", Person("bob", 25))
    m.put("c", Person("carol", 45))

    # values(age > 30) -> alice(35), carol(45)
    older = {p.name for p in m.values(predicate.greater("age", 30))}
    assert older == {"alice", "carol"}, older

    # values(age == 25) -> bob
    eq = [p.name for p in m.values(predicate.equal("age", 25))]
    assert eq == ["bob"], eq

    # key_set(name == "alice") -> {"a"}
    keys = set(m.key_set(predicate.equal("name", "alice")))
    assert keys == {"a"}, keys

    # entry_set(age >= 35 AND name == "carol") -> [("c", carol)]
    entries = m.entry_set(
        predicate.and_(predicate.greater_or_equal("age", 35), predicate.equal("name", "carol"))
    )
    assert len(entries) == 1, entries
    k, v = entries[0]
    assert k == "c" and v.name == "carol" and v.age == 45, entries

    # entry_set(age > 100) -> empty
    assert m.entry_set(predicate.greater("age", 100)) == []

    print("QUERY SMOKE OK")
    c.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
