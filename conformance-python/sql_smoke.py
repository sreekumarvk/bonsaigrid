"""SQL over Compact IMap values via a stock client.

Puts Person Compact records into an IMap, then runs
`SELECT name, age FROM people WHERE age > 30` and checks the projected rows.
Columns come back as text (VARCHAR) in this MVP.
"""
import sys

import hazelcast
from hazelcast.serialization.api import CompactSerializer


class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age


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


def rows_of(sql, client):
    result = client.sql.execute(sql).result()
    out = []
    for row in result:
        out.append((row.get_object("name"), row.get_object("age")))
    return out


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

    older = sorted(rows_of("SELECT name, age FROM people WHERE age > 30", c))
    assert older == [("alice", "35"), ("carol", "45")], older

    one = rows_of("SELECT name, age FROM people WHERE name = 'bob'", c)
    assert one == [("bob", "25")], one

    allrows = sorted(rows_of("SELECT name, age FROM people", c))
    assert allrows == [("alice", "35"), ("bob", "25"), ("carol", "45")], allrows

    none = rows_of("SELECT name, age FROM people WHERE age > 100", c)
    assert none == [], none

    print("SQL SMOKE OK")
    c.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
