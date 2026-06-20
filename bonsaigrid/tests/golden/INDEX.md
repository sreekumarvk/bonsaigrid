# Golden protocol fixture

`2.10.protocol.compatibility.binary` is copied verbatim from Apache Hazelcast
(`hazelcast/hazelcast/src/test/resources/2.10.protocol.compatibility.binary`,
Apache-2.0). It is a concatenation of 985 complete client messages, in the order
of the `test_*` methods in
`hazelcast/.../client/protocol/compatibility/ClientCompatibilityTest_2_10.java`.
The canonical field values each message encodes are in the sibling
`ReferenceObjects.java`.

## How BonsaiGrid tests consume it

We do **not** rely on positional indices (brittle). Instead we locate a message
by its **message-type field** (`type` @ offset 0 of the initial frame), which is
unique per request/response/event:

| Codec | Request type | Response type | Event types |
|-------|-------------:|--------------:|-------------|
| ClientAuthentication | 256 | 257 | — |
| ClientAddClusterViewListener | 768 | 769 | members 770, partitions 771, member-groups 772, cluster-version 773 |
| MapPut | 65792 | 65793 | — |
| MapGet | 66048 | 66049 | — |

A test helper `message_of_type(t)` scans all parsed messages and returns the one
whose initial-frame type equals `t`. Our encoder, fed the `ReferenceObjects`
values, must reproduce that message's bytes exactly; our decoder, fed that
message, must recover the `ReferenceObjects` values.
