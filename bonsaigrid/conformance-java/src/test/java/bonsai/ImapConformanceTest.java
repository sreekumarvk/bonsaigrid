package bonsai;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Behavioural conformance: the real Hazelcast Java client, pointed at a running
 * BonsaiGrid on 127.0.0.1:5701, executing ported IMap scenarios. Each passing
 * scenario contributes to the parity score.
 *
 * Prereq: JDK 17+ and a running `cargo run -p server`.
 */
class ImapConformanceTest {

    private HazelcastInstance client() {
        ClientConfig cfg = new ClientConfig();
        cfg.setClusterName("dev");
        cfg.getNetworkConfig().addAddress("127.0.0.1:5701");
        return HazelcastClient.newHazelcastClient(cfg);
    }

    @Test
    void put_then_get_returns_value() {
        HazelcastInstance client = client();
        try {
            IMap<String, String> map = client.getMap("m");
            assertNull(map.put("k", "v"));
            assertEquals("v", map.get("k"));
        } finally {
            client.shutdown();
        }
    }

    @Test
    void put_returns_previous_value() {
        HazelcastInstance client = client();
        try {
            IMap<String, String> map = client.getMap("m2");
            assertNull(map.put("k", "v1"));
            assertEquals("v1", map.put("k", "v2"));
            assertEquals("v2", map.get("k"));
        } finally {
            client.shutdown();
        }
    }

    @Test
    void get_absent_key_is_null() {
        HazelcastInstance client = client();
        try {
            assertNull(client.getMap("m3").get("missing"));
        } finally {
            client.shutdown();
        }
    }
}
