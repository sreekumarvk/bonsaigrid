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

    /** TPC-enabled client: connects to the per-core TPC ports and routes each
     *  partition to its owning core. Validates the increment-3 TPC path. */
    @Test
    void tpc_put_then_get_returns_value() {
        ClientConfig cfg = new ClientConfig();
        cfg.setClusterName("dev");
        cfg.getNetworkConfig().addAddress("127.0.0.1:5701");
        cfg.getTpcConfig().setEnabled(true);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(cfg);
        try {
            IMap<String, String> map = client.getMap("tpcmap");
            for (int i = 0; i < 50; i++) {
                map.put("tk" + i, "tv" + i);
            }
            for (int i = 0; i < 50; i++) {
                assertEquals("tv" + i, map.get("tk" + i));
            }
        } finally {
            client.shutdown();
        }
    }
}
