package com.github.wongoo.hazelcast;

import com.github.wongoo.hazelcast.discovery.DirectoryDiscoveryStrategyFactory;
import com.github.wongoo.hazelcast.discovery.DirectoryRegister;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

/**
 * @author wongoo
 */
public class HazelcastServer {

    private static final String MERGE_POLICY = "HigherHitsMergePolicy";
    private static final String CLUSTER_NAME = "hazelcast-demo";
    private static final String MAP_NAME = "my-map";
    private static final String MASTER_NAME = "master";

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("require name and port");
            return;
        }

        String name = args[0];
        int port = Integer.parseInt(args[1]);
        System.out.println(name + " start at " + port);
        HazelcastNetworkConfig.setLocalPort(port);

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(createHazelcastConfig());

        IMap<Object, Object> map = instance.getMap(MAP_NAME);

        if (MASTER_NAME.equals(name)) {
            loopWrite(map);
        } else {
            loopRead(map);
        }
    }

    private static Config createHazelcastConfig() throws Exception {
        return new Config()
                .setProperty("hazelcast.discovery.enabled", "true")
                .setClusterName(CLUSTER_NAME)
                .setNetworkConfig(createNetworkConfig())
                .addMapConfig(createMapConfig());
    }

    private static MapConfig createMapConfig() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(MERGE_POLICY)
                .setBatchSize(100);

        MapConfig mapConfig = new MapConfig(MAP_NAME)
                .setMergePolicyConfig(mergePolicyConfig);

        return mapConfig;
    }

    private static NetworkConfig createNetworkConfig() throws Exception {
        NetworkConfig networkConfig = new NetworkConfig();
        networkConfig.setPort(HazelcastNetworkConfig.getLocalPort());

        configDiscoverySPI(networkConfig);

        return networkConfig;
    }

    /**
     * see: https://docs.hazelcast.org/docs/latest/manual/html-single/#discovery-spi
     *
     * @param networkConfig
     * @throws Exception
     */
    private static void configDiscoverySPI(NetworkConfig networkConfig) throws Exception {
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.getJoin().getAwsConfig().setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(false);

        DiscoveryStrategyConfig discoveryStrategyConfig = new DiscoveryStrategyConfig();
        discoveryStrategyConfig.setDiscoveryStrategyFactory(new DirectoryDiscoveryStrategyFactory());
        networkConfig.getJoin().getDiscoveryConfig().addDiscoveryStrategyConfig(discoveryStrategyConfig);

        DirectoryRegister.Register(HazelcastNetworkConfig.getLocalIp(), HazelcastNetworkConfig.getLocalPort());
    }

    private static void loopRead(IMap<Object, Object> map) throws Exception {
        while (true) {
            Object message = map.get("message");
            System.out.println("read message: " + message);

            Thread.sleep(5 * 1000);
        }
    }

    private static void loopWrite(IMap<Object, Object> map) throws Exception {
        int index = 0;
        while (true) {
            String message = "hello " + (index++);
            map.put("message", message);
            System.out.println("write message: " + message);

            Thread.sleep(5 * 1000);
        }
    }
}
