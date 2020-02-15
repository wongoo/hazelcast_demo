package com.github.wongoo.hazelcast;

import com.github.wongoo.hazelcast.discovery.DirectoryDiscoveryStrategyFactory;
import com.github.wongoo.hazelcast.discovery.DirectoryRegister;
import com.hazelcast.cluster.Member;
import com.hazelcast.collection.IQueue;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.util.concurrent.TimeUnit;

/**
 * @author wongoo
 */
public class HazelcastServer {

    private static final String MERGE_POLICY = "HigherHitsMergePolicy";
    private static final String CLUSTER_NAME = "hazelcast-demo";
    private static final String MAP_NAME = "my-map";
    private static final String TASK_QUEUE = "my-task";

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

        loopOutputInfo(instance);
    }


    private static Config createHazelcastConfig() throws Exception {
        return new Config()
                .setProperty("hazelcast.discovery.enabled", "true")
                .setClusterName(CLUSTER_NAME)
                .setNetworkConfig(createNetworkConfig())
                .addMapConfig(createMapConfig())
                .addQueueConfig(new QueueConfig(TASK_QUEUE));
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

        DirectoryRegister.register(HazelcastNetworkConfig.getLocalIp(), HazelcastNetworkConfig.getLocalPort());
    }


    private static void loopOutputInfo(HazelcastInstance instance) throws Exception {
        IMap<Object, Object> map = instance.getMap(MAP_NAME);
        int index = 0;

        while (true) {
            Member localMember = instance.getCluster().getLocalMember();
            Member masterMember = instance.getCluster().getMembers().iterator().next();
            IQueue<String> taskQueue = instance.getQueue(TASK_QUEUE);

            if (localMember.equals(masterMember)) {
                String task = (index++) + ":" + localMember.getSocketAddress().toString();
                System.out.println("send task: " + task);

                // task provider
                taskQueue.put(task);
            } else {
                // message get
                Object message = map.get("message");
                System.out.println("read message: " + message);

                // task consumer
                String task = taskQueue.poll(5, TimeUnit.SECONDS);
                if (task != null) {
                    System.out.println("receive task: " + task);

                    // message set
                    map.put("message", "result of task: " + task);
                }
            }

            Thread.sleep(5 * 1000);
        }
    }

}
