package com.github.wongoo.hazelcast.discovery;

import com.hazelcast.cluster.Address;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wongoo
 */
public class DirectoryDiscoveryStrategy extends AbstractDiscoveryStrategy {

    public DirectoryDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
        super(logger, properties);
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        Collection<DiscoveryNode> discoveredNodes = new ArrayList<DiscoveryNode>();
        for (String host : DirectoryRegister.list()) {
            String[] arr = host.split(":");
            if (arr.length != 2) {
                continue;
            }

            try {
                InetAddress inetAddress = mapToInetAddress(arr[0]);
                Address address = new Address(inetAddress, Integer.parseInt(arr[1]));
                discoveredNodes.add(new SimpleDiscoveryNode(address, new HashMap<String, String>(0)));
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        return discoveredNodes;
    }

    private InetAddress mapToInetAddress(String address) {
        try {
            return InetAddress.getByName(address);
        } catch (UnknownHostException e) {
            throw new RuntimeException("Could not resolve ip address", e);
        }
    }
}
