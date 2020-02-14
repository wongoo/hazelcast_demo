package com.github.wongoo.hazelcast.discovery;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;

import java.util.Collection;
import java.util.Map;

/**
 * @author wongoo
 */
public class DirectoryDiscoveryStrategyFactory implements DiscoveryStrategyFactory {

    @Override
    public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
        return DirectoryDiscoveryStrategy.class;
    }

    @Override
    public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger, Map<String, Comparable> properties) {
        return new DirectoryDiscoveryStrategy(logger, properties);
    }

    @Override
    public Collection<PropertyDefinition> getConfigurationProperties() {
        return null;
    }
}
