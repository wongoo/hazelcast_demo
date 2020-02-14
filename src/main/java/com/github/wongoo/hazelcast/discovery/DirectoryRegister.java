package com.github.wongoo.hazelcast.discovery;

import com.github.wongoo.hazelcast.HazelcastNetworkConfig;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * @author wongoo
 */
public class DirectoryRegister {

    public static final String DIRECTORY = "/tmp/hazelcast_members";

    public static void Register(String ip, int port) throws Exception {
        System.out.println("register " + ip + ":" + port);
        new File(DIRECTORY).mkdirs();
        File file = new File(DIRECTORY + File.separator + ip + ":" + port);
        file.createNewFile();
        file.deleteOnExit();
    }

    public static void Unregister(String ip, int port) throws Exception {
        System.out.println("unregister " + ip + ":" + port);
        new File(DIRECTORY + File.separator + ip + ":" + port).delete();
    }

    public static Set<String> list() {
        String localIp = HazelcastNetworkConfig.getLocalIp();
        int localPort = HazelcastNetworkConfig.getLocalPort();
        String localHost = localIp + ":" + localPort;

        Set<String> hosts = new HashSet<String>(4);

        File dir = new File(DIRECTORY);

        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            for (File f : files) {
                if (f.isFile()) {
                    String host = f.getName();
                    if (host.equals(localHost)) {
                        continue;
                    }

                    if (host.split(":").length != 2) {
                        continue;
                    }

                    hosts.add(host);
                }
            }
        }

        return hosts;
    }
}
