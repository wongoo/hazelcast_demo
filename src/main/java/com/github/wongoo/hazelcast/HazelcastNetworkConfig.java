package com.github.wongoo.hazelcast;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

/**
 * @author wongoo
 */
public class HazelcastNetworkConfig {

    private static final String NET_CARD_0_NAME = "eth0";
    private static final String OS_LINUX = "linux";

    private static String localIp = null;

    private static int localPort;

    public static int getLocalPort() {
        return localPort;
    }

    public static void setLocalPort(int localPort) {
        HazelcastNetworkConfig.localPort = localPort;
    }

    private static boolean isLinux() {
        String os = System.getProperty("os.name");
        return os != null && os.toLowerCase().contains(OS_LINUX);
    }

    public static String getLocalIp() {
        try {
            if (localIp != null) {
                return localIp;
            }
            synchronized (HazelcastNetworkConfig.class) {
                Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();
                String ip = null;

                while (netInterfaces.hasMoreElements()) {
                    NetworkInterface ni = netInterfaces.nextElement();
                    Enumeration ips = ni.getInetAddresses();

                    while (ips.hasMoreElements()) {
                        InetAddress ipObj = (InetAddress) ips.nextElement();
                        if (ipObj.isSiteLocalAddress()) {
                            ip = ipObj.getHostAddress();
                            if (!isLinux()) {
                                localIp = ip;
                                return ip;
                            } else if (ni.getName().equals(NET_CARD_0_NAME)) {
                                localIp = ip;
                                return ip;
                            }
                        }
                    }
                }
                return ip;
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return null;
        }
    }
}
