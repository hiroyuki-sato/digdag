package io.digdag.client;

import java.net.InetSocketAddress;
import java.net.Proxy;
import com.treasuredata.client.ProxyConfig;
import com.google.common.base.Optional;

public class JavaNetProxyBuilder
{
    public static Proxy proxyConfigBuilder(Optional<ProxyConfig> config){
        if( config.isPresent() ){
            ProxyConfig c = config.get();
            return new Proxy(Proxy.Type.HTTP,new InetSocketAddress(c.getHost(),c.getPort()));
        } else {
            return Proxy.NO_PROXY;
        }
    }
}
