package util;

import org.apache.log4j.Logger;

import com.nearinfinity.honeycomb.mysql.Bootstrap;
import com.nearinfinity.honeycomb.mysql.HandlerProxy;
import com.nearinfinity.honeycomb.mysql.HandlerProxyFactory;

/**
 * Configures the Honeycomb environment required to use a {@link HandlerProxy}
 */
public final class HoneycombEnvironment {
    private static final Logger log = Logger.getLogger(HoneycombEnvironment.class);

    private HandlerProxyFactory factory;
    private HandlerProxy proxy;

    public HandlerProxy setupEnvironment() {
        if( factory == null ) {
            factory = Bootstrap.startup("/usr/share/mysql/honeycomb/honeycomb-test.xml", "/usr/share/mysql/honeycomb/honeycomb.xsd");
        }

        if( proxy == null ) {
            proxy = factory.createHandlerProxy();
        }

        log.debug("Ignoring command line configuration arguments; using value from specified XML file");

        return proxy;
    }
}
