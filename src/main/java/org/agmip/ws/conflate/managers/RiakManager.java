package org.agmip.ws.conflate.managers;

import org.agmip.ws.conflate.config.RiakConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.raw.config.Configuration;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.yammer.dropwizard.lifecycle.Managed;

public class RiakManager implements Managed {
    private static final Logger LOG = LoggerFactory.getLogger(RiakManager.class);
    private RiakConfig config;
    private IRiakClient client;

    public RiakManager(RiakConfig config) {
        Configuration clientConfig;
        this.config = config;
        try {
        if (this.config.getClientType().equals("pb")) {
            clientConfig = new PBClientConfig.Builder().withHost(this.config.getHost()).withPort(this.config.getPort()).build();
        } else {
            clientConfig = new HTTPClientConfig.Builder().withHost(this.config.getHost()).withPort(this.config.getPort()).build();
        }
        this.client = RiakFactory.newClient(clientConfig);
        } catch (Exception ex) {
            LOG.error("Unable to comminucate with Riak cluster");
        }
    }

    public IRiakClient getClient() {
        return this.client;
    }
    
    @Override
    public void start() throws Exception {
        this.client.ping();
    }

    @Override
    public void stop() throws RiakException {
        this.client.shutdown();
    }

}
