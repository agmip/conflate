package org.agmip.ws.conflate.health;

import com.basho.riak.client.IRiakClient;
import com.yammer.metrics.core.HealthCheck;

public class RiakHealthCheck extends HealthCheck {
    private IRiakClient client;
    
    public RiakHealthCheck(IRiakClient client)  {
        super("riak");
        this.client = client;
    }
    
    @Override
    protected Result check() throws Exception {
        this.client.ping();
        return Result.healthy();
    }
}
