package org.agmip.ws.conflate;

import org.agmip.ws.conflate.config.RiakConfig;
import org.agmip.ws.conflate.health.RiakHealthCheck;
import org.agmip.ws.conflate.managers.CacheManager;
import org.agmip.ws.conflate.managers.RiakManager;
import org.agmip.ws.conflate.resources.CacheResource;
import org.agmip.ws.conflate.resources.DatasetResource;
import org.agmip.ws.conflate.resources.MetadataResource;
import org.agmip.ws.conflate.resources.QueryResource;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class ConflateService extends Service<ConflateConfig> {
    //private static final Logger LOG = LoggerFactory.getLogger(ConflateService.class);
    public static void main(String[] args) throws Exception {
        new ConflateService().run(args);
    }

    @Override
    public void initialize(Bootstrap<ConflateConfig> bootstrap) {
        bootstrap.setName("conflate");
    }

    @Override
    public void run(ConflateConfig config, Environment env) {
        final RiakConfig riak = config.getRiakConfig();
        

        RiakManager riakConnection = new RiakManager(riak);
        env.manage(riakConnection);
        env.manage(new CacheManager(riakConnection.getClient()));
        env.addHealthCheck(new RiakHealthCheck(riakConnection.getClient()));
        env.addResource(new DatasetResource(riakConnection.getClient()));
        env.addResource(new QueryResource(riakConnection.getClient()));
        env.addResource(new CacheResource(riakConnection.getClient()));
        env.addResource(new MetadataResource(riakConnection.getClient()));
    }
}
