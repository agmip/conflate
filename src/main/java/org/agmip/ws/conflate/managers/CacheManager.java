package org.agmip.ws.conflate.managers;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.bucket.Bucket;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.yammer.dropwizard.lifecycle.Managed;
import org.agmip.ace.util.JsonFactoryImpl;
import org.agmip.ws.conflate.core.caches.CropCache;

public class CacheManager implements Managed {
    IRiakClient client;

    public CacheManager(IRiakClient client) {
       this.client = client;
    }

    @Override
    public void start() throws Exception {
        Bucket b = this.client.fetchBucket("ace_cache_crop").execute();
        IRiakObject obj = b.fetch("main").execute();
        if (obj != null) {
            JsonParser p = JsonFactoryImpl.INSTANCE.getParser(obj.getValue());
            JsonToken t = p.nextToken();
            while (t != null) {
                if (t == JsonToken.FIELD_NAME) {
                    CropCache.INSTANCE.updateCrops(p.getCurrentName());
                }
                t = p.nextToken();
            }
            p.close();
        }
    }

    @Override
    public void stop() throws Exception {
    }
}
