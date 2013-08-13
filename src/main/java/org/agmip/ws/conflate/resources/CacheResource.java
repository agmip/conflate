package org.agmip.ws.conflate.resources;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.bucket.Bucket;
import org.agmip.ws.conflate.core.caches.CropCache;

@Path("/ace/1/cache/")
public class CacheResource {
    IRiakClient client;

    public CacheResource(IRiakClient client) {
        this.client = client;
    }

    @GET
    @Path("crop")
    @Produces(MediaType.APPLICATION_JSON)
    public String getCropCache(@QueryParam("callback") String jsonp) {
        try {
            if (jsonp != null) {
                StringBuilder sb = new StringBuilder();
                sb.append(jsonp);
                sb.append("(");
                sb.append(CropCache.INSTANCE.serialize());
                sb.append(");");
                return sb.toString();
            } else {
                return CropCache.INSTANCE.serialize();
            }
        } catch (IOException ex) {
            return ("{}");
        }
    }

    @GET
    @Path("writecrop")
    @Produces(MediaType.TEXT_PLAIN)
    public String updateCropCache() throws RiakException, IOException {
        Bucket cropCache = this.client.fetchBucket("ace_cache_crop").execute();
        String currentCache = CropCache.INSTANCE.serialize();
        IRiakObject liveCache = cropCache.fetch("main").execute();
        if (liveCache != null) {
            if (! currentCache.equals(liveCache.getValueAsString())) {
                liveCache.setValue(currentCache);
                cropCache.store(liveCache).execute();
            }
        } else {
            cropCache.store("main", currentCache).execute();
        }
        return "ok.";
    }
    
    @GET
    @Path("location")
    @Produces(MediaType.APPLICATION_JSON)
    public String getLocationCache(@QueryParam("callback") String jsonp) {
        return "{}";
    }

    @GET
    @Path("manual/{crid}")
    @Produces(MediaType.TEXT_PLAIN)
    public String updateManually(@PathParam("crid") String crid) throws RiakException, IOException {
        CropCache.INSTANCE.updateCrops(crid);
        return updateCropCache();
    }
}
