package org.agmip.ws.conflate.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.functions.NamedJSFunction;
import com.basho.riak.client.query.indexes.BinIndex;
import com.basho.riak.client.raw.query.indexes.BinValueQuery;
import com.basho.riak.client.raw.query.indexes.IndexQuery;
import com.yammer.metrics.annotation.Timed;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

@Path("/metadata/")
public class MetadataResource {
    //private static final Logger LOG = LoggerFactory.getLogger(MetadataResource.class);
    private IRiakClient client;
    
    public MetadataResource(IRiakClient client) {
        this.client = client;
    }

    @GET
    @Path("geohash")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public String getMetadataByGeohash(@QueryParam("hash") String geohash) {
        if (geohash == null) {
            return "[]";
        } else {
            // TODO: Implement caching (etags for clients, cache for server)
            try {
                Bucket bM = this.client.fetchBucket("ace_metadata").execute();
                IndexQuery iq = new BinValueQuery(BinIndex.named("fl_geohash"), bM.getName(), geohash);
                /** 
                 * ETAG IMPL
                 *
              try {

                List<String> indexResults = bM.fetchIndex(BinIndex.named("fl_geohash")).execute(); 
                 */ 
                MapReduceResult results = client.mapReduce(iq).addMapPhase(new NamedJSFunction("Riak.mapValuesJson")).execute();
                return results.getResultRaw();

            } catch(RiakException ex) {
                //TODO: return a proper error here
                return "{}";
            }
        }
    }
}
