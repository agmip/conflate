package org.agmip.ws.conflate.resources;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.agmip.ace.util.GeoPoint;
import org.agmip.ace.util.MetadataFilter;
import org.agmip.ws.conflate.core.ConflateFunctions;
import org.agmip.ws.conflate.core.search.SearchBootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.query.BucketKeyMapReduce;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.functions.NamedJSFunction;
import com.basho.riak.client.query.indexes.BinIndex;
import com.basho.riak.client.raw.query.indexes.BinValueQuery;
import com.basho.riak.client.raw.query.indexes.IndexQuery;
import com.yammer.metrics.annotation.Timed;

@Path("ace/1/query")
public class QueryResource {
    private final static Logger LOG = LoggerFactory.getLogger(QueryResource.class);
    private final IRiakClient riak;
    private static final String metabucketName = "ace_metadata";
    private final ForkJoinPool queryPool = new ForkJoinPool();

    public QueryResource(IRiakClient client) {
        this.riak = client;
    }


    /**
* Primary searching function
*
* @todo Allow for ranged searches (ex. lat, lon)
* @todo Implement MR filtering on all other fields
*/
    @GET
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public String searchMe(@Context UriInfo uriInfo) {
        MultivaluedMap<String, String> searchParams = uriInfo.getQueryParameters();
        String baseSearchParam,
            baseSearchValue = null;
        if (searchParams.containsKey("fl_lat") || searchParams.containsKey("fl_long")) {
            GeoPoint searchPoint = new GeoPoint(searchParams.getFirst("fl_lat"), searchParams.getFirst("fl_long"));
            baseSearchValue = searchPoint.getGeoHash();
            if (baseSearchValue != null) {
                baseSearchParam = "~fl_geohash~";
            } else {
                baseSearchParam = findBestParam(searchParams.keySet());
            }
        } else {
            baseSearchParam = findBestParam(searchParams.keySet());
        }
        MapReduceResult results;
        if(baseSearchParam.equals("")) {
            throw new WebApplicationException(Response
                .status(400)
                .entity("Searching without indexes not implemented")
                .build());
        }
        try {
            if (baseSearchValue == null) {
                baseSearchValue = searchParams.getFirst(baseSearchParam);
            }
            IndexQuery iq = new BinValueQuery(BinIndex.named(baseSearchParam), metabucketName, baseSearchValue);
            results = riak.mapReduce(iq)
                          .addMapPhase(new NamedJSFunction("Riak.mapValuesJson"), true)
                          .execute();
        } catch (RiakException e) {
            throw new WebApplicationException(Response
                .status(400)
                .entity(e.getMessage())
                .build());
        }
        if (searchParams.containsKey("callback")) {
           return ConflateFunctions.JSONPWrap(searchParams.getFirst("callback"), results.getResultRaw());
        } else {
            return results.getResultRaw();
        }
    }

    @GET
    @Path("beta")
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    public String fjSearch(@Context UriInfo uriInfo) {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        List<String> keyFilter = queryPool.invoke(new SearchBootstrap(this.riak, params));
        String results;
        try {
            if (keyFilter.size() == 0) {
                results = "[]";
            } else if (keyFilter.size() == 1) {
                results = "["+this.riak.fetchBucket("ace_metadata").execute().fetch(keyFilter.get(0)).execute().getValueAsString()+"]";
            } else {
                BucketKeyMapReduce mr = this.riak.mapReduce();
                for(String key: keyFilter) {
                    mr.addInput("ace_metadata", key);
                }
                MapReduceResult mrResults = mr.addMapPhase(new NamedJSFunction("Riak.mapValuesJson"), true)
                  .execute();
                results = mrResults.getResultRaw();
            }
        } catch (RiakException e) {
            results = "[]";
        }
        if (params.containsKey("callback")) {
            return ConflateFunctions.JSONPWrap(params.getFirst("callback"), results);
        } else {
            return results;
        }
    }

    /**
     * Find the best parameter (lowest weight) to search on for 2i.
     * If there are more than one with the same lowest weight, just
     * pick an arbitary one.
     *
     * @todo Add some protection (params ! null)
     * @param params A set of parameters to weigh.
     * @return the parameter with the lowest weight.
     */
    private String findBestParam(Set<String> params) {
        int winWeight = 11; // Our amps don't go to 11 :(
        String winParam = "";
        Map<String, Integer> weights = MetadataFilter.INSTANCE.getWeights();
        Set<String> fields = MetadataFilter.INSTANCE.getIndexedMetadata();
        Iterator<String> i = params.iterator();
        // This SHOULD get anything that is indexed in the search,
        // to prevent from MapReducing the whole bucket.
        while(i.hasNext()) {
            String var = i.next().toString();
            if(fields.contains(var)) {
                Integer weight = weights.get(var);
                if (weight == null) {
                    weight = 10;
                }
                if( weight < winWeight ) {
                    winWeight = weight;
                    winParam = var;
                }
            }
        }
        LOG.debug("Winning weight: "+winWeight);
        LOG.debug("Winning param: "+winParam);
        return winParam;
    }
}
