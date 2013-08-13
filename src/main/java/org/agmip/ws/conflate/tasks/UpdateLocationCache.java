package org.agmip.ws.conflate.tasks;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.agmip.ace.util.JsonFactoryImpl;
import org.agmip.ws.conflate.core.caches.LocationCacheEntry;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.functions.NamedJSFunction;
import com.basho.riak.client.query.indexes.IntIndex;
import com.basho.riak.client.raw.query.indexes.IndexQuery;
import com.basho.riak.client.raw.query.indexes.IntRangeQuery;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableMultimap;
import com.yammer.dropwizard.tasks.Task;

public class UpdateLocationCache extends Task {
    IRiakClient client;

    protected UpdateLocationCache(String name, IRiakClient client) {
        super(name);
        this.client = client;
    }

    @Override
    public void execute(ImmutableMultimap<String, String> params,
            PrintWriter writer) throws Exception {

        String startTimestamp = params.get("start").asList().get(0);
        Long start = 0L;
        if (startTimestamp != null) {
            try {
                start = Long.parseLong(startTimestamp);
            } catch (NumberFormatException ex) {
                // LOG an error
                return;
            }
        }

        Long end = System.currentTimeMillis() - 5000;
        Bucket locationCache = this.client.fetchBucket("ace_cache_location")
                .execute();

        // First attempt to get the base cache
        IRiakObject base = locationCache.fetch("main").execute();

        // Perform a MR on everything in the range (2i)
        IndexQuery iq = new IntRangeQuery(IntIndex.named("timestamp"),
                locationCache.getName(), start, end);
        MapReduceResult results = this.client.mapReduce(iq)
                .addMapPhase(new NamedJSFunction("Riak.mapValuesJson"), true)
                .execute();

        Map<String, Integer> locationCount = new HashMap<>();
        Collection<LocationCacheEntry> newCacheEntries = results
                .getResult(LocationCacheEntry.class);
        for (LocationCacheEntry entry : newCacheEntries) {
            String geohash = entry.getPoint().getGeoHash();
            if (locationCount.containsKey(geohash)) {
                int currentCount = locationCount.get(geohash);
                locationCount.put(geohash, (currentCount + entry.getCount()));
            } else {
                locationCount.put(geohash, entry.getCount());
            }
        }

        JsonParser p = JsonFactoryImpl.INSTANCE.getParser(base.getValue());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonGenerator g = JsonFactoryImpl.INSTANCE.getGenerator(out);
        JsonToken t = p.nextToken();

        while (t != null) {
            if (t == JsonToken.FIELD_NAME) {
                String geohash = p.getCurrentName();
                // Before we advance, we need to write the current field
                g.copyCurrentEvent(p);
                t = p.nextToken();
                if (t == JsonToken.VALUE_NUMBER_INT) {
                    if (locationCount.containsKey(geohash)) {
                        Integer currentCount = p.getValueAsInt(0);
                        currentCount = currentCount + locationCount.get(geohash);
                        g.writeNumber(currentCount);
                    } else {
                        g.copyCurrentEvent(p);
                    }
                } else {
                    g.copyCurrentEvent(p);
                }
            } else {
                g.copyCurrentEvent(p);
            }
            p.nextToken();
        }
        g.flush();
        g.close();
        p.close();
        base.setValue(out.toByteArray());
        locationCache.store(base).execute();
    }
}
