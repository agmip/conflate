package org.agmip.ws.conflate.tasks;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.agmip.ace.util.JsonFactoryImpl;
import org.agmip.ws.conflate.core.caches.LocationCacheEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private static final Logger LOG = LoggerFactory.getLogger(UpdateLocationCache.class);
	private IRiakClient client;

	public UpdateLocationCache(String name, IRiakClient client) {
		super(name);
		this.client = client;
	}

    /*
     * Main cache format: [{geohash: <hash>, lat: <lat>, lng: <lng>, count: <count>}...]
     */

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

		Map<String, LocationCacheEntry> locationCount = new HashMap<>();
		Collection<LocationCacheEntry> newCacheEntries = results
				.getResult(LocationCacheEntry.class);
		for (LocationCacheEntry entry : newCacheEntries) {
			String geohash = entry.getPoint().getGeoHash();
            if (locationCount.containsKey(geohash)) {
                LocationCacheEntry mainEntry = locationCount.get(geohash);
                mainEntry.setCount(mainEntry.getCount()+entry.getCount());
			} else {
				locationCount.put(geohash, entry);
			}
		}

		if (locationCount.size() != 0) {
			JsonParser p;
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			JsonGenerator g = JsonFactoryImpl.INSTANCE.getGenerator(out);
			if (base == null) {
                // This is the first running of this on this cluster... setup.
                g.writeStartArray();
				for(LocationCacheEntry newLoc : locationCount.values()) {
                    writeLocationEntry(g, newLoc);
                }
				g.flush();
				g.close();
				locationCache.store("main", out.toByteArray()).execute();
			} else {
                p = JsonFactoryImpl.INSTANCE.getParser(base.getValue());
				JsonToken t = p.nextToken();
                String geohash = "";
                int count = 0;
                boolean inEntry = false;
				while (t != null) {
                    if(t == JsonToken.START_ARRAY) {
                        g.writeStartArray();
                    }
                    if(t == JsonToken.START_OBJECT) {
                        inEntry = true;
                    }
                    if(t == JsonToken.END_OBJECT) {
                        inEntry = false;
                        // Now write this entry into the database.
                        if(locationCount.containsKey(geohash)) {
                            writeLocationEntry(g, locationCount.get(geohash), count);
                            locationCount.remove(geohash);
                        } else {
                            writeLocationEntry(g, new LocationCacheEntry(geohash, count));
                        }
                    }
                    if (t == JsonToken.END_ARRAY) {
                        for(LocationCacheEntry newLoc: locationCount.values()) {
                            writeLocationEntry(g, newLoc);
                        }
                        g.writeEndArray();
                    }
                    if (inEntry) {
                        if(t == JsonToken.FIELD_NAME) {
                            String currentName = p.getCurrentName();
                            if (currentName != null)
                                if (currentName.equals("geohash")) {
                                    geohash = p.nextTextValue();
                                } else if (currentName.equals("count")) {
                                    count = p.nextIntValue(0);
                                }
                        }
                    }
					t = p.nextToken();
				}
				g.flush();
				g.close();
				p.close();
                LOG.info("Writing out... {}", out.toString("UTF-8"));
				base.setValue(out.toByteArray());
				locationCache.store(base).execute();
			}
			writer.print(end);
			writer.flush();
			writer.close();
		}
	}

    private static void writeLocationEntry(JsonGenerator g, LocationCacheEntry entry) throws Exception {
        g.writeStartObject();
        g.writeObjectField("geohash", entry.getPoint().getGeoHash());
        g.writeObjectField("lat", entry.getPoint().getLat());
        g.writeObjectField("lng", entry.getPoint().getLng());
        g.writeObjectField("count", entry.getCount());
        g.writeEndObject();
    }

    private static void writeLocationEntry(JsonGenerator g, LocationCacheEntry entry, int originalCount) throws Exception {
        entry.setCount(originalCount+entry.getCount());
        writeLocationEntry(g, entry);
    }
}
