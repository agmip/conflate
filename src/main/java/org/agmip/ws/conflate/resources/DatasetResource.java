package org.agmip.ws.conflate.resources;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.agmip.ace.AceDataset;
import org.agmip.ace.AceExperiment;
import org.agmip.ace.AceRecord;
import org.agmip.ace.AceSoil;
import org.agmip.ace.AceWeather;
import org.agmip.ace.io.AceParser;
import org.agmip.ace.util.AceFunctions;
import org.agmip.ace.util.JsonFactoryImpl;
import org.agmip.ace.util.GeoPoint;
import org.agmip.ace.util.MetadataFilter;
import org.agmip.ws.conflate.core.caches.CropCache;
import org.agmip.ws.conflate.core.caches.LocationCacheEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.multipart.FormDataParam;

@Path("/ace/1/dataset")
public class DatasetResource {
    private static final Logger LOG = LoggerFactory.getLogger(DatasetResource.class);
    private IRiakClient client;
    private static final ObjectMapper mapper = new ObjectMapper();

    public DatasetResource(IRiakClient riak)  {
        this.client = riak;
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public String uploadFile(
        @FormDataParam("file") final InputStream stream) throws Exception {

        try {
            String randomId = UUID.randomUUID().toString();
            AceDataset dataset = AceParser.parseACEB(stream);
            long timestamp = System.currentTimeMillis();
            // Fire off the threaded controller (does not need to report back to this one)
            //addCalculatedFields(dataset);
            //storeMetadata(dataset);
            //storeData(dataset);
            //updateCaches(dataset, timestamp);
            // Fire off store event to Riak
            // key: uuid, value: {uud, path, timestamp, client-server-id}
            // UploadRef upload = new UploadRef(randomId, tmpFile.toString(), timestamp);
            // upload = null;
            //return the UUID to the client, for caching
            return "{\"reqid\":\""+randomId+"\"}";

        } catch (IOException ex) {
            LOG.error("500 Error: {}", ex.getMessage());
            return "{}";
        }

    }

    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public String deleteFromFile(
        @FormDataParam("file") final InputStream stream) throws Exception {

        try {
            AceDataset dataset = AceParser.parseACEB(stream);
            removeMetadata(dataset);
            return "{\"removed\":\"true\"}";

        } catch (IOException ex) {
            LOG.error("500 Error: {}", ex.getMessage());
            return "{}";
        }

    }

    //TODO: Add data to experiment here!
    private void addCalculatedFields(AceDataset dataset) throws Exception {
        Double tavp = null;
        Double precp = null;
        for(AceExperiment e: dataset.getExperiments()) {
            LOG.info("Calculating geohash for {}", e.getValue("exname"));
            GeoPoint point = new GeoPoint(e.getValue("fl_lat"), e.getValue("fl_long"));
            e.update("~fl_geohash~", point.getGeoHash(), true);
        }
    }

    private void storeMetadata(AceDataset dataset) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Bucket metadataBucket = this.client.fetchBucket("ace_metadata").execute();
        for(AceExperiment e : dataset.getExperiments()) {
            RiakObjectBuilder builder = RiakObjectBuilder.newBuilder(metadataBucket.getName(), e.getId());
            JsonGenerator g = JsonFactoryImpl.INSTANCE.getGenerator(out);
            g.writeStartObject();
            for(String key : MetadataFilter.INSTANCE.getMetadata()) {
                String value = AceFunctions.deepGetValue(e, key);
                if (value != null) {
                    g.writeStringField(key, value);
                    if (MetadataFilter.INSTANCE.getIndexedMetadata().contains(key)) {
                        builder.addIndex(key, value);
                    }
                }
                builder.addLink("ace_experiments", e.getId(), "linked");
            }
            String geohash = e.getValue("~fl_geohash~");
            if (geohash != null) {
                g.writeStringField("~fl_geohash~", geohash);
                builder.addIndex("fl_geohash", geohash);
            }
            g.writeEndObject();
            g.close();
            builder.withValue(out.toByteArray());
            metadataBucket.store(builder.build()).execute();
            out.reset();
        }
    }

    private void storeData(AceDataset dataset) throws Exception {
        Bucket bE = this.client.fetchBucket("ace_experiments").execute();
        Bucket bW = this.client.fetchBucket("ace_weathers").execute();
        Bucket bS = this.client.fetchBucket("ace_soils").execute();
        for(AceWeather w : dataset.getWeathers()) {
            if(w.getValue("wst_distrib") == null) {
                bW.store(w.getId(), w.getRawComponent()).execute();
            }
        }
        for (AceSoil s : dataset.getSoils()) {
            bS.store(s.getId(), s.getRawComponent()).execute();
        }
        for (AceExperiment e : dataset.getExperiments()) {
            if(e.getValue("ex_distrib") == null) {
                RiakObjectBuilder builder = RiakObjectBuilder.newBuilder(bE.getName(), e.getId());
                builder.addLink(bW.getName(), e.getWeather().getId(), "linked");
                builder.addLink(bS.getName(), e.getSoil().getId(), "linked");
                builder.withValue(e.getRawComponent());
                bE.store(builder.build()).execute();
            }
        }
    }

    private void removeMetadata(AceDataset dataset) throws Exception {
        Bucket metadataBucket = this.client.fetchBucket("metadata").execute();
        Bucket bM = this.client.fetchBucket("ace_metadata").execute();
        Bucket bE = this.client.fetchBucket("ace_experiments").execute();
        Bucket bW = this.client.fetchBucket("ace_weathers").execute();
        Bucket bS = this.client.fetchBucket("ace_soils").execute();
        for (AceExperiment e : dataset.getExperiments()) {
            bW.delete(e.getWeather().getId()).execute();
            bS.delete(e.getSoil().getId()).execute();
            bE.delete(e.getId()).execute();
            bM.delete(e.getId()).execute();
            metadataBucket.delete(e.getId()).execute();
        }
    }

    private void updateCaches(AceDataset dataset, long ts) throws Exception {
        List<String> crops = new ArrayList<>();
        Map<String, LocationCacheEntry> points = new HashMap<>();

        for(AceExperiment e : dataset.getExperiments()) {
            String crid = e.getValue("crid");
            if (crid != null && ! crops.contains(crid)) {
                crops.add(crid);
            }

            //String geohash = e.getValue("~fl_geohash~");
            GeoPoint point = new GeoPoint(e.getValue("fl_lat"), e.getValue("fl_long"));
            if(point.getGeoHash() != null) {
                if(! points.containsKey(point.getGeoHash())) {
                    points.put(point.getGeoHash(), new LocationCacheEntry(point));
                } else {
                    LocationCacheEntry ce = points.get(point.getGeoHash());
                    ce.incrementCount();
                }
            } else {
                LOG.info("dataset is returning a null geohash");
            }
      }
        boolean updated = false;
        for (String crid : crops) {
            if (CropCache.INSTANCE.updateCrops(crid)) {
                updated = true;
            }
        }
        if(updated){}
        // TODO: if updated write to database

        Bucket bLC = this.client.fetchBucket("ace_cache_location").execute();
        for(LocationCacheEntry entry : points.values()) {
            RiakObjectBuilder builder = RiakObjectBuilder.newBuilder(bLC.getName(), entry.getPoint().getGeoHash()+"_"+ ts);
            builder.addIndex("timestamp", ts);
            builder.withValue(mapper.writeValueAsBytes(entry));
            bLC.store(builder.build()).execute();
        }
    }
}
