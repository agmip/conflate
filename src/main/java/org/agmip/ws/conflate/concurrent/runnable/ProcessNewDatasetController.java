package org.agmip.ws.conflate.concurrent.runnable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.agmip.ace.AceDataset;
import org.agmip.ace.AceExperiment;
import org.agmip.ace.AceSoil;
import org.agmip.ace.AceWeather;
import org.agmip.ace.util.AceFunctions;
import org.agmip.ace.util.GeoPoint;
import org.agmip.ace.util.JsonFactoryImpl;
import org.agmip.ace.util.MetadataFilter;
import org.agmip.ws.conflate.core.caches.CropCache;
import org.agmip.ws.conflate.core.caches.LocationCacheEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ProcessNewDatasetController implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(ProcessNewDatasetController.class);
	private static final ObjectMapper mapper = new ObjectMapper();
	String            datasetId;
	AceDataset        dataset;
	ExecutorService   ex;
	IRiakClient       riak;
	
	public ProcessNewDatasetController(String datasetId, AceDataset dataset, ExecutorService ex, IRiakClient riak) {
		this.datasetId = datasetId;
		this.dataset   = dataset;
		this.ex        = ex;
		this.riak      = riak;
	}
		
	@Override
	public void run() {
		// Run all required blocking code first.
		try {
			calculateDatasetFields();
			// After this succeeds, execute all other threaded methods
			ex.execute(new StoreMetadataProcess());
			ex.execute(new StoreDataProcess());
			ex.execute(new UpdateCachesProcess());
		} catch(Exception ex) {
			//Submit this error back to the user tracker.
			LOG.error("Unable to process this dataset: {}", ex.getMessage());
		}
	}
	
	private void calculateDatasetFields() throws IOException {
		for(AceExperiment e: this.dataset.getExperiments()) {
			GeoPoint point = new GeoPoint(e.getValue("fl_lat"), e.getValue("fl_long"));
			e.update("~fl_geohash~", point.getGeoHash(), true);
		}
	}
	
	private class StoreMetadataProcess implements Runnable {
		@Override
		public void run() {
			try {
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				Bucket metadataBucket = riak.fetchBucket("ace_metadata").execute();
				for(AceExperiment e : dataset.getExperiments()) {
					RiakObjectBuilder builder = RiakObjectBuilder.newBuilder(metadataBucket.getName(), e.getId());
					JsonGenerator g = JsonFactoryImpl.INSTANCE.getGenerator(out);
					g.writeStartObject();
					for(String key : MetadataFilter.INSTANCE.getMetadata()) {
						LOG.info("Looking for key: {}", key);
						String value = AceFunctions.deepGetValue(e, key);
						LOG.info("Found value: {}", value);
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
			}	catch(Exception ex) {
				LOG.error("Error processing metadata: {}", ex.getMessage());
			}
		}
	}
	private class StoreDataProcess implements Runnable {
		@Override
		public void run() {
			try {
				Bucket bE = riak.fetchBucket("ace_experiments").execute();
		        Bucket bW = riak.fetchBucket("ace_weathers").execute();
		        Bucket bS = riak.fetchBucket("ace_soils").execute();
		        for(AceWeather w : dataset.getWeathers()) {
		            if(w.getValue("wst_distrib") == null) {
		                bW.store(w.getId(), w.rebuildComponent()).execute();
		            }
		        }
		        for (AceSoil s : dataset.getSoils()) {
		            bS.store(s.getId(), s.rebuildComponent()).execute();
		        }
		        dataset.linkDataset();
		        for (AceExperiment e : dataset.getExperiments()) {
		            if(e.getValue("ex_distrib") == null) {
		                RiakObjectBuilder builder = RiakObjectBuilder.newBuilder(bE.getName(), e.getId());
		                builder.addLink(bW.getName(), e.getWeather().getId(), "linked");
		                builder.addLink(bS.getName(), e.getSoil().getId(), "linked");
		                builder.withValue(e.rebuildComponent());
		                bE.store(builder.build()).execute();
		            }
		        }
			} catch (Exception ex) {
				LOG.error("Unable to store data: {}", ex.getMessage());
			}
		}
	}
	private class UpdateCachesProcess implements Runnable {
		@Override
		public void run() {
			long ts = System.currentTimeMillis();
			try {
				List<String> crops = new ArrayList<>();
				Map<String, LocationCacheEntry> points = new HashMap<>();

				for(AceExperiment e : dataset.getExperiments()) {
					String crid = AceFunctions.deepGetValue(e, "crid");
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
				if(updated){
					LOG.info("I can update my crop listing...");
					Bucket bCC = riak.fetchBucket("ace_cache_crop").execute();
					String currentCache = CropCache.INSTANCE.serialize();
			        IRiakObject liveCache = bCC.fetch("main").execute();
			        if (liveCache != null) {
			            if (! currentCache.equals(liveCache.getValueAsString())) {
			                liveCache.setValue(currentCache);
			                bCC.store(liveCache).execute();
			            }
			        } else {
			            bCC.store("main", currentCache).execute();
			        }
				}

				Bucket bLC = riak.fetchBucket("ace_cache_location").execute();
				for(LocationCacheEntry entry : points.values()) {
					RiakObjectBuilder builder = RiakObjectBuilder.newBuilder(bLC.getName(), entry.getPoint().getGeoHash()+"_"+ ts);
					builder.addIndex("timestamp", ts);
					builder.withValue(mapper.writeValueAsBytes(entry));
					bLC.store(builder.build()).execute();
				}
			} catch (Exception ex) {
				LOG.error("Unable to update location cache: {}", ex.getMessage());
			}
		}
	}
}

