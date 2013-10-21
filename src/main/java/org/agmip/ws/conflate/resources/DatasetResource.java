package org.agmip.ws.conflate.resources;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.sun.jersey.api.client.Client;
import org.agmip.ace.AceDataset;
import org.agmip.ace.AceExperiment;
import org.agmip.ace.io.AceParser;
import org.agmip.ws.conflate.concurrent.runnable.ProcessNewDatasetController;
import org.agmip.ws.conflate.managers.ConcurrencyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.bucket.Bucket;
import com.sun.jersey.multipart.FormDataParam;

@Path("/ace/1/dataset")
public class DatasetResource {
	private static final Logger LOG = LoggerFactory.getLogger(DatasetResource.class);
	private IRiakClient client;
	//private static final ObjectMapper mapper = new ObjectMapper();
	private ExecutorService datasetService;
    private Client httpClient;

	public DatasetResource(IRiakClient riak, ConcurrencyManager executors, Client httpClient)  {
		this.client = riak;
		this.datasetService = executors.getDatasetExecutor();
        this.httpClient = httpClient;
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response uploadFile(
			@FormDataParam("file") final InputStream stream) throws Exception {

		try {
			String randomId = UUID.randomUUID().toString();
			AceDataset dataset = AceParser.parseACEB(stream);
			this.datasetService.execute(new ProcessNewDatasetController(randomId, dataset, this.datasetService, this.client, this.httpClient));

			return Response.ok("{\"reqid\":\""+randomId+"\"").build();

		} catch (IOException ex) {
			LOG.error("500 Error: {}", ex.getMessage());
			return Response.ok("{}").build();
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
	
	@POST
	@Path("/nuke")
	public String destroyDatabase() throws Exception {
		wipeDatabase();
		return "Eliminated...";
	}
	
	// Implementation methods
	
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
	
	private void wipeDatabase() throws Exception {
		Set<String> buckets = this.client.listBuckets();
		for(String bucket : buckets) {
			LOG.info("Starting to delete bucket: {}", bucket);
			Bucket b = this.client.fetchBucket(bucket).execute();
			for(String key : b.keys()) {
				LOG.info("Deleting key: {}", key);
				b.delete(key).execute();
			}
			LOG.info("Deleted bucket: {}", bucket);
		}
	}
}
