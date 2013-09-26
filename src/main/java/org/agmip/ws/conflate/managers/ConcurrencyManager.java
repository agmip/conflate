package org.agmip.ws.conflate.managers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.yammer.dropwizard.lifecycle.ExecutorServiceManager;
import com.yammer.dropwizard.lifecycle.Managed;

public class ConcurrencyManager implements Managed {
	private ExecutorServiceManager datasetPoolManager;
	private ExecutorService datasetPool;
	public ConcurrencyManager() {
		this.datasetPool = Executors.newFixedThreadPool(15);
		this.datasetPoolManager = new ExecutorServiceManager(this.datasetPool, 50L, TimeUnit.SECONDS, "datasetPool");
	}
	
	@Override
	public void start() throws Exception {
		this.datasetPoolManager.start();
	}

	@Override
	public void stop() throws Exception {
		this.datasetPoolManager.stop();
	}
	
	public ExecutorService getDatasetExecutor() {
		return this.datasetPool;
	}
	

}
