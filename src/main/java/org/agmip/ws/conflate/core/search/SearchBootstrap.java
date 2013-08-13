package org.agmip.ws.conflate.core.search;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;

import javax.ws.rs.core.MultivaluedMap;

import org.agmip.ace.util.MetadataFilter;

import com.basho.riak.client.IRiakClient;

public class SearchBootstrap extends RecursiveTask<List<String>> {

    private static final long serialVersionUID = 1L;
    private final MultivaluedMap<String, String> query;
    private IRiakClient client;

    public SearchBootstrap(IRiakClient client, MultivaluedMap<String, String> searchQuery) {
        this.query = searchQuery;
        this.client = client;
    }

    @Override
    protected List<String> compute() {
        List<String> finalGroup = new ArrayList<>();
        List<RecursiveTask<List<String>>> tasks = new ArrayList<>();
        for(String key : query.keySet()) {
            key = key.toLowerCase();
            if(MetadataFilter.INSTANCE.getIndexedMetadata().contains(key)) {
                IndexedSearch task = new IndexedSearch(this.client, key, this.query.getFirst(key));
                tasks.add(task);
                task.fork();
            }
            if (key.endsWith("_year")) {
                String year = this.query.getFirst(key);
                key = key.substring(0, key.length()-5);
                RangeSearch task = new RangeSearch(this.client, key, year+"0101", year+"1231");
                tasks.add(task);
                task.fork();
            }
        }
        //for(RecursiveTask<List<String>> task : tasks) {
        for(int i=0; i < tasks.size(); i++) {
            List<String> newList = tasks.get(i).join();
            if (i==0 && newList.size() != 0) {
            }
            if (newList.size() != 0) {
                if(i==0) {
                    finalGroup.addAll(newList);
                } else {
                    finalGroup.retainAll(newList);
                }
            } else {
                finalGroup.clear();
            }
        }
        return finalGroup;
    }
}
