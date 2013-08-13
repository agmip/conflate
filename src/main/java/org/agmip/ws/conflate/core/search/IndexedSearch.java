package org.agmip.ws.conflate.core.search;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.query.indexes.BinIndex;

public class IndexedSearch extends RecursiveTask<List<String>> {
    private static final long serialVersionUID = 1L;
    private IRiakClient client;
    private String key;
    private String value;

    public IndexedSearch(IRiakClient client, String key, String value) {
        this.client = client;
        this.key = key;
        this.value = value;
    }

    @Override
    protected List<String> compute() {
        try {
            Bucket bMD = this.client.fetchBucket("ace_metadata").execute();
            List<String> riakKeys = bMD.fetchIndex(BinIndex.named(key)).withValue(value).execute();
            return riakKeys;
        } catch (Exception ex) {
            return new ArrayList<String>();
        }
    }

}
