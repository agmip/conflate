package org.agmip.ws.conflate.core.search;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.query.indexes.BinIndex;

public class RangeSearch extends RecursiveTask<List<String>> {
    private static final long serialVersionUID = 1L;
    private IRiakClient client;
    private String key;
    private String to;
    private String from;

    public RangeSearch(IRiakClient client, String key, String to, String from) {
        this.client = client;
        this.key = key;
        this.to = to;
        this.from = from;
    }

    @Override
    protected List<String> compute() {
        try {
            Bucket bMD = this.client.fetchBucket("ace_metadata").execute();
            return bMD.fetchIndex(BinIndex.named(this.key)).to(this.to).from(this.from).execute();
        } catch(RiakException ex) {
            return new ArrayList<String>();
        }
    }
}
