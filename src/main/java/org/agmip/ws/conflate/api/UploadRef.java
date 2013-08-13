package org.agmip.ws.conflate.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UploadRef {
    @JsonProperty
    private String id;

    @JsonProperty
    private long timestamp;

    @JsonProperty
    private String fileLocation;

    public UploadRef(String id, String fileLocation, long timestamp) {
        this.id = id;
        this.fileLocation = fileLocation;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public String getFileLocation() {
        return fileLocation;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
