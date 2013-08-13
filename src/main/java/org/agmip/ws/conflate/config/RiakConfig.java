package org.agmip.ws.conflate.config;

import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RiakConfig {
    private static final Logger LOG = LoggerFactory.getLogger(RiakConfig.class);
    
    @JsonProperty
    @NotEmpty
    private String host;
    
    @JsonProperty
    private int port = 8087;
    
    @JsonProperty
    private String client = "http";
    
    public String getHost() {
        return this.host;
    }
    
    public int getPort() {
        return this.port;
    }
    
    public String getClientType() {
        if ((this.client.equals("pb")) || this.client.equals("http")) {
            return this.client;
        } else {
            LOG.error("Invalid clientType in configuration: {}. clientType is now http", this.client);
            return "http";
        }
    }
}
