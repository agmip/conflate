package org.agmip.ws.conflate;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.agmip.ws.conflate.config.RiakConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.dropwizard.config.Configuration;

public class ConflateConfig extends Configuration {
    @Valid
    @NotNull
    @JsonProperty
    private RiakConfig riak = new RiakConfig();

    public RiakConfig getRiakConfig() {
        return this.riak;
    }
}
