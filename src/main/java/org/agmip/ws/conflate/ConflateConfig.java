package org.agmip.ws.conflate;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.yammer.dropwizard.client.JerseyClientConfiguration;
import org.agmip.ws.conflate.config.ExternalServicesConfig;
import org.agmip.ws.conflate.config.RiakConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.dropwizard.config.Configuration;

public class ConflateConfig extends Configuration {
    @Valid
    @NotNull
    @JsonProperty
    private RiakConfig riak = new RiakConfig();

    @Valid
    @NotNull
    @JsonProperty
    private JerseyClientConfiguration httpClient = new JerseyClientConfiguration();

    @Valid
    @JsonProperty
    private ExternalServicesConfig services = new ExternalServicesConfig();

    public RiakConfig getRiakConfig() {
        return this.riak;
    }

    public JerseyClientConfiguration getHttpClientConfig() {
        return this.httpClient;
    }

    public ExternalServicesConfig getServices() {
        return this.services;
    }
}
