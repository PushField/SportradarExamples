/*
 * Copyright (C) Sportradar AG. See LICENSE for full license governing this code
 */

package com.sportradar.unifiedodds.example.examples;

import com.pushtechnology.gateway.framework.GatewayApplication;
import com.pushtechnology.gateway.framework.Publisher;
import com.pushtechnology.gateway.framework.ServiceDefinition;
import com.pushtechnology.gateway.framework.ServiceMode;
import com.pushtechnology.gateway.framework.StateHandler;
import com.pushtechnology.gateway.framework.StreamingSourceHandler;
import com.pushtechnology.gateway.framework.exceptions.InvalidConfigurationException;
import com.sportradar.unifiedodds.sdk.exceptions.InitException;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.pushtechnology.gateway.framework.DiffusionGatewayFramework.newApplicationDetailsBuilder;

/**
 * A basic example demonstrating on how to start the SDK with a single session
 */
public class DiffusionSessionSetup implements GatewayApplication {


    private static final String STREAMING_ODDS_SOURCE = "STREAMING_ODDS_SOURCE";
    private static final String APPLICATION_TYPE = "SPORTSRADAR_ODDS_SOURCE";
    private final SourceConfigValidator sourceConfigValidator;
    private String token;

    public DiffusionSessionSetup(
            SourceConfigValidator sourceConfigValidator) {
        this.sourceConfigValidator = sourceConfigValidator;
    }

    public void setToken(String tokenToTry) {
        this.token = tokenToTry;
        System.out.println("\tsetToken()");
    }

    @Override
    public ApplicationDetails getApplicationDetails() {
        System.out.println("\tgetApplicationDetails()...");
        return newApplicationDetailsBuilder()
                .addServiceType(STREAMING_ODDS_SOURCE,
                        ServiceMode.STREAMING_SOURCE,
                        null)
                .build(APPLICATION_TYPE);
    }

    public void run(boolean doRecoveryFromTimestamp, String diffusion_server) throws IOException, InitException, InterruptedException {
        System.out.println("\tRunning()...");
    }


    private static void logEntry(String s) {
        System.out.println(s);
    }

    @Override
    public StreamingSourceHandler addStreamingSource(
            String serviceName,
            ServiceDefinition serviceDefinition,
            Publisher publisher,
            StateHandler stateHandler) throws InvalidConfigurationException {

        System.out.println("\taddStreamingSource()...");
        final Map<String, Object> parameters =
                serviceDefinition.getParameters();
        final SourceConfig sourceConfig =
                sourceConfigValidator.validateAndGet(parameters);

        System.out.println("Setting Streaming Source");
        return new SportradarStreamingSourceHandler(
                sourceConfig.getFileName(),
                sourceConfig.getDiffusionTopicName(),
                stateHandler,
                publisher,
                token);

    }

    @Override
    public CompletableFuture<?> stop() {
        return CompletableFuture.completedFuture(null);
    }

}
