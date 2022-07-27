package com.sportradar.unifiedodds.example.examples;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pushtechnology.gateway.framework.exceptions.InvalidConfigurationException;

import java.util.Map;

/**
 * Validator for {@link SourceConfig} instance.
 *
 * @author Push Technology Limited
 */
public final class SourceConfigValidator {

    private final ObjectMapper objectMapper;

    public SourceConfigValidator(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    com.sportradar.unifiedodds.example.examples.SourceConfig validateAndGet(Map<String, Object> parameters) throws InvalidConfigurationException {
        final SourceConfig sourceConfig =
            objectMapper.convertValue(parameters, SourceConfig.class);

        final String diffusionTopicName = sourceConfig.getDiffusionTopicName();

        if (diffusionTopicName == null ||
            diffusionTopicName.isEmpty()) {

            throw new InvalidConfigurationException(
                "Invalid config value");
        }
        System.out.println("Config is good!");
        return sourceConfig;
    }
}
