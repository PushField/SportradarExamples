package com.sportradar.unifiedodds.example.examples;

import com.pushtechnology.gateway.framework.Publisher;
import com.pushtechnology.gateway.framework.StateHandler;
import com.pushtechnology.gateway.framework.StreamingSourceHandler;
import com.pushtechnology.gateway.framework.exceptions.InvalidConfigurationException;
import com.sportradar.unifiedodds.example.common.GlobalEventsListener;
import com.sportradar.unifiedodds.example.common.SdkConstants;
import com.sportradar.unifiedodds.sdk.MessageInterest;
import com.sportradar.unifiedodds.sdk.OddsFeed;
import com.sportradar.unifiedodds.sdk.cfg.Environment;
import com.sportradar.unifiedodds.sdk.cfg.OddsFeedConfiguration;
import com.sportradar.unifiedodds.sdk.exceptions.InitException;
import com.sportradar.unifiedodds.sdk.ProducerManager;
import com.sportradar.unifiedodds.sdk.oddsentities.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;

import static com.pushtechnology.gateway.framework.DiffusionGatewayFramework.newSourceServicePropertiesBuilder;

/**
 * Implementation of {@link StreamingSourceHandler} which listens to Sportradar Odds feed
 * changes and publishes contents to Diffusion server.
 *
 * @author Push Technology Limited
 */

final class SportradarStreamingSourceHandler implements StreamingSourceHandler {

    private static final Logger LOG =
            LoggerFactory.getLogger(SportradarStreamingSourceHandler.class);

    private final StateHandler stateHandler;
    private final Publisher publisher;
    private final String diffusionTopicName;
    private final String fileName;
    private final OddsFeed oddsFeed;
    private final String token;
    private final SportradarDiffusionWriter diffusionWriter;

    private boolean is_ok = true;
    private Random  random;

    SportradarStreamingSourceHandler(
            final String fileName,
            final String diffusionTopicName,
            final StateHandler stateHandler,
            final Publisher publisher,
            final String token) {

        this.diffusionTopicName = diffusionTopicName;
        this.stateHandler = stateHandler;
        this.publisher = publisher;
        this.fileName = fileName;
        this.token    = token;
        this.random   = new Random();

        LOG.info("Running the OddsFeed SDK Basic example - Diffusion session");

        LOG.info("Building the configuration using the provided token");
        OddsFeedConfiguration configuration = OddsFeed.getOddsFeedConfigurationBuilder()
                .setAccessToken(token)
                .selectEnvironment(Environment.GlobalIntegration)
                .setSdkNodeId(SdkConstants.NODE_ID)
                .setDefaultLocale(Locale.ENGLISH)
                .build();

        LOG.info("Creating a new OddsFeed instance");
        oddsFeed = new OddsFeed(new GlobalEventsListener(), configuration);
        this.diffusionWriter = new SportradarDiffusionWriter(oddsFeed, publisher, "DiffusionSessionSetup");

        LOG.info("Building a simple session which will receive all messages");
        oddsFeed.getSessionBuilder()
                .setMessageInterest(MessageInterest.AllMessages)
                .setListener(diffusionWriter)
                .build();

        /** This code will create a random feed for FA Cup **/
        new Thread(() -> {
            while(true) {
                diffusionWriter.randomiseFACup();
                int wait = random.nextInt(10 - 1 + 1) + 1;
                try {
                    TimeUnit.SECONDS.sleep(wait);
                } catch (InterruptedException ex) {
                    LOG.error("Randomising thread was interruped - {}", ex.getMessage());
                }
            }
        }).start();
        /** Delete the above when FA Cup randomisation is no longer required **/
    }

    /** Set Producer Recovery Timestamp **/
    /** Set to be 2hrs earlier          **/

    private void setProducersRecoveryTimestamp() {
        LOG.info("Setting last message timestamp(used for recovery) for all the active producers to two hours back");

        // using the timestamp from 2 hours back, in real case scenarios you need to monitor the timestamp for recovery
        // with the producerManager.getProducer(producerId).getTimestampForRecovery(); method
        long recoveryFromTimestamp = System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(2, TimeUnit.HOURS);

        ProducerManager producerManager = oddsFeed.getProducerManager();
//        producerManager.getActiveProducers().values().forEach(p -> producerManager.setProducerRecoveryFromTimestamp(p.getId(), recoveryFromTimestamp));
        for (Producer p : producerManager.getActiveProducers().values()) {
            LOG.info("Setting Producer {} to have recoveryFromTimestamp {}", p.getId(), recoveryFromTimestamp);
            producerManager.setProducerRecoveryFromTimestamp(p.getId(), recoveryFromTimestamp);
        }
    }

    private void pause() {

        LOG.info("Pausing");

    }

    /* Overrides of abstract methods */

    @Override
    public SourceServiceProperties getServiceProperties() throws InvalidConfigurationException {
        return
                newSourceServicePropertiesBuilder()
                        .updateMode(SourceServiceProperties.UpdateMode.STREAMING)
                        //.payloadConvertor("Sportradar_to_JSON")
                        .build();
    }

    @Override
    public CompletableFuture<?> pause(PauseReason reason) {
        pause();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<?> resume(ResumeReason reason) {
        is_ok = true;

        return start();
    }

    @Override
    public CompletableFuture<?> start() {

        try {
            LOG.info("Setting recovery timestamp to 2hrs before");
            setProducersRecoveryTimestamp();
            LOG.info("Opening the feed instance");
            oddsFeed.open();
            /*LOG.info("Fetching Sport Data");
            diffusionWriter.fetchSportsData();
            LOG.info("Fetching Market Mappings");
            diffusionWriter.fetchMarketMappings();
            LOG.info("Successfully obtained Sport Data.");*/
        }
        catch(InitException ex) {
            LOG.info("Unable to connect to Odds feed - {}", ex.getMessage());
        }

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<?> stop() {
        pause();

        return CompletableFuture.completedFuture(null);
    }
}
