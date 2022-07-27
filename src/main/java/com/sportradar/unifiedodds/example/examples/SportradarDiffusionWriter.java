package com.sportradar.unifiedodds.example.examples;

import com.pushtechnology.gateway.framework.Publisher;
import com.pushtechnology.gateway.framework.exceptions.PayloadConversionException;
import com.sportradar.unifiedodds.sdk.MarketDescriptionManager;
import com.sportradar.unifiedodds.sdk.OddsFeed;
import com.sportradar.unifiedodds.sdk.OddsFeedListener;
import com.sportradar.unifiedodds.sdk.OddsFeedSession;
import com.sportradar.unifiedodds.sdk.SportsInfoManager;
import com.sportradar.unifiedodds.sdk.entities.BasicTournament;
import com.sportradar.unifiedodds.sdk.entities.Category;
import com.sportradar.unifiedodds.sdk.entities.Match;
import com.sportradar.unifiedodds.sdk.entities.Season;
import com.sportradar.unifiedodds.sdk.entities.Sport;
import com.sportradar.unifiedodds.sdk.entities.SportEvent;
import com.sportradar.unifiedodds.sdk.entities.Stage;
import com.sportradar.unifiedodds.sdk.entities.Tournament;
import com.sportradar.unifiedodds.sdk.entities.markets.MarketMappingData;
import com.sportradar.unifiedodds.sdk.entities.markets.Specifier;
import com.sportradar.unifiedodds.sdk.oddsentities.BetCancel;
import com.sportradar.unifiedodds.sdk.oddsentities.BetSettlement;
import com.sportradar.unifiedodds.sdk.oddsentities.BetStop;
import com.sportradar.unifiedodds.sdk.oddsentities.FixtureChange;
import com.sportradar.unifiedodds.sdk.oddsentities.MarketStatus;
import com.sportradar.unifiedodds.sdk.oddsentities.MarketWithOdds;
import com.sportradar.unifiedodds.sdk.oddsentities.OddsChange;
import com.sportradar.unifiedodds.sdk.oddsentities.OutcomeOdds;
import com.sportradar.unifiedodds.sdk.oddsentities.Producer;
import com.sportradar.unifiedodds.sdk.oddsentities.RollbackBetCancel;
import com.sportradar.unifiedodds.sdk.oddsentities.RollbackBetSettlement;
import com.sportradar.unifiedodds.sdk.oddsentities.UnparsableMessage;
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public class SportradarDiffusionWriter implements OddsFeedListener {
    private final Logger    logger;
    private final OddsFeed oddsFeed;
    private final Publisher publisher;
    private final Locale locale;
    private final SportsInfoManager sportsInfoManager;
    private final MarketDescriptionManager marketDescriptionManager;

    private Map<String, JSONObject> sportMap;
    private Map<String, JSONObject> countryMap;
    private Map<String, JSONObject> competitionMap;
    private Map<String, JSONObject> eventMap;
    private Map<String,  String>    sportCountries;
    private Random                  random;
    private SimpleDateFormat        formatter;
    private TreeMap<Double, String> oddsMap;
    private JSONObject              FACupOdds;
    private String                  FACupTopic;
    private boolean                 randomisationStarted;
    private final ScheduledExecutorService scheduler;

    private double upper;
    private double lower;


    public SportradarDiffusionWriter(
            OddsFeed inOddsFeed, Publisher inPublisher, String listener_version) {
        this.oddsFeed                   = inOddsFeed;
        this.sportsInfoManager          = oddsFeed.getSportsInfoManager();
        this.marketDescriptionManager   = oddsFeed.getMarketDescriptionManager();
        this.publisher          = inPublisher;
        this.logger             = LoggerFactory.getLogger(this.getClass().getName() + "-" + listener_version);
        this.sportMap       = new HashMap();
        this.countryMap     = new HashMap(); //  All these value mappings are currently unused.
        this.competitionMap = new HashMap(); //  They are kept as lookup value caches in case they
        this.eventMap       = new HashMap(); //  are required in future.   Customer requirements
        this.sportCountries = new HashMap(); //  are changing so that may bt the case.   If not,
        this.oddsMap        = new TreeMap(); //  they can be deleted.
        this.locale         = Locale.ENGLISH;
        this.FACupTopic     = "Sports/Soccer/ENG/FA Cup/FA Cup - Winner";
        this.random         = new Random();
        this.formatter      = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
        this.scheduler      = Executors.newScheduledThreadPool(1);
        this.randomisationStarted   = false;
        this.upper = 100.0;
        this.lower = -100.0;

        String oddsMappings = "/Users/dcarson/OddsMappings.csv";
        try(BufferedReader br = new BufferedReader(new FileReader(oddsMappings))) {
            String line = "";
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                Double fixed = Double.parseDouble(values[1]);
                oddsMap.put(fixed, values[0]);
            }
        } catch (FileNotFoundException e) {
            logger.error("Couldn't find Odds Mappings CSV! - {}", e.getMessage());
        }
        catch (IOException ex) {
            logger.error("Couldn't process Odds Mappings CSV! - {}", ex.getMessage());
        }

        /** Now read the sample odds **/
        try {
            String faCupJSON = "/Users/dcarson/FACupOdds.json";
            InputStream is = new FileInputStream(faCupJSON);
            String jsonTxt = IOUtils.toString(is, "UTF-8");
            this.FACupOdds = new JSONObject(jsonTxt);
        } catch (FileNotFoundException e) {
            logger.error("Couldn't find FA Cup Odds JSON! - {}", e.getMessage());
        }
        catch (IOException ex) {
            logger.error("Couldn't process FA Cup Odds JSON! - {}", ex.getMessage());
        }

    }


    /**
     * Any kind of odds update, or betstop signal results in an OddsChanges Message.
     *
     * @param sender the session
     * @param oddsChanges the odds changes message
     */
    @Override
    public void onOddsChange(OddsFeedSession sender, OddsChange<SportEvent> oddsChanges) {
        String      feedRootTopic   = "Sportradar/";
        String      sportsRootTopic = "Sports/";

        // Now loop through the odds for each market
        for (MarketWithOdds marketLine : oddsChanges.getMarkets()) {
            // Now loop through the outcomes within this particular market
            String marketDescription = marketLine.getName();
            marketDescription = marketDescription.replace('/', ',');
            String rawTopicName = feedRootTopic + marketDescription;
            logger.info("Received OddsChange for Producer {} (ID: {}).", oddsChanges.getProducer().getName(), oddsChanges.getProducer().getId());

            JSONObject  joMarketLine  = new JSONObject();
            joMarketLine.put("Status", marketLine.getStatus());

            // If the market is active, capture all odds/fixed odds for all outcomes, then specifiers
            if (marketLine.getStatus() == MarketStatus.Active) {
                Map<String, String> outcomeOddsMap = new HashMap();
                JSONArray outcomeOddsArray = new JSONArray();

                for (OutcomeOdds outcomeOdds : marketLine.getOutcomeOdds()) {
                    String outcomeDesc = outcomeOdds.getName();

                    JSONObject tmpOddsObject = new JSONObject();
                    tmpOddsObject.put("Desc",   outcomeDesc);

                    String fixed = oddsMap.get(oddsMap.floorKey(outcomeOdds.getOdds()));
                    tmpOddsObject.put("FixedOdds",        fixed);
                    tmpOddsObject.put("Odds",             outcomeOdds.getOdds());

                    try {
                        outcomeOddsMap.put(outcomeDesc, tmpOddsObject.toString());
                        outcomeOddsArray.put(tmpOddsObject);
                    }
                    catch(Exception ex) {
                        logger.error(ex.toString());
                    }
                }
                joMarketLine.put("Odds", outcomeOddsArray);

                //Finally add specifiers
                /*
                JSONObject tmpSpecifierMap = new JSONObject();
                for (Iterator<Map.Entry<String, String>> entries = marketLine.getSpecifiers().entrySet().iterator(); entries.hasNext(); ) {
                    Map.Entry<String, String> entry = entries.next();
                    tmpSpecifierMap.put(entry.getKey(),entry.getValue());
                }
                joMarketLine.put("Specifiers", tmpSpecifierMap);*/
            }

            joMarketLine.put("MarketLineID",    marketLine.getId());

            try {
                /*** Process message type as received **/

                    JSONObject eventDetails = getEventDetails(oddsChanges.getEvent());
                    String eventTopic = sportsRootTopic + getTopicPath(eventDetails);
                    Date date = new Date(System.currentTimeMillis());  //Date to format updates
                    joMarketLine.put("Updated", formatter.format(date));
                    try {
                        String topic = eventTopic + "/" + marketLine.getName(locale);
                        logger.info("Published OddsChange to topic {}", topic);
                        publisher.publish(topic, joMarketLine);

                        int initialDelay = 0;
                        int delay = 5;

                        //BELOW IS NEEDED FOR TRACKING FA CUP AND RANDOMISING ITS DATA - SEE RANDOMISE FA CUP FUNCTION
                        if(topic.equals(FACupTopic)) {

                            if (!randomisationStarted) {
                                FACupOdds = joMarketLine;

                                randomisationStarted = true;
                            }
                        }
                    }
                    catch(PayloadConversionException ex) {
                        logger.error("Payload Conversion Error - unable to publish to Diffusion", ex);
                    }
                    catch (Exception pubProblem) {
                        logger.error("Failed to publish to Diffusion - {}", pubProblem.getMessage());
                    }
              }
              catch (Exception except) {
                logger.error("Failed to publish to Diffusion - {}", except.getMessage());
              }
        }
    }

    /*** A runnable to create random FACup odds ***/
    Runnable randomiseIt = new Runnable() {
        @Override
        public void run() {

            randomiseFACup();
        }
    };

    /*********************************************/
    /** Obtain Topic information from event     **/
    /** - This takes a SportEvent and returns   **/
    /**   the sport, country, name details      **/

    private JSONObject getEventDetails(SportEvent event) {
        String sport        = "unknown-sport";
        String country      = "unknown-country";
        String competition  = "unknown-competition";
        String eventName    = "unknown-eventName";

        try {
            if (event instanceof Match) {
                Match match = (Match) event;
                sport = match.getTournament().getSport().getName(locale);
                Tournament t = (Tournament) match.getTournament();
                country = t.getCategory().getCountryCode() == null ? "INTL" : t.getCategory().getCountryCode();
                eventName = match.getTournament().getName(locale);
            }
            else if (event instanceof BasicTournament) {
                BasicTournament tournament = (BasicTournament) event;
                sport = tournament.getSport().getName(locale);
                country = tournament.getCategory().getCountryCode() == null ? "INTL" : tournament.getCategory().getCountryCode();
                eventName = tournament.getName(locale);
            }
            else if ((event instanceof Tournament) && !(event instanceof BasicTournament)) {
                Tournament tournament = (Tournament) event;
                sport = tournament.getSport().getName(locale);
                country = tournament.getCategory().getCountryCode() == null ? "INTL" : tournament.getCategory().getCountryCode();
                eventName = tournament.getName(locale);
            }
            else if (event instanceof Season) {
                Season season = (Season) event;
                sport = season.getSport().getName(locale);
                country = season.getTournamentInfo().getCategory().getCountryCode() == null ? "INTL" : season.getTournamentInfo().getCategory().getCountryCode();
                eventName = season.getTournamentInfo().getName(locale);
            } else if (event instanceof Stage) {
                Stage stage = (Stage) event;
                if(stage.getSport() != null)    sport = stage.getSport().getName(locale);
                if(stage.getCategory() != null) {
                    country = stage.getCategory().getCountryCode() == null ? "INTL" : stage.getCategory().getCountryCode();
                }
                eventName = stage.getName(locale);
            }
        }
        catch (ClassCastException h) {
            logger.error("Couldn't cast to Match, Tournament, Season nor Stage - {}", h.getMessage());
        }
        catch(Exception i) {
            logger.error("Sunable to parse event - {}", i.getMessage());
            logger.error("Event was of type {} ", event.getClass().getName());
        }
        if(sport.equals("unknown-sport")) {
            logger.error("Was unable to parse details of this sport!");
        }

        JSONObject topicDetails = new JSONObject();
        topicDetails.put("sport",       sport);
        topicDetails.put("country",     country);
        topicDetails.put("name",        eventName);
        topicDetails.put("competition", "");

        return topicDetails;
    }

    /*************************************
    * Takes a JSONObject and returns
    * a path string
     * **********************************/
    private String getTopicPath(JSONObject input) {
        return input.get("sport") + "/" + input.get("country") + "/" + input.get("name");
    }

    /*************************************
     * Randomise FA Cup data - outer function
     * can be used for any topic/odds
     * **********************************/

    public JSONObject publishRandomOdds(String topic, JSONObject content) {
        JSONArray oldArray = content.getJSONArray("Odds");
        JSONArray newArray = new JSONArray();
        for (int i = 0; i < oldArray.length(); i++) {
            JSONObject oddsEntry = oldArray.getJSONObject(i);
            JSONObject newEntry  = new JSONObject();
            Double newAbsolute = 0.0;
            try {
                String fixedOdds = oddsEntry.get("FixedOdds").toString();
                String newFixedOdds = "";

                String name = "";
                if(oddsEntry.get("Desc") != null) {
                    name = oddsEntry.get("Desc").toString();
                }

                Set<Map.Entry<Double, String>> entries = oddsMap.entrySet();
                for (Map.Entry<Double, String> entry : entries) {
                    if (entry.getValue().equals(fixedOdds)) {
                        Double adjustment = Math.random() * (upper - lower) + lower;

                        newAbsolute = entry.getKey() + adjustment < 0 ? entry.getKey() - adjustment : entry.getKey() + adjustment + 1;
                        newFixedOdds = oddsMap.get(oddsMap.floorKey(newAbsolute));
                        break;
                    }
                }
                String prettyOdds = new DecimalFormat("#.##").format(newAbsolute);
                newEntry.put("Desc", name);
                newEntry.put("FixedOdds", newFixedOdds);
                newEntry.put("Odds", prettyOdds);
                newArray.put(newEntry);
            }
            catch(Exception e) {
                System.out.println("Caught execption - " + e.getMessage());
            }
        }
        content.remove("Odds");
        content.put("Odds", newArray);
        try {
            Date date = new Date(System.currentTimeMillis());  //Date to format updates
            content.put("Updated", formatter.format(date));
            publisher.publish(topic, content);
        }
        catch(PayloadConversionException ex) {
            logger.error("Unable to publish new odds to Diffusion - {}", ex.getMessage());
        }
        catch(Exception e) {
            System.out.println("Caught publishing exception! " + e.getMessage());
        }
        return content;
    }

    public void randomiseFACup(){
        FACupOdds = publishRandomOdds(FACupTopic, FACupOdds);
    }

    /**
     * Send to rapidly suspend a set of markets (often all)
     *
     * @param sender the session
     * @param betStop the betstop message
     */
    @Override
    public void onBetStop(OddsFeedSession sender, BetStop<SportEvent> betStop) {
        logger.info("Received betstop for sport event " + betStop.getEvent());
    }

    /**
     * The onBetSettlement callback is received whenever a BetSettlement message is received. It
     * contains information about what markets that should be settled how. All markets and outcomes
     * that you have received odds changes messages for at some point in time you will receive
     * betsettlement messages for at some later point in time. That is if you receive odds for
     * outcome X for market Y, you will at a later time receive a BetSettlement message that
     * includes outcome X for market Y.
     *
     * @param sender the session
     * @param clearBets the BetSettlement message
     */
    @Override
    public void onBetSettlement(OddsFeedSession sender, BetSettlement<SportEvent> clearBets) {
        String      outcomeStub     = "Outcomes/";

        return; //UNCOMMENT ME WHEN OUTCOMES ARE TO BE INCLUDED
        // Iterate through the betsettlements for each market
        /*
        for (MarketWithSettlement marketSettlement : clearBets.getMarkets()) {
            // Then iterate through the result for each outcome (win or loss)
            MarketDefinition def =  marketSettlement.getMarketDefinition();

            for (OutcomeSettlement result : marketSettlement.getOutcomeSettlements()) {
                JSONObject  outcomeContent  = new JSONObject();
                String outcomeTopic = outcomeStub + marketSettlement.getName(locale) + "/" + result.getName(locale);;

                if (result.isWinning()) {
                    outcomeContent.put("Outcome", "Win");
                }
                else {
                    outcomeContent.put("Outcome", "Loss");
                }

                try {
                    publisher.publish(outcomeTopic, outcomeContent);
                }
                catch(PayloadConversionException ex) {
                    logger.warn("Unable to publish fixture to Diffusion - {}", ex.getMessage());
                    System.exit(1);
                }
            }
        }*/
    }

    /**
     * If a BetSettlement was generated in error, you may receive a RollbackBetsettlement and have
     * to try to do whatever you can to undo the BetSettlement if possible.
     *
     * @param sender the session
     * @param rollbackBetSettlement the rollbackBetSettlement message referring to a previous
     *        BetSettlement
     */
    @Override
    public void onRollbackBetSettlement(OddsFeedSession sender, RollbackBetSettlement<SportEvent> rollbackBetSettlement) {
        logger.info("Received rollback betsettlement for sport event " + rollbackBetSettlement.getEvent());
    }

    /**
     * If the markets were cancelled you may receive a
     * {@link BetCancel} describing which markets were
     * cancelled
     *
     * @param sender the session
     * @param betCancel A {@link BetCancel} instance
     *        specifying which markets were cancelled
     */
    @Override
    public void onBetCancel(OddsFeedSession sender, BetCancel<SportEvent> betCancel) {
        logger.info("Received bet cancel for sport event " + betCancel.getEvent());
    }

    /**
     * If the bet cancellations were send in error you may receive a
     * {@link RollbackBetCancel} describing the
     * erroneous cancellations
     *
     * @param sender the session
     * @param rbBetCancel A {@link RollbackBetCancel}
     *        specifying erroneous cancellations
     */
    @Override
    public void onRollbackBetCancel(OddsFeedSession sender, RollbackBetCancel<SportEvent> rbBetCancel) {
        logger.info("Received rollback betcancel for sport event " + rbBetCancel.getEvent());
    }

    /**
     * If there are important fixture updates you will receive fixturechange message. The thinking
     * is that most fixture updates are queried by you yourself using the SportInfoManager. However,
     * if there are important/urgent changes you will also receive a fixture change message (e.g. if
     * a match gets delayed, or if Sportradar for some reason needs to stop live coverage of a match
     * etc.). This message allows you to promptly respond to such changes
     *
     * @param sender the session
     * @param fixtureChange the SDKFixtureChange message - describing what sport event and what type
     *        of fixture change
     */
    @Override
    public void onFixtureChange(OddsFeedSession sender, FixtureChange<SportEvent> fixtureChange) {
        String fixtureStub = "Fixtures/";

        JSONObject fixtureContent = new JSONObject();
        fixtureContent.put("ChangeType",   fixtureChange.getChangeType().toString());
        fixtureContent.put("Producer",     fixtureChange.getProducer().getName());
        fixtureContent.put("Event",        fixtureChange.getEvent().getName(locale));

        String fixtureTopic = fixtureStub + getTopicPath(getEventDetails(fixtureChange.getEvent()));
        try {
            publisher.publish(fixtureTopic, fixtureContent);
        }
        catch(PayloadConversionException ex) {
            logger.warn("Unable to publish fixture to Diffusion - {}", ex.getMessage());
            System.exit(1);
        }
    }

    /**
     * This handler is called when the SDK detects that it has problems parsing a certain message.
     * The handler can decide to take some custom action (shutting down everything etc. doing some
     * special analysis of the raw message content etc) or just ignore the message. The SDK itself
     * will always log that it has received an unparseable message and will ignore the message so a
     * typical implementation can leave this handler empty.
     *
     * @deprecated in favour of {{@link #onUnparsableMessage(OddsFeedSession, UnparsableMessage)}} from v2.0.11
     *
     * @param sender the session
     * @param rawMessage the raw message received from Betradar
     * @param event if the SDK was able to extract the event this message is for it will be here
     *        otherwise null
     */
    @Override
    @Deprecated
    public void onUnparseableMessage(OddsFeedSession sender, byte[] rawMessage, SportEvent event) {
        if (event != null) {
            logger.info("Problems deserializing received message for event " + event.getId());
        } else {
            logger.info("Problems deserializing received message"); // probably a system message deserialization failure
        }
    }

    /**
     * This handler is called when the SDK detects that it has problems parsing/dispatching a message.
     * The handler can decide to take some custom action (shutting down everything etc. doing some
     * special analysis of the raw message content etc) or just ignore the message. The SDK itself
     * will always log that it has received an unparseable message.
     *
     * @param sender            the session
     * @param unparsableMessage A {@link UnparsableMessage} instance describing the message that had issues
     * @since v2.0.11
     */
    @Override
    public void onUnparsableMessage(OddsFeedSession sender, UnparsableMessage unparsableMessage) {
        Producer possibleProducer = unparsableMessage.getProducer(); // the SDK will try to provide the origin of the message

        if (unparsableMessage.getEvent() != null) {
            logger.info("Problems detected on received message for event " + unparsableMessage.getEvent().getId());
        } else {
            logger.info("Problems detected on received message"); // probably a system message deserialization failure
        }
    }

    /**
     * This method will fetch all Market Mappings and publish to Diffusion.
     * Not currently used.
     */
    public void fetchMarketMappings() {
        JSONArray marketDefArray = new JSONArray();
        String marketsRootTopic = "MarketMappings/";

        logger.info("Starting fetch of Market Mappings");

        marketDescriptionManager.getMarketDescriptions()
                .forEach(market -> {
                    JSONObject marketDef     = new JSONObject();
                    marketDef.put("Id",    market.getId());
                    marketDef.put("Name",  market.getName(locale));
                    marketDef.put("Desc",  market.getDescription(locale) == null ? "" : market.getDescription(locale));
                    marketDef.put("Groups", market.getGroups());
                    marketDef.put("Specifiers", market.getSpecifiers() == null ? "" : market.getSpecifiers().stream().map(Specifier::getName).collect(Collectors.joining(",")));

                    if (market.getMappings() != null) {
                        JSONObject mappingsJSON = new JSONObject();

                        for(MarketMappingData mm : market.getMappings())
                        {
                            mappingsJSON.put("ProducerIds",  mm.getProducerIds());
                            mappingsJSON.put("SportId",      mm.getSportId());
                            mappingsJSON.put("MarketId",     mm.getMarketId());
                            mappingsJSON.put("MarketSubId",  mm.getMarketSubTypeId());
                            mappingsJSON.put("Valid For",    mm.getValidFor() == null ? "" : mm.getValidFor());
                            mappingsJSON.put("SovTemplate",  mm.getSovTemplate());
                        }

                        marketDef.put("Mappings", mappingsJSON);
                    }

                    try {
                        publisher.publish(marketsRootTopic + market.getId(), marketDef);
                    }
                    catch(PayloadConversionException ple) {
                        logger.error("Payload Conversion Error - unable to publish Markets to Diffusion", ple.getMessage());
                    }
                    //marketDefArray.put(marketDef);
                });
    }

    /**
     * This method will fetch all Sports Entity data and publish to Diffusion.
     * Not currently used.
     */

    public void fetchSportsData() {
        String sportsRootTopic      = "Sports/";
        int maxEvents               = 1000;
        int batchCount              = 10;

        logger.info("Starting fetch of Sports Data");

        if(sportsInfoManager == null) {

            logger.error("Unable to access sports data from Sportsradar");
            return;
        }

        try {

            List<Sport> sports = sportsInfoManager.getSports(locale);
            for (Sport sport : sports) {
                JSONObject joSportInfo = new JSONObject();
                String sportNameSanitised = sport.getName(locale).replace('/', ',');

                joSportInfo.put("SportId", sport.getId().toString());
                //Publish Sport
                publisher.publish(sportsRootTopic + sportNameSanitised, joSportInfo);

                /****************/
                /** Categories **/
                JSONArray jaCategories = new JSONArray();
                List<Category> categories = sport.getCategories();
                for (Category category : categories) {

                    String categoryNameSanitised = category.getName(locale).replace('/', ',');
                    String countryNameSanitised  = category.getCountryCode() == null ? "INTL" : category.getCountryCode().replace('/', ',');

                    JSONObject joCats  = new JSONObject();
                    JSONObject joComps = new JSONObject();
                    joCats.put("CategoryId", category.getId().toString());

                    //publisher.publish(sportsRootTopic + sportNameSanitised + "/Categories/" + countryNameSanitised + "/" + categoryNameSanitised, joCats);

                    JSONArray jaEvents = new JSONArray();
                    List<SportEvent> tournaments = category.getTournaments();
                    for (SportEvent sportEvent : tournaments) {
                        String sportEventNameSanitised = sportEvent.getName(locale).replace('/', ',');

                        JSONObject joSportEvent = new JSONObject();
                        joSportEvent.put("SportEventId", sportEvent.getSportId().toString());
                        if(sportEvent.getScheduledTime() != null) {
                            joSportEvent.put("ScheduledTime", sportEvent.getScheduledTime().toString());
                        }

                        String eventTopic = sportsRootTopic + sportNameSanitised + "/" + countryNameSanitised + "/" + sportEventNameSanitised;
                        publisher.publish(eventTopic,   joSportEvent);
                        jaEvents.put(joSportEvent);
                    }
                    joCats.put("Events", jaEvents);
                    jaCategories.put(joCats);
                    joSportInfo.put("Categories", jaCategories);
                }

                sportMap.put(sport.getName(locale), joSportInfo);
            }
        }
        catch (PayloadConversionException ple) {
              logger.error("Payload Conversion Error - unable to publish Sport/Category/Event data to Diffusion", ple.getMessage());
        }
    }
}
