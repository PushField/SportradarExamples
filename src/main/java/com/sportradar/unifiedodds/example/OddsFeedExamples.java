/*
 * Copyright (C) Sportradar AG. See LICENSE for full license governing this code
 */

package com.sportradar.unifiedodds.example;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportradar.unifiedodds.example.examples.AdvancedConfigurationSetup;
import com.sportradar.unifiedodds.example.examples.AvailableMarketsPrinter;
import com.sportradar.unifiedodds.example.examples.DataSessionSetup;
import com.sportradar.unifiedodds.example.examples.DiffusionSessionSetup;
import com.sportradar.unifiedodds.example.examples.ExportImportSetup;
import com.sportradar.unifiedodds.example.examples.MultiSessionSetup;
import com.sportradar.unifiedodds.example.examples.MultithreadedSessionSetup;
import com.sportradar.unifiedodds.example.examples.ReplaySessionSetup;
import com.sportradar.unifiedodds.example.examples.SingleSessionSetup;
import com.sportradar.unifiedodds.example.examples.SourceConfigValidator;
import com.sportradar.unifiedodds.example.examples.SportEventDataPrinter;
import com.sportradar.unifiedodds.sdk.exceptions.InitException;

import java.io.IOException;
import java.util.Scanner;

import static com.pushtechnology.gateway.framework.DiffusionGatewayFramework.initialize;

/**
 * The SDK basic example selector
 */
public class OddsFeedExamples {

    public static void main(String[] args) throws IOException, InitException, InterruptedException {
        System.out.println("OddsFeedExamples - example selector, START");
        System.out.println();

        doExampleSelection();
    }

    private static void doExampleSelection() throws IOException, InitException, InterruptedException {
        System.out.println("Available examples: (select the one you would like to run)");
        System.out.println("  1 - Sample setup: Basic \t\t\t\t\t\tSingle session (full odds recovery)");
        System.out.println("  2 - Sample setup: Multi-session \t\t\t\tMultiple sessions: Prematch, Liveodds, Virtuals (full odds recovery)");
        System.out.println("  3 - Sample setup: Basic (recovery timestamp) \tSingle session (recovery for the last 2 hours - on all active producers)");
        System.out.println("  4 - Sample setup: Full message data \t\t\tSingle session (recovery for the last 2 hours - on all active producers)");
        System.out.println("  5 - Extra: Print available markets \t\t\tList the full static market descriptions list");
        System.out.println("  6 - Extra: Print available market mappings \tList the available static market description mappings");
        System.out.println("  7 - Extra: Replay Server \t\t\t\t\t\tA simple replay demonstration");
        System.out.println("  8 - Extra: Print sport event data \t\t\tPrints out static sport event data");
        System.out.println("  9 - Extra: Advanced configuration setup \t\tA basic demonstration on how to do an advanced configuration setup");
        System.out.println(" 10 - Extra: Cache export/import \t\t\t\tA basic demonstration on how to export/import current cache state");
        System.out.println(" 11 - Extra: Advanced setup \t\t\t\t\tSingle session with multithreaded message parsing");
        System.out.println(" 12 - Extra: Diffusion Client\t\t\t\t\tSingle session (full odds recovery) published into Push Technology's Diffusion");

        String selection = getConsoleInput();

        System.out.println("Please enter a valid Unified Feed token for integration environment:");
        String token = getConsoleInput();

        switch (selection) {
            case "1":
                SingleSessionSetup singleSessionSetup = new SingleSessionSetup(token);
                singleSessionSetup.run(false);
                break;
            case "2":
                MultiSessionSetup multiSessionSetup = new MultiSessionSetup(token);
                multiSessionSetup.run();
                break;
            case "3":
                SingleSessionSetup singleSessionSetupRecovery = new SingleSessionSetup(token);
                singleSessionSetupRecovery.run(true);
                break;
            case "4":
                DataSessionSetup dataSessionSetup = new DataSessionSetup(token);
                dataSessionSetup.run(true);
                break;
            case "5":
                AvailableMarketsPrinter availableMarketsPrinter = new AvailableMarketsPrinter(token);
                availableMarketsPrinter.print();
                break;
            case "6":
                AvailableMarketsPrinter availableMarketsPrinterMappings = new AvailableMarketsPrinter(token);
                availableMarketsPrinterMappings.print(true);
                break;
            case "7":
                ReplaySessionSetup replaySessionSetup = new ReplaySessionSetup(token);
                replaySessionSetup.run();
                break;
            case "8":
                SportEventDataPrinter sportEventDataPrinter = new SportEventDataPrinter(token);
                sportEventDataPrinter.print();
                break;
            case "9":
                AdvancedConfigurationSetup advancedConfigurationSetup = new AdvancedConfigurationSetup(token);
                advancedConfigurationSetup.run();
                break;
            case "10":
                ExportImportSetup exportImportSetup = new ExportImportSetup(token);
                exportImportSetup.run();
                break;
            case "11":
                MultithreadedSessionSetup multithreadedSessionSetup = new MultithreadedSessionSetup(token);
                multithreadedSessionSetup.run(false);
                break;
            case "12":
                System.out.println("Creating new DiffusionSessionSetup...");
                final DiffusionSessionSetup diffusionSessionApplication =
                        new DiffusionSessionSetup(new SourceConfigValidator(new ObjectMapper()));
                System.out.println("Calling setToken()...");
                diffusionSessionApplication.setToken(token);
                System.out.println("Initializing...");
                initialize(diffusionSessionApplication)
                        .connect();
                break;
            default:
                System.out.println();
                System.out.println("Invalid example id selected, please select a valid example id.");
                System.out.println();
                doExampleSelection();
                break;
        }
    }

    private static String getConsoleInput() {
        Scanner scanner = new Scanner(System.in);
        if (scanner.hasNextLine()) {
            return scanner.nextLine();
        } else {
            return getConsoleInput();
        }
    }
}
