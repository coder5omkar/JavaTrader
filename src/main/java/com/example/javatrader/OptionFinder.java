package com.example.javatrader;

import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.*;

public class OptionFinder {

    static class OptionData {
        String customSymbol;
        int strikePrice;
        LocalDate expiryDate;
        String type;
        int securityId;
    }

    public static void main(String[] args) throws IOException {
        String filePath = "instrument_cache.csv";
        int spotPrice = 24823;
        String index = "NIFTY";

        OptionData atmCall = findATMOption(filePath, spotPrice, "CALL", index);
        OptionData atmPut = findATMOption(filePath, spotPrice, "PUT", index);

        System.out.println("ATM CALL: " + (atmCall != null ? atmCall.customSymbol + " - Security ID: " + atmCall.securityId : "Not Found"));
        System.out.println("ATM PUT:  " + (atmPut != null ? atmPut.customSymbol + " - Security ID: " + atmPut.securityId : "Not Found"));
    }

    public static OptionData findATMOption(String filePath, int spotPrice, String type, String index) throws IOException {
        LocalDate today = LocalDate.now();

        return Files.lines(Paths.get(filePath))
                .skip(1)
                .map(line -> line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
                .filter(tokens -> tokens.length > 7)
                .map(tokens -> {
                    try {
                        String symbol = tokens[7].replaceAll("\"", "").trim(); // SEM_CUSTOM_SYMBOL
                        if (!symbol.startsWith(index + " ")) return null;
                        if (!symbol.endsWith(type)) return null;

                        int strike = extractStrike(symbol);
                        LocalDate expiry = extractExpiryDate(symbol);
                        if (expiry == null || expiry.isBefore(today) || expiry.getDayOfWeek() != DayOfWeek.THURSDAY) {
                            return null;
                        }

                        int secId = Integer.parseInt(tokens[2].trim()); // SEM_SMST_SECURITY_I

                        OptionData data = new OptionData();
                        data.customSymbol = symbol;
                        data.strikePrice = strike;
                        data.expiryDate = expiry;
                        data.type = type;
                        data.securityId = secId;
                        return data;
                    } catch (Exception e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .min(Comparator
                        .comparingInt((OptionData o) -> Math.abs(o.strikePrice - spotPrice))
                        .thenComparing(o -> o.expiryDate))
                .orElse(null);
    }

    private static int extractStrike(String symbol) {
        String[] parts = symbol.split(" ");
        for (int i = parts.length - 1; i >= 0; i--) {
            if (parts[i].matches("\\d+")) {
                return Integer.parseInt(parts[i]);
            }
        }
        return -1;
    }

    private static LocalDate extractExpiryDate(String symbol) {
        try {
            // Example: NIFTY 29 MAY 18250 CALL → extract 29 MAY
            String[] parts = symbol.split(" ");
            int day = Integer.parseInt(parts[1]);
            Month month = Month.valueOf(parts[2].toUpperCase());
            int year = Year.now().getValue(); // Assume current year

            LocalDate date = LocalDate.of(year, month, day);
            // If this date has already passed this year, assume next year expiry
            if (date.isBefore(LocalDate.now())) {
                date = date.plusYears(1);
            }

            return date;
        } catch (Exception e) {
            return null;
        }
    }


}
