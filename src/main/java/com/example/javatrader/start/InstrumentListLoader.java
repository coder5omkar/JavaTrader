package com.example.javatrader.start;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

public class InstrumentListLoader {

    private static final String INSTRUMENT_LIST_URL = "https://images.dhan.co/api-data/api-scrip-master.csv";
    private List<Map<String, String>> instruments;

    public InstrumentListLoader() throws Exception {
        this.instruments = loadInstrumentList();
    }

    private List<Map<String, String>> loadInstrumentList() throws Exception {
        List<Map<String, String>> instrumentList = new ArrayList<>();
        URL url = new URL(INSTRUMENT_LIST_URL);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
            String headerLine = reader.readLine();
            if (headerLine == null) {
                throw new Exception("Instrument list CSV is empty.");
            }
            String[] headers = headerLine.split(",");
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(",", -1);
                Map<String, String> instrument = new HashMap<>();
                for (int i = 0; i < headers.length && i < fields.length; i++) {
                    instrument.put(headers[i].trim(), fields[i].trim());
                }
                instrumentList.add(instrument);
            }
        }
        return instrumentList;
    }

    public List<Map<String, String>> getInstruments() {
        return instruments;
    }
}

