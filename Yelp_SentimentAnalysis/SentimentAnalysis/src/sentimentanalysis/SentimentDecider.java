/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sentimentanalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 *
 * @author meenakshi
 */
public class SentimentDecider {

    private final static SentimentDecider decider = new SentimentDecider();

    public static SentimentDecider getInstance() {
        return decider;
    }

    public HashMap<String, Double> lexicon = new HashMap<String, Double>();

    private SentimentDecider() {
    }

    public void decide(String line) throws IOException {
   
            String type = "";
            String word = "";
            String polarity = "";

            String[] tokens = line.split(" ");
            for (String token : tokens) {

                if (!token.contains("=")) {
                    continue;
                }

                String[] obj = token.split("=");
                String key = obj[0];
                String val = obj[1];

                switch (key) {
                    case "type":
                        type = val;
                        break;
                    case "word1":
                        word = val;
                        break;
                    case "priorpolarity":
                        polarity = val;
                        break;                 
                }
            }

            double value;
            if (type.equals("strongsubj")) {
                value = 5.0;
            } else {
                value = 1.0;
            }

            if (polarity.equals("negative")) {
                value = -value;
            }

            lexicon.put(word, value);
           
    }
}
