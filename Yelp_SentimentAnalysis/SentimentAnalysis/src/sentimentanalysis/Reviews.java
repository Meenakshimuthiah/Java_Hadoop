/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sentimentanalysis;

/**
 *
 * @author meenakshi
 */
public class Reviews {

    public String review_id;
    public String user_id;
    public String business_id;
    public String stars;
    public String date;
    public String text;
    public String useful;
    public String funny;
    public String cool;
    public String type;

    public Reviews() {

    }

    public double calculate(SentimentDecider decider) {

       

        double sentimentValue = 2.5;

        String[] tokens = this.text.split(" ");

        for (String token : tokens) {
               // return decider.lexicon.size();
            if (decider.lexicon.containsKey(token)) {
               
                double value = decider.lexicon.get(token);

                sentimentValue += value;
            }
        }
       return sentimentValue;
    }
}
