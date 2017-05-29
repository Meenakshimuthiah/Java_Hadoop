/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sentimentanalysis;

import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author meenakshi
 */
public class SentimentAnalysis {

    private static SentimentAnalysis analysis = new SentimentAnalysis();

    public static class SentimentMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        Configuration conf;
        String instance;
        SentimentDecider decider;
        Gson gson = new Gson();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            instance = conf.get("decider");
            decider = gson.fromJson(instance, SentimentDecider.class);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Reviews reviews = gson.fromJson(value.toString(), Reviews.class);
            DoubleWritable sentimentValue = new DoubleWritable(reviews.calculate(decider));
            Text businessId = new Text(reviews.business_id + " (Stars: " + reviews.stars + ")");
            context.write(businessId, sentimentValue);
        }

    }

    public SentimentDecider sample() throws IOException {
        SentimentDecider decider = SentimentDecider.getInstance();
        return decider;

    }

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     * @throws java.lang.ClassNotFoundException
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here

        Configuration conf = new Configuration();
        SentimentDecider decider = analysis.sample();
        Path pt = new Path(args[0]);
        FileSystem local = FileSystem.getLocal(conf);
        BufferedReader buffer = new BufferedReader(new InputStreamReader(local.open(pt)));
        String line;
        while ((line = buffer.readLine()) != null) {
            decider.decide(line);
        }
        Gson gson = new Gson();
        String d = gson.toJson(decider);
        conf.set("decider", d);
        Job job = Job.getInstance(conf, "sentiment");

        job.setJarByClass(SentimentAnalysis.class);

        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapperClass(SentimentMapper.class);
         job.setReducerClass(SentimentReducer.class);
        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(DoubleWritable.class);
              job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
