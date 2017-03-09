/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package movierating;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author meenakshi
 */
public class MovieRating {

    public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) {
            String row[] = value.toString().split("::");
            String movieId = row[1];
            String rating = row[2];

            try {
                IntWritable ratings = new IntWritable(Integer.parseInt(rating));
                context.write(new Text(movieId), ratings);
            } catch (NumberFormatException | IOException | InterruptedException e) {

            }
        }
    }

    public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable totalRating = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();

            }
            totalRating.set(sum);
            context.write(key, totalRating);
        }
    }

    public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) {
            String[] row = (value.toString()).split("\\t");
            Text movieId = new Text(row[0]);
            String rating = row[1].trim();

            try {
                IntWritable count = new IntWritable(Integer.parseInt(rating));
                context.write(count, movieId);
            } catch (NumberFormatException | IOException | InterruptedException e) {

            }
        }
    }

    public static class Reduce2 extends Reducer<IntWritable, Text, Text, IntWritable> {
        int counter = 0;
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if(counter<25){
            for (Text val : values) {
                
                context.write(val, key);
                counter++;
                if(counter>25)
                    break;
                }
                
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "MovieRating");
        job1.setJarByClass(MovieRating.class);
        job1.setMapperClass(Map1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setReducerClass(Reduce1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        boolean complete = job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "MovieRating");

        if (complete) {
            job2.setJarByClass(MovieRating.class);
            job2.setMapperClass(Map2.class);
            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setSortComparatorClass(SortKeyComaparator.class);

            job2.setReducerClass(Reduce2.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }

    }

}
