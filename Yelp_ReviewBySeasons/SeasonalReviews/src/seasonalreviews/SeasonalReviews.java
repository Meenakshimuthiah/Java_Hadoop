/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package seasonalreviews;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author meenakshi
 */
public class SeasonalReviews {

    /**
     * @param args the command line arguments
     */
    
    public static class SeasonalMapper extends Mapper<Object, Text, CompositeKey, IntWritable>{
        CompositeKey c = new CompositeKey();
        IntWritable count = new IntWritable(1);
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
              String business_id;
            String date;
            String line = value.toString();
            String[] tuple = line.split("\\n");

            try {
                for (int i = 0; i < tuple.length; i++) {
                    JSONObject obj = new JSONObject(tuple[i]);
                    business_id = obj.getString("business_id");
                    c.setBusiness_id(business_id);
                    date = obj.getString("date");
                    String datevalues[] = date.split("-");
                    String month = datevalues[1];
                    int m = Integer.parseInt(month);
                    if(m>=0 && m<=3){
                        c.setSeason("winter");
                    }
                    if(m>=4 && m<=5){
                        c.setSeason("spring");
                    }
                    if(m>=6 && m<=8){
                        c.setSeason("summer");
                    }
                    if(m>=9 && m<=12){
                        c.setSeason("fall");
                    }
                       context.write(c, count);
                }
            } catch (JSONException e) {
            }
        }
        
    }
    
    public static class SeasonalReducer extends Reducer<CompositeKey, IntWritable, CompositeKey, IntWritable>{

        @Override
        protected void reduce(CompositeKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val:values){
                sum+= val.get();
            }
            context.write(key, new IntWritable(sum));
        }
        
    }
    public static void main(String[] args) {
        // TODO code application logic here
         try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Unique");
            job.setJarByClass(SeasonalReviews.class);
            job.setMapperClass(SeasonalMapper.class);
            job.setMapOutputKeyClass(CompositeKey.class);
            job.setMapOutputValueClass(IntWritable.class);
            // job.setCombinerClass(UniqueReducer.class);
            job.setReducerClass(SeasonalReducer.class);
            job.setOutputKeyClass(CompositeKey.class);
            job.setOutputValueClass(IntWritable.class);
            // System.out.println("Main method");
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException ex) {
            Logger.getLogger(SeasonalMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
}
