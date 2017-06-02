/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package averagebystate;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
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
public class AverageByState {

    /**
     * @param args the command line arguments
     */
    public static class AverageMapper extends Mapper<Object, Text, Text, CountAverage>{
       private CountAverage countAverage = new CountAverage();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String state;
            double stars;
            String line = value.toString();
            String[] tuple = line.split("\\n");
            try{
                for(int i=0;i<tuple.length; i++){
                    JSONObject obj = new JSONObject(tuple[i]);
                    state=obj.getString("state");
                    stars = obj.getDouble("stars");
                    
                    countAverage.setCount(1);
                countAverage.setAverage(stars);
                    context.write(new Text(state), countAverage);
                }
            }catch(JSONException e){
                System.out.println("averagebystate.AverageByState.AverageMapper.map()");
            }
        }
        
    }
    
    public static class AverageReducer extends Reducer<Text, CountAverage, Text, CountAverage>{
        private CountAverage result = new CountAverage();
        @Override
        protected void reduce(Text key, Iterable<CountAverage> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;
            for(CountAverage val : values){
                
//                context.write(key, val);
               sum += val.getCount() * val.getAverage();
                count += val.getCount();
            }
            result.setCount(count);
            result.setAverage(sum/count);
            context.write(key, result);
        }
        
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
         Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"P_Average");
        job.setJarByClass(AverageByState.class);
        job.setMapperClass(AverageMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CountAverage.class);
        job.setCombinerClass(AverageReducer.class);
        job.setReducerClass(AverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CountAverage.class);
       // System.out.println("Main method");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
