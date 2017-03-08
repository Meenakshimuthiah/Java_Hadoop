/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package SecondarySort;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
public class SecondarySort {

    /**
     */
    public static class CompositeKeyMapper extends Mapper<Object, Text, CompositeKey, Text>{
        private CompositeKey c = new CompositeKey();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            if(!value.toString().contains("stock")){
            String[] values = value.toString().split(",");
            String stock_vol = values[7];
            String symbol = values[1];
            
            int stock = Integer.parseInt(stock_vol);
            
           Text date = new Text(values[2]);
            
            c.setSymbol(symbol);
            c.setStock_vol(stock);
            context.write(c, date);
            }
            
        }
    }
        
        public static class CompositeKeyReducer extends Reducer<CompositeKey, Text,CompositeKey, Text>{
    
            @Override
            public void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
                for(Text val:values){
                    context.write(key, val);
                }
            }
        }
       
        public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
            Job job = Job.getInstance(new Configuration(), "Secondary Sorting");
		job.setJarByClass(SecondarySort.class);

		// Mapper configuration
		job.setMapperClass(CompositeKeyMapper.class);
		//job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(Text.class);

		// Partitioning/Sorting/Grouping configuration
		job.setPartitionerClass(KeyPartitioner.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
		job.setGroupingComparatorClass(GroupingComparator.class);

		// Reducer configuration
		job.setReducerClass(CompositeKeyReducer.class);
		job.setOutputKeyClass(CompositeKey.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

        }
    
}
