/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package checkin;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author meenakshi
 */
public class Checkin {

    public static class JoinMapper1 extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String user_id;

            String line = value.toString();
            String[] tuple = line.split("\\n");
            try {
                for (int i = 0; i < tuple.length; i++) {
                    JSONObject obj = new JSONObject(tuple[i]);
                    user_id = obj.getString("business_id");
                    outkey.set(user_id);
                    // Flag this record for the reducer and then output
                    outvalue.set("A" + value.toString());
                    context.write(outkey, outvalue);
                }
            } catch (JSONException e) {
                System.out.println("averagebystate.AverageByState.AverageMapper.map()");
            }
        }
    }

    public static class JoinMapper2 extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String user_id;

            String line = value.toString();
            String[] tuple = line.split("\\n");
            try {
                for (int i = 0; i < tuple.length; i++) {
                    JSONObject obj = new JSONObject(tuple[i]);
                    user_id = obj.getString("business_id");
                    outkey.set(user_id);
                    outvalue.set("B" + value.toString());
                    context.write(outkey, outvalue);
                }
            } catch (JSONException e) {
                System.out.println("averagebystate.AverageByState.AverageMapper.map()");
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        private static final Text EMPTY_TEXT = new Text("");
        private Text tmp = new Text();
        private ArrayList<Text> listA = new ArrayList<Text>();
        private ArrayList<Text> listB = new ArrayList<Text>();
        private String joinType = null;

        @Override
        public void setup(Context context) {
            joinType = context.getConfiguration().get("join.type");
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            listA.clear();
            listB.clear();
            while (values.iterator().hasNext()) {
                tmp = values.iterator().next();
                System.out.println(Character.toString((char) tmp.charAt(0)));
                if (Character.toString((char) tmp.charAt(0)).equals("A")) {

                    listA.add(new Text(tmp.toString().substring(1)));
                }
                if (Character.toString((char) tmp.charAt(0)).equals("B")) {

                    listB.add(new Text(tmp.toString().substring(1)));
                }
                System.out.println(tmp);
            }
            System.out.println(listB.size());
            executeJoinLogic(context);
        }

        private void executeJoinLogic(Context context) throws IOException, InterruptedException {

            if (joinType.equalsIgnoreCase("inner")) {

                if (!listA.isEmpty() && !listB.isEmpty()) {
                    System.out.println("here");
                    for (Text A : listA) {

                        for (Text B : listB) {

                            context.write(A, B);
                        }
                    }
                }
            } else if (joinType.equalsIgnoreCase("leftouter")) {

                for (Text A : listA) {

                    if (!listB.isEmpty()) {
                        for (Text B : listB) {
                            context.write(A, B);
                        }
                    } else {

                        context.write(A, EMPTY_TEXT);
                    }
                }
            } else if (joinType.equalsIgnoreCase("rightouter")) {

                for (Text B : listB) {

                    if (!listA.isEmpty()) {
                        for (Text A : listA) {
                            context.write(A, B);
                        }
                    } else {

                        context.write(EMPTY_TEXT, B);
                    }
                }
            } else if (joinType.equalsIgnoreCase("fullouter")) {

                if (!listA.isEmpty()) {

                    for (Text A : listA) {

                        if (!listB.isEmpty()) {
                            for (Text B : listB) {
                                context.write(A, B);
                            }
                        } else {

                            context.write(A, EMPTY_TEXT);
                        }
                    }
                } else {

                    for (Text B : listB) {
                        context.write(EMPTY_TEXT, B);
                    }
                }
            } else if (joinType.equalsIgnoreCase("anti")) {

                if (listA.isEmpty() ^ listB.isEmpty()) {

                    for (Text A : listA) {
                        context.write(A, EMPTY_TEXT);
                    }
                    for (Text B : listB) {
                        context.write(EMPTY_TEXT, B);
                    }
                }
            }
        }
    }

    public static class ChainMapper extends Mapper<Object, Text, CompositeKey, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String time;
            String business_id;
            int check_in;
            String[] line = value.toString().split("\\t");

            try {
                JSONObject object = new JSONObject(line[0]);
                JSONObject obj = new JSONObject(line[1]);
                business_id = object.getString("state");
                JSONArray array = obj.getJSONArray("time");
                for (int j = 0; j < array.length(); j++) {
                    time = array.get(j).toString();
                    String[] check_ins = time.split(":");
                    check_in = Integer.parseInt(check_ins[1]);
                    CompositeKey c = new CompositeKey(business_id, check_in);
                    context.write(c, new Text(time));
                }

            } catch (JSONException e) {
            }

        }
    }

    public static class ChainReducer extends Reducer<CompositeKey, Text, CompositeKey, Text> {

        @Override
        protected void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;

            for (Text t : values) {
                if (count < 5) {
                    context.write(key, t);
                    count++;
                }
            }

        }

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "ReduceSideJoin");
        job.setJarByClass(Checkin.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, JoinMapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, JoinMapper2.class);
        job.getConfiguration().set("join.type", "inner");

        job.setReducerClass(JoinReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean complete = job.waitForCompletion(true);

        Configuration conf2 = new Configuration();

        Job job1 = Job.getInstance(conf2, "chain");
        if (complete) {
            job1.setJarByClass(Checkin.class);

            job1.setMapperClass(ChainMapper.class);

            job1.setReducerClass(ChainReducer.class);

            FileInputFormat.addInputPath(job1, new Path(args[2]));
            FileOutputFormat.setOutputPath(job1, new Path(args[3]));
job1.setMapOutputKeyClass(CompositeKey.class);
job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(CompositeKey.class);
            job1.setOutputValueClass(Text.class);

            System.exit(job1.waitForCompletion(true) ? 0 : 1);
        }
    }
}
