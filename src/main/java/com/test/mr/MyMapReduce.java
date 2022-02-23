package com.test.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyMapReduce {
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        /****
         * Implement map
         * @param k1
         * @param v1
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable k1, Text v1, Mapper.Context context)
                throws IOException, InterruptedException {
            System.out.println("<k1, v1>=<" + k1.get()+","+v1.toString()+">");
            String[] words = v1.toString().split(" ");
            for (String word:words) {
                Text k2 = new Text(word);
                LongWritable v2 = new LongWritable(1L);
                System.out.println("k2:" + word + "...v2:1");
                context.write(k2, v2);
            }

        }
    }
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        /***
         *
         * @param k2
         * @param v2s
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text k2, Iterable<LongWritable> v2s, Context context)
                throws IOException, InterruptedException {
            long sum = 0L;
            for (LongWritable v2 : v2s){
                System.out.println("<k2, v2>=<" + k2.toString()+","+v2.get()+">");
                sum += v2.get();
            }
            Text k3 = k2;
            LongWritable v3 = new LongWritable(sum);
            System.out.println("<k3, v3>=<" + k3.toString() +", "+v3.get()+ ">");
            context.write(k3, v3);
        }
    }
    public static void main(String[] args){
        if(args.length!=2){
            System.exit(100);
        }
        try{
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf);
            job.setJarByClass(MyMapReduce.class);
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.setMapperClass(MyMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            job.setReducerClass(MyReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            job.waitForCompletion(true);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    }


