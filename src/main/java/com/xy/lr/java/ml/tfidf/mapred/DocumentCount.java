package com.xy.lr.java.ml.tfidf.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 统计一共有多少篇文章
 * Created by hadoop on 12/14/16.
 */
public class DocumentCount extends Configured implements Tool {
    /**
     *
     * @param args
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Document Counts");

        job.setJarByClass(DocumentCount.class);
        job.setMapperClass(DocumentCountMapper.class);
        job.setReducerClass(DocumentCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class DocumentCountMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(new Text("all"), new IntWritable(1));
        }
    }

    public static class DocumentCountReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text Key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable intWritable : values) {
                count += 1;
            }
            context.write(new Text("ALL"), new IntWritable(count));
        }
    }

    public static void main(String[] args) {
        int res = 0;
        try {
            res = ToolRunner.run(new Configuration(),
                    new DocumentCount(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(res);
    }
}
