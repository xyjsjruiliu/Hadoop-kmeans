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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * map reduce tf-idf second steps
 * Created by hadoop on 12/14/16.
 */
public class WordCountsInDocuments extends Configured implements Tool {

    // where to put the data in hdfs when we're done
    private static final String OUTPUT_PATH = "/usr/shen/chinesewebkmeans/wordcount2";

    // where to read the data from.
    private static final String INPUT_PATH = "/usr/shen/chinesewebkmeans/wordcount";

    public static class WordCountsForDocsMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private Text docName = new Text();
        private Text wordAndCount = new Text();

        /**
         * 调整顺序
         * @param key 单词@文本编号
         * @param value 单词在文本中出现次数
         * @param context context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String wordAndDocCounter = value.toString().split("\t")[1];
            String[] wordAndDoc = value.toString().split("\t")[0].split("@");
            if (wordAndDoc.length == 2) {
                //文本编号
                this.docName.set(wordAndDoc[1]);
                //单词 + = + 次数
                this.wordAndCount.set(wordAndDoc[0] + "=" + wordAndDocCounter);
                //key : 文本编号
                //value : 单词 + = + 次数
                context.write(this.docName, this.wordAndCount);
            }
        }
    }

    public static class WordCountsForDocsReducer extends
            Reducer<Text, Text, Text, Text> {

        private Text wordAtDoc = new Text();
        private Text wordAvar = new Text();

        /**
         * 统计每个文本中有多少个单词
         * @param key 文本编号
         * @param values 单词 + = + 次数
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //每个文本中单词的总数
            int sumOfWordsInDocument = 0;
            //Map <单词, 单词出现次数>
            Map<String, Integer> tempCounter = new HashMap<String, Integer>();
            for (Text val : values) {
                String[] wordCounter = val.toString().split("=");
                tempCounter
                        .put(wordCounter[0], Integer.valueOf(wordCounter[1]));
                sumOfWordsInDocument += Integer.parseInt(wordCounter[1]);
            }
            for (String wordKey : tempCounter.keySet()) {
                //单词 + @ + 文本编号
                this.wordAtDoc.set(wordKey + "@" + key.toString());
                //单词出现次数 + / + 文本单词总数
                this.wordAvar.set(tempCounter.get(wordKey) + "/"
                        + sumOfWordsInDocument);
                //key : 单词 + @ + 文本编号
                //value : 单词出现次数 + / + 文本单词总数
                context.write(this.wordAtDoc, this.wordAvar);
            }
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Words Counts");

        job.setJarByClass(WordCountsInDocuments.class);
        job.setMapperClass(WordCountsForDocsMapper.class);
        job.setReducerClass(WordCountsForDocsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static int start(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(),
                new WordCountsInDocuments(), args);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new WordCountsInDocuments(), args);
        System.exit(res);
    }
}
