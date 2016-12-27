package com.xy.lr.java.ml.tfidf.mapred;

import org.ansj.domain.Result;
import org.ansj.splitWord.analysis.ToAnalysis;
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
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 提取中文,分词,去停词,统计词频
 * Created by hadoop on 12/14/16.
 */
public class WordFrequenceInDocument extends Configured implements Tool {

    // 输入目录
    private String INPUT_PATH;
    // 输出目录
    private String OUTPUT_PATH;


    public static class WordFrequenceInDocMapper extends
            Mapper<LongWritable, Text, Text, IntWritable> {

        // 匹配中文正则表达式
        private static final Pattern PATTERN = Pattern
                .compile("[\u4e00-\u9fa5]");
        // 记录单词
        private Text word = new Text();
        // 记录出现次数
        private IntWritable singleCount = new IntWritable(1);

        /**
         * map 函数
         * @param key 行偏移量
         * @param value 行值
         * @param context context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            int index = value.toString().indexOf("\t");
            String documentNumber = value.toString().substring(0, index);

            String document = value.toString().substring(index + "\t".length());

            Matcher m = PATTERN.matcher(document);
            // 记录一条记录中所有中文
            StringBuilder valueBuilder = new StringBuilder();
            // 过滤中文
            while (m.find()) {
                String matchkey = m.group();
                valueBuilder.append(matchkey);
            }
            String text = valueBuilder.toString();
            // 使用Ansj工具包分词
            String result = ToAnalysis.parse(text).toStringWithOutNature(",");

            for (String word : result.split(",")) {
                //key : word + @ + 文本编号
                this.word.set(word + "@" + documentNumber);
                //value : 次数1
                context.write(this.word, this.singleCount);
            }

            valueBuilder.setLength(0);

        }
    }

    public static class WordFrequenceInDocReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        // 记录词语出现次数
        private IntWritable wordSum = new IntWritable();

        /**
         * 统计词频
         * @param key word + @ + 文本编号
         * @param values 次数
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context) throws IOException, InterruptedException {
            // 统计词语出现次数
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            this.wordSum.set(sum);
            //key : word + @ + 文本编号
            //value : word在文本中出现次数
            context.write(key, this.wordSum);
        }
    }

    /**
     *
     * @param args 参数
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Word Frequence In Document");

        job.setJarByClass(WordFrequenceInDocument.class);
        job.setMapperClass(WordFrequenceInDocMapper.class);
        job.setReducerClass(WordFrequenceInDocReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /*public WordFrequenceInDocument(String input, String output) {
        this.INPUT_PATH = input;
        this.OUTPUT_PATH = output;
    }*/

    public static int start(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(),
                new WordFrequenceInDocument(), args);

    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new WordFrequenceInDocument(), args);
        System.exit(res);
    }
}

