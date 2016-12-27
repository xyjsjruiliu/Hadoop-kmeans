package com.xy.lr.java.ml.tfidf.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * map reduce tf-idf third steps
 * Created by hadoop on 12/14/16.
 */
public class WordsInCorpusTFIDF extends Configured implements Tool {

    public static class WordsInCorpusTFIDFMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private Text wordAndDoc = new Text();
        private Text wordAndCounters = new Text();

        /**
         * map 函数
         * @param key 单词 + @ + 文本编号
         * @param value 单词出现次数 + / + 文本单词总数
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] wordAndDoc = value.toString().split("\t")[0].split("@"); // 3/1500
            //
//            if (!WEB_INDEX.containsKey((Long.parseLong(wordAndDoc[1]))))
//                WEB_INDEX.put(Long.parseLong(wordAndDoc[1]),
//                        Long.parseLong(wordAndDoc[1]));
            //单词
            this.wordAndDoc.set(new Text(wordAndDoc[0]));
            //文本编号 + = + (单词出现次数 + / + 文本单词总数)
            this.wordAndCounters.set(wordAndDoc[1] + "=" + value.toString().split("\t")[1]);
            //key : 单词
            //value : 文本编号 + = + (单词出现次数 + / + 文本单词总数)
            context.write(this.wordAndDoc, this.wordAndCounters);
        }

    }

    public static class WordsInCorpusTFIDFReducer extends
            Reducer<Text, Text, Text, Text> {

        private Text wordAtDocument = new Text();
        private Text tfidfCounts = new Text();

        //词典
        private static List<String> wordList = new ArrayList<String>();

        //文档个数
        private String documentCount;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            Configuration configuration = context.getConfiguration();
            this.documentCount = configuration.get("document.count");
            super.setup(context);
        }

        /**
         *
         * @param key 单词
         * @param values 文本编号 + = + (单词出现次数 + / + 文本单词总数)
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            if (!wordList.contains(key.toString())) {
                wordList.add(key.toString());
            }

            //key : 单词 + "\t" + 文档编号
            //value : tf值
            Map<String, Double> wordAndTF = new HashMap<String, Double>();

            int count = 0;
            //单词
            String word = key.toString();
            for (Text value : values) {
                count++;
                String documentID = value.toString()
                        .substring(0, value.toString().indexOf("="));
                String wordCountAtDocument = value.toString()
                        .substring(value.toString().indexOf("=") + 1).split("/")[0];
                String wordAllCountAtDocument = value.toString()
                        .substring(value.toString().indexOf("=") + 1).split("/")[1];

                Double wordTF = Double.valueOf(wordCountAtDocument) /
                        Double.valueOf(wordAllCountAtDocument);
                wordAndTF.put(word + "\t" + documentID, wordTF);
            }

            for (Map.Entry<String, Double> entry : wordAndTF.entrySet() ) {
                Double idf = Math.log(Double.valueOf(documentCount) / count + 1.0);
                this.wordAtDocument.set(entry.getKey());
                this.tfidfCounts.set(String.valueOf(idf * entry.getValue()));

                //key : 单词 + \t + documentID
                //value : tfidf值
                context.write(this.wordAtDocument, this.tfidfCounts);
            }
        }

        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            Configuration conf = context.getConfiguration();
            Path outPath = new Path(conf.get("dictionary.path"));

            FileSystem fs = FileSystem.get(conf);
            fs.delete(outPath, true);
            final SequenceFile.Writer out = SequenceFile.createWriter(fs, conf,
                    outPath, Text.class, LongWritable.class);
            for (String word : wordList) {
                out.append(new Text(word),
                        new LongWritable(1));
            }
            out.close();
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //文章数量
        conf.set("document.count", args[2]);
        //词典路径
        conf.set("dictionary.path", args[3]);

        Job job = new Job(conf, "TF-IDF of Words in Corpus");

        job.setJarByClass(WordsInCorpusTFIDF.class);
        job.setMapperClass(WordsInCorpusTFIDFMapper.class);
        job.setReducerClass(WordsInCorpusTFIDFReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static int start(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(), new WordsInCorpusTFIDF(),
                args);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordsInCorpusTFIDF(),
                args);
        System.exit(res);
    }
}