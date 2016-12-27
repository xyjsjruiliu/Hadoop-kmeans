package com.xy.lr.java.ml.tfidf.mapred;

import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * map reduce tf-idf first steps
 * Created by hadoop on 12/16/16.
 */
public class WordFrequenceInDocumentHBase extends Configured implements Tool {
    public static class WordFrequenceInDocumentHBaseMapper
            extends TableMapper<Text, IntWritable> {
        // 匹配中文正则表达式
        private static final Pattern PATTERN = Pattern
                .compile("[\u4e00-\u9fa5]");
        // 记录单词
        private Text word = new Text();
        // 记录出现次数
        private IntWritable singleCount = new IntWritable(1);

        public String number = "";

        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            number = conf.get("cluster.name");

            System.out.println(number);
        }

        /**
         * 从HBase中取数据，分词，统计
         * @param row
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(ImmutableBytesWritable row, Result value, Context context)
                throws IOException, InterruptedException {
            Cell[] cells = value.rawCells();

            String documentName = "";
            String abs = "";
            String cluster = "";

            for(Cell cell : cells){
                String colFamily = new String(CellUtil.cloneFamily(cell));
                String col = new String(CellUtil.cloneQualifier(cell));
                if (colFamily.equals("inf") && col.equals("abstracts")) {
                    documentName = new String(CellUtil.cloneRow(cell));
                    abs = new String(CellUtil.cloneValue(cell));
                }
                if (colFamily.equals("inf") && col.equals("newCluster")) {
                    cluster = new String(CellUtil.cloneValue(cell));
                }
            }

            //分别不同的聚类
            if (cluster.equals(number)) {
                Matcher m = PATTERN.matcher(abs);
                // 记录一条记录中所有中文
                StringBuilder valueBuilder = new StringBuilder();
                // 过滤中文
                while (m.find()) {
                    String matchkey = m.group();
                    valueBuilder.append(matchkey);
                }
                String text = valueBuilder.toString();

                if(!text.equals("")) {
                    // 使用Ansj工具包分词
                    String result = ToAnalysis.parse(text).toString(",");

                    for (String word : result.split(",")) {
                        if(word.split("/").length == 2) {
                            String type = word.split("/")[1];
                            String w = word.split("/")[0];

                            if (type.indexOf("n") != -1 || type.equals("v")){
                                //key : word + @ + 文本编号
                                this.word.set(w + "@" + documentName);
                                //value : 次数1
                                context.write(this.word, this.singleCount);
                            }
                        }
                    }
                }
                valueBuilder.setLength(0);
            }
        }
    }

    public static class WordFrequenceInDocumentHBaseReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
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
     * 运行程序
     * @param args 参数
     * @return 运行成果结果
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("cluster.name", args[2]);
        Job job = Job.getInstance(conf, "WordFrequenceInDocumentHBase");
        job.setJarByClass(WordFrequenceInDocumentHBase.class);

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        //HBase mapper
        TableMapReduceUtil.initTableMapperJob(
                args[0],        // input HBase table name
                scan,             // Scan instance to control CF and attribute selection
                WordFrequenceInDocumentHBaseMapper.class,   // mapper
                Text.class,             // mapper output key
                IntWritable.class,             // mapper output value
                job);
        job.setReducerClass(WordFrequenceInDocumentHBaseReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static int start(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(),
                new WordFrequenceInDocumentHBase(), args);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new WordFrequenceInDocumentHBase(), args);
        System.exit(res);
    }
}
