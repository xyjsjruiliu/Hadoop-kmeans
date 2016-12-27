package com.xy.lr.java.ml.tfidf.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 文档生成编号
 * Created by hadoop on 12/16/16.
 */
public class DocumentIndex extends Configured implements Tool {
    public static class DocumentIndexMapper extends TableMapper<Text, Text> {
        // 匹配中文正则表达式
        private static final Pattern PATTERN = Pattern
                .compile("[\u4e00-\u9fa5]");
        // 记录单词
        private Text word = new Text();
        // 记录出现次数
        private Text singleCount = new Text("1");

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
            String kw = "";
            String abs = "";

            for(Cell cell : cells){
                String colFamily = new String(CellUtil.cloneFamily(cell));
                String col = new String(CellUtil.cloneQualifier(cell));
                if (colFamily.equals("inf") && col.equals("abstracts")) {
                    documentName = new String(CellUtil.cloneRow(cell));
                    abs = new String(CellUtil.cloneValue(cell));
                }
                if (colFamily.equals("inf") && col.equals("keywords")) {
                    kw = new String(CellUtil.cloneValue(cell));
                }
            }


            //key : word + @ + 文本编号
            this.word.set(abs + ":" + kw + "@" + documentName);
            //value : 次数1
            context.write(this.singleCount, this.word);
        }
    }

    public static class MapReduceHBaseInputReducer
            extends TableReducer<Text, Text, ImmutableBytesWritable> {
        public static final byte[] CF = "inf".getBytes();
        public static final byte[] ABS = "abstracts".getBytes();
        public static final byte[] KW = "keywords".getBytes();

        public static Map<Integer, String> indexOfDocument = new HashMap<Integer, String>();

        public static int count = 1;

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                Put put = new Put(Bytes.toBytes(String.valueOf(count)));
                String documentID = val.toString().substring(val.toString().indexOf("@") + 1);
                String v = val.toString().split("@")[0];

                if (v.split(":").length == 2) {
                    String abs = v.split(":")[0];
                    String kw = v.split(":")[1];

                    //关键词
                    put.addColumn(CF, KW, kw.getBytes());
                    //摘要
                    put.addColumn(CF, ABS, abs.getBytes());

                    indexOfDocument.put(count, documentID);

                    count++;
                    context.write(null, put);
                }
            }
        }

        /**
         * 写入index文件
         * @param context
         */
        protected void cleanup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            Path outPath = new Path("indexDocument.txt");

            FileSystem fs = FileSystem.get(conf);
            fs.delete(outPath, true);
            final SequenceFile.Writer out = SequenceFile.createWriter(fs, conf,
                    outPath, IntWritable.class, Text.class);
            for (Map.Entry<Integer, String> entry : indexOfDocument.entrySet()) {
                out.append(new IntWritable(entry.getKey()),
                        new Text(entry.getValue()));
            }
            out.close();
        }
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(configuration);
        Job job = Job.getInstance(conf, "DocumentIndex");
        job.setJarByClass(DocumentIndex.class);

        conf.set("index.document.path", args[1]);
//        conf.set("fs.default.name", "hdfs://hadoop-server:9000");

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        //HBase mapper
        TableMapReduceUtil.initTableMapperJob(
                args[0],        // input HBase table name
                scan,             // Scan instance to control CF and attribute selection
                DocumentIndexMapper.class,   // mapper
                Text.class,             // mapper output key
                Text.class,             // mapper output value
                job);
        TableMapReduceUtil.initTableReducerJob(
                "PaperIndexed",      // output table
                MapReduceHBaseInputReducer.class,             // reducer class
                job);
        job.setNumReduceTasks(1);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new DocumentIndex(), args);
        System.exit(res);
    }
}
