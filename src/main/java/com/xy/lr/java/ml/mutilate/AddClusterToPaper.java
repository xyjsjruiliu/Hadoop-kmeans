package com.xy.lr.java.ml.mutilate;

import com.xy.lr.java.ml.tools.SequenceFileBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 把聚类好的标签加入到Paper表中
 * Created by hadoop on 12/22/16.
 */
public class AddClusterToPaper extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("indexDocument", args[2]);
        Job job = Job.getInstance(conf, "AddClusterToPaper");
        job.setJarByClass(AddClusterToPaper.class);

        job.setMapperClass(InputMapper.class);
        TableMapReduceUtil.initTableReducerJob(
                args[1],      // output table
                HBaseInputReducer.class,             // reducer class
                job);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     *
     */
    public static class InputMapper extends Mapper<Text, Text, Text, Text> {
        public Map<Integer, String> map = new HashMap<Integer, String>();

        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            String path = conf.get("indexDocument");

            map = SequenceFileBuilder.readIndexSequenceFile(path, conf);
        }

        /**
         * 输出聚类信息
         * @param key 文档编号
         * @param value 聚类编号 + \t + 向量
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String documentID = key.toString();
            String clusterID = value.toString().split("\t")[0];

            if (map.get(Integer.valueOf(documentID)) != null) {
                String doc = map.get(Integer.valueOf(documentID));

                //key : 文档编号
                //val : 聚类编号
                context.write(new Text(doc), new Text(clusterID));
            }
        }
    }

    public static class HBaseInputReducer
            extends TableReducer<Text, Text, ImmutableBytesWritable> {
        public static final byte[] CF = "inf".getBytes();
        public static final byte[] CLUSTER = "newCluster".getBytes();

        /**
         * 输出聚类中心到HBase表中
         * @param key 文档编号
         * @param values 聚类编号
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                Put put = new Put(Bytes.toBytes(key.toString()));
                put.add(CF, CLUSTER, Bytes.toBytes(val.toString()));

                context.write(null, put);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new AddClusterToPaper(),
                args);
        System.exit(res);
    }
}
