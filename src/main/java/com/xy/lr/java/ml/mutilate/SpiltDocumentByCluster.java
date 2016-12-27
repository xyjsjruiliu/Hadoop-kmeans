package com.xy.lr.java.ml.mutilate;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 从HBase文献表中到出数据
 * Created by hadoop on 12/23/16.
 */
public class SpiltDocumentByCluster extends Configured implements Tool {
    public static class SpiltDocumentByClusterMapper
            extends TableMapper<Text, Text> {
        //聚类编号
        public String clusterNumber = "";

        /**
         * 初始化聚类编号
         * @param context
         */
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            clusterNumber = conf.get("cluster.number");
        }

        public void map(ImmutableBytesWritable row, Result value, Context context)
                throws IOException, InterruptedException {
//            System.out.println(String.valueOf(row.get()));
            Cell[] cells = value.rawCells();

            //文档编号
            String documentID = "";
            //聚类编号
            String num = "";

            for(Cell cell:cells){
                documentID = new String(CellUtil.cloneRow(cell));
                String colFamily = new String(CellUtil.cloneFamily(cell));
                String col = new String(CellUtil.cloneQualifier(cell));

                if (colFamily.equals("inf") && col.equals("newCluster")) {
                    num = new String(CellUtil.cloneValue(cell));
                }
            }

            //聚类编号 + 文档编号
            context.write(new Text(num), new Text(documentID));
        }
    }

    public static class SpiltDocumentByClusterReducer
            extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Text value : values)
                count++;

            context.write(key, new Text(String.valueOf(count)));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("cluster.number", args[2]);
        Job job = Job.getInstance(conf, "SpiltDocumentByCluster");
        job.setJarByClass(SpiltDocumentByCluster.class);

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        TableMapReduceUtil.initTableMapperJob(
                args[0],        // input HBase table name
                scan,             // Scan instance to control CF and attribute selection
                SpiltDocumentByClusterMapper.class,   // mapper
                Text.class,             // mapper output key
                Text.class,             // mapper output value
                job);
        job.setReducerClass(SpiltDocumentByClusterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new SpiltDocumentByCluster(), args);
        System.exit(res);
    }
}
