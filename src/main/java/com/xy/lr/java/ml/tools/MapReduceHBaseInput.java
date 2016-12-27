package com.xy.lr.java.ml.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by hadoop on 12/16/16.
 */
public class MapReduceHBaseInput extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "MapReduceHBaseInput");
        job.setJarByClass(MapReduceHBaseInput.class);

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        TableMapReduceUtil.initTableMapperJob(
                "person",        // input HBase table name
                scan,             // Scan instance to control CF and attribute selection
                MapReduceHBaseInputMapper.class,   // mapper
                Text.class,             // mapper output key
                Text.class,             // mapper output value
                job);
        TableMapReduceUtil.initTableReducerJob(
                "liurui",      // output table
                MapReduceHBaseInputReducer.class,             // reducer class
                job);
//        job.setOutputFormatClass(NullOutputFormat.class);   // because we aren't emitting anything

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class MapReduceHBaseInputMapper
            extends TableMapper<Text, Text> {
        public void map(ImmutableBytesWritable row, Result value, Context context)
                throws IOException, InterruptedException {
//            System.out.println(String.valueOf(row.get()));
            Cell[] cells = value.rawCells();
            for(Cell cell:cells){
                System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
                System.out.println("Timetamp:"+cell.getTimestamp()+" ");
                System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
                System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
                System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
            }
            context.write(null, null);
        }
    }

    public static class MapReduceHBaseInputReducer
            extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
        public static final byte[] CF = "cf".getBytes();
        public static final byte[] COUNT = "count".getBytes();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int i = 0;
            for (IntWritable val : values) {
                i += val.get();
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.add(CF, COUNT, Bytes.toBytes(i));

            context.write(null, put);
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MapReduceHBaseInput(),
                args);
        System.exit(res);
    }
}
