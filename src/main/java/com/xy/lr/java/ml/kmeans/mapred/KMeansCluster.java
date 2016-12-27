package com.xy.lr.java.ml.kmeans.mapred;

import com.xy.lr.java.ml.tools.SequenceFileBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
 * 聚类1
 * Created by hadoop on 12/13/16.
 */

public class KMeansCluster extends Configured implements Tool {
    public static class KMeansClusterMapper extends Mapper<Text, Text, Text, Text> {
        //聚类中心
        //key : 聚类编号
        //val : 聚类向量 word1:tfidf1,word2:tfidf2....
        private Map<String, String> vector = new HashMap<String, String>();

        /**
         * 读取词典，计算文档新的聚类编号
         * key : 聚类编号
         * value : 向量 word1:tfidf1,word2:tfidf2....
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            Configuration configuration = context.getConfiguration();
            String dictPath = configuration.get("default.vector.path");
            if (dictPath.startsWith("/newCluster_Vector")) {
                dictPath += "/part-r-00000";
            }

            //读取初始化向量
            this.vector = SequenceFileBuilder.readDefaultVectorSequenceFile(dictPath, configuration);
            super.setup(context);
        }

        /**
         * map function
         * @param key 文档编号
         * @param value 聚类编号 + \t + 向量 (word1:tfidf1, word2:tfidf2, ....)
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            //距离最小值
            double minDistance = 0.0;
            //目标聚类编号
            String targetCluster = "";

            int count  = 0;
            for (Map.Entry<String, String> vec : this.vector.entrySet()) {
                //聚类编号
                String clusterID = vec.getKey();
                //聚类中心向量
                String vec1 = vec.getValue();
                String vec2 = value.toString().split("\t")[1];

                double distance = calDistance(vec1, vec2);

                if (count == 0) {
                    minDistance = distance;
                    targetCluster = clusterID;
                }else {
                    if (distance < minDistance) {
                        minDistance = distance;
                        targetCluster = clusterID;
                    }
                }

                count++;
            }

            //key : 目标聚类
            //value : 文档编号 + \t + 文档向量
            context.write(new Text(targetCluster), new Text(key.toString() + "\t" +
                    value.toString().split("\t")[1]));
        }

        /**
         * 计算两个向量的距离
         * @param vector1 向量1 word1:tfidf1,word2:tfidf2....
         * @param vector2 向量2
         * @return
         */
        private double calDistance(String vector1, String vector2) {
            String[] vec1 = vector1.split(",");
            String[] vec2 = vector2.split(",");

            Map<String, Double> word1 = new HashMap<String, Double>();
            Map<String, Double> word2 = new HashMap<String, Double>();

            for (String word : vec1) {
                word1.put(word.split(":")[0],
                        Double.valueOf(word.split(":")[1]));
            }
            for (String word : vec2) {
                word2.put(word.split(":")[0],
                        Double.valueOf(word.split(":")[1]));
            }

            Double a = 1.0;
            for (Map.Entry<String, Double> entry : word1.entrySet()) {
                if (word2.containsKey(entry.getKey())) {
                    Double a1 = word2.get(entry.getKey());
                    Double a2 = entry.getValue();

                    a += (a1 - a2) * (a1 - a2);
                }
            }

            double dis = Math.sqrt(a);

            return dis;
        }
    }

    public static class KMeansClusterReducer extends Reducer<Text, Text, Text, Text> {
        /**
         * 输出文本新的聚类编号
         * @param key 聚类编号
         * @param values 文档编号 + \t + 文档向量
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //输出新的聚类向量
            for (Text val : values) {
                //key : 文档编号
                //value : 聚类编号 + \t + 文档向量
                context.write(
                        new Text(val.toString().split("\t")[0]),
                        new Text(key.toString() + "\t" + val.toString().split("\t")[1]));
            }
        }
    }

    /**
     *
     * @param args
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("default.vector.path", args[2]);
        Job job = Job.getInstance(conf, "KMeansCluster");

        job.setJarByClass(KMeansCluster.class);
        job.setMapperClass(KMeansClusterMapper.class);
        job.setReducerClass(KMeansClusterReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        //序列化文件输入和输出
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static int start(String[] args) throws Exception {
        if (args.length != 3) {
            System.exit(1);
        }

        int res = new KMeansCluster().run(args);
        return res;
    }

    /**
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.exit(1);
        }

        int res = ToolRunner.run(new Configuration(),
                new KMeansCluster(), args);
        System.out.println(res);
    }
}


