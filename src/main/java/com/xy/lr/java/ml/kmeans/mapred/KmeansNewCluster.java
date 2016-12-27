package com.xy.lr.java.ml.kmeans.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
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
 * Created by hadoop on 12/22/16.
 */
public class KmeansNewCluster extends Configured implements Tool {
    public static class KmeansNewClusterMapper extends Mapper<Text, Text, Text, Text> {
        /**
         * key和value转换
         * @param key 文档编号
         * @param value 聚类编号 + \t + 文档向量
         * @param context
         */
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String documentID = key.toString();
            String clusterID = value.toString().split("\t")[0];
            String document_vector = value.toString().split("\t")[1];

            //key : 聚类编号
            //val : 文档编号 + \t + 文档向量
            context.write(new Text(clusterID),
                    new Text(documentID + "\t" + document_vector));
        }
    }

    public static class KmeansNewClusterReducer extends Reducer<Text, Text, Text, Text> {
        /**
         * 重新计算新的聚类中心
         * @param key 聚类编号
         * @param values 文档编号 + \t + 文档向量
         * @param context
         */
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //新聚类编号
            String newClusterID = key.toString();
            //新聚类向量
            String newClusterVector;

            Map<String, String> wordWithTFIDF = new HashMap<String, String>();
            //临时新聚类向量
            String new_vec = "";
            //遍历一个类别下的所有文档
            for (Text value : values) {
                //文档编号
//                String documentID = value.toString().split("\t")[0];
                //文但向量
                String document_vector = value.toString().split("\t")[1];

                //遍历一个文档下的所有单词
                for (String wordAndTFIDF : document_vector.split(",")) {
                    //单词
                    String word = wordAndTFIDF.split(":")[0];
                    //tfidf值
                    String tfidf = wordAndTFIDF.split(":")[1];

                    if (wordWithTFIDF.containsKey(word)) {
                        String va = wordWithTFIDF.get(word);
                        String sum_tfidf = va.split(",")[0];
                        String sum = va.split(",")[1];

                        //tfidf值总和
                        String new_sum_tfidf = String.valueOf(Double.valueOf(tfidf) +
                                Double.valueOf(sum_tfidf));
                        //tfidf个数
                        String new_sum = String.valueOf(Double.valueOf(sum) + 1);

                        //key : 单词
                        //val : 单词tfidf值总和 + , + 单词tfidf个数
                        wordWithTFIDF.put(word, new_sum_tfidf + "," + new_sum);
                    }else {
                        wordWithTFIDF.put(word, tfidf + "," + 1);
                    }
                }
            }

            //求每个单词的tfidf值平均
            for (Map.Entry<String, String> wwT : wordWithTFIDF.entrySet()) {
                String word = wwT.getKey();
                String num_tfidf = wwT.getValue();

                if (Double.valueOf(num_tfidf.split(",")[1]) == 1.0) {
                }else {
                    Double tfidf = Double.valueOf(num_tfidf.split(",")[0]) /
                            Double.valueOf(num_tfidf.split(",")[1]);

                    new_vec += word + ":" + tfidf + ",";
                }

            }
            newClusterVector = new_vec.substring(0, new_vec.length() - 1);

            //key : 新的聚类编号
            //value : 聚类向量
            context.write(new Text(key.toString()),
                    new Text(newClusterVector));
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
        Job job = Job.getInstance(conf, "KMeansNewCluster");

        /*FileSystem fs = FileSystem.get(conf);
        Path path = new Path(args[1]);
        //如果输出路径存在，删除
        if (fs.exists(path))
            fs.delete(path, true);*/

        job.setJarByClass(KmeansNewCluster.class);
        job.setMapperClass(KmeansNewClusterMapper.class);
        job.setReducerClass(KmeansNewClusterReducer.class);

        //输出key和value类型
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

    /**
     * 启动mapreduce
     * @param args
     * @return
     * @throws Exception
     */
    public static int start(String[] args) throws Exception {
        int res = new KmeansNewCluster().run(args);
        return res;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new KmeansNewCluster(), args);
        System.exit(res);
    }
}
