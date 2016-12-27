package com.xy.lr.java.ml.tfidf.mapred;

import com.xy.lr.java.ml.tools.SequenceFileBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.util.*;

/**
 * 向量生成
 * Created by hadoop on 12/15/16.
 */
public class DocumentVectorBuilder extends Configured implements Tool {
    public static class DocumentVectorMapper extends
            Mapper<Text, Text, Text, Text> {
        //字典
        private List<String> dictionary = new ArrayList<String>();

        /**
         * 读取词典
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            Configuration configuration = context.getConfiguration();
            String dictPath = configuration.get("dictionary.path");

            //读取字典
//            dictionary = SequenceFileBuilder.readDictionarySequenceFile(dictPath, configuration);
            super.setup(context);
        }

        /**
         *
         * @param key 单词 + \t + 文档编号
         * @param value 单词的tfidf值
         * @param context
         */
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String word = key.toString().split("\t")[0];
            String documentID = key.toString().split("\t")[1];
            String tfidf = value.toString();

            //输出
            //key : 文档编号
            //value : 单词 + : + tfidf值
            context.write(new Text(documentID), new Text(word + ":" + tfidf));
            /*for (String w : dictionary) {
                if (w.equals(word)) {
                    //key : 文档编号
                    //value : tfidf值
                    context.write(new Text(documentID), new Text(tfidf));
                } else {
                    context.write(new Text(documentID), new Text("0.001"));
                }
            }*/
        }
    }

    public static class DocumentVectorReducer
            extends Reducer<Text, Text, Text, Text> {
        //向量
        private static Map<Integer, String> default_vector = new HashMap<Integer, String>();

        protected void setup(Context context) {

        }

        /**
         * 输出向量
         * @param key 文档编号
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String vec = "";
            for (Text vector : values) {
                vec += vector.toString() + ",";
            }

            System.out.println(vec);

            if (!vec.equals("")) {
                default_vector.put(Integer.valueOf(key.toString()),
                        vec.substring(0, vec.length() - 1));

                //key : 文档ID
                //value : 初始聚类中心 + \t + 向量
                context.write(key, new Text( key.toString() +
                        "\t" + vec.substring(0, vec.length() - 1)));
            }
        }

        /**
         * 初始化聚类中心
         * @param context asd
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            Configuration configuration = context.getConfiguration();
            //初始化向量个数
            int number = Integer.valueOf(configuration.get("default.cluster.number"));
            //聚类中心输出路径
            Path path = new Path(configuration.get("default.cluster.path"));

            System.out.println("-----------------------------------");
            System.out.println("初始化向量的个数：" + number);
            System.out.println("聚类中心输出路径：" + configuration.get("default.cluster.path"));
            System.out.println("-----------------------------------");

            FileSystem fs = FileSystem.get(configuration);
            fs.delete(path, true);
            final SequenceFile.Writer out = SequenceFile.createWriter(fs, configuration,
                    path, Text.class, Text.class);

            //已经选择的编号
            List<Integer> order = new ArrayList<Integer>();
            //key序列
            Integer[] keys = default_vector.keySet().toArray(new Integer[0]);

//            System.out.println(keys.length);
            //随机数发生器
            Random random = new Random();
            //最终值
//            Map<Integer, String> result = new HashMap<Integer, String>();
            while (true) {
                if (order.size() == number)
                    break;
                //这句话报错，原因是default_vector的长度为0
                Integer randomKey = keys[random.nextInt(keys.length)];
                if (order.contains(randomKey))
                    continue;
                order.add(randomKey);
                String randomValue = default_vector.get(randomKey);

//                result.put(randomKey, randomValue);
                out.append(new Text(randomKey.toString()), new Text(randomValue));
            }

            out.close();
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //词典路径
        conf.set("dictionary.path", args[2]);
        //初始聚类中心个数
        conf.set("default.cluster.number", args[3]);
        //初始聚类中心存放地址
        conf.set("default.cluster.path", args[4]);

        Job job = Job.getInstance(conf, "Document Vector Build");

        job.setJarByClass(DocumentVectorBuilder.class);
        job.setMapperClass(DocumentVectorMapper.class);
        job.setReducerClass(DocumentVectorReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置 Reduce 数量
        job.setNumReduceTasks(1);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static int start(String[] args) throws Exception {
        return ToolRunner.run(
                new Configuration(), new DocumentVectorBuilder(), args);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DocumentVectorBuilder(),
                args);
        System.exit(res);
    }
}