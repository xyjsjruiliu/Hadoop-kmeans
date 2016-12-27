package com.xy.lr.java.ml.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Created by hadoop on 12/15/16.
 */
public class SequenceFileBuilder {
    /**
     *
     */
    public static class UserWritable implements Writable,Comparable{
        private long userId;
        private String userName;
        private int userAge;

        public long getUserId() {
            return userId;
        }

        public void setUserId(long userId) {
            this.userId = userId;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public int getUserAge() {
            return userAge;
        }

        public void setUserAge(int userAge) {
            this.userAge = userAge;
        }

        public UserWritable(long userId, String userName, int userAge) {
            super();
            this.userId = userId;
            this.userName = userName;
            this.userAge = userAge;
        }

        public UserWritable() {
            super();
        }

        public void write(DataOutput out) throws IOException {
            out.writeLong(this.userId);
            out.writeUTF(this.userName);
            out.writeInt(this.userAge);
        }

        public void readFields(DataInput in) throws IOException {
            this.userId=in.readLong();
            this.userName=in.readUTF();
            this.userAge=in.readInt();
        }

        @Override
        public String toString() {
            return this.userId+"\t"+this.userName+"\t"+this.userAge;
        }

        /**
         * 只对比userId
         */
        @Override
        public boolean equals(Object obj) {
            if(obj instanceof UserWritable){
                UserWritable u1=(UserWritable)obj;
                return u1.getUserId()==this.getUserId();
            }
            return false;
        }

        /**
         * 只对比userId
         */
        public int compareTo(Object obj) {
            int result=-1;
            if(obj instanceof UserWritable){
                UserWritable u1=(UserWritable)obj;
                if(this.userId>u1.userId){
                    result=1;
                }else if(this.userId==u1.userId){
                    result=1;
                }
            }
            return result;
        }

        @Override
        public int hashCode() {
            return (int)this.userId&Integer.MAX_VALUE;
        }
    }

    public static void writeMapSequenceFile(String filePath,
                                            Configuration conf, Map<String, String> map) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);
        fs.delete(path, true);
        final SequenceFile.Writer out = SequenceFile.createWriter(fs, conf,
                path, Text.class, Text.class);

        int i = 0;

        for (Map.Entry<String, String> entry : map.entrySet()) {
            out.append(new Text(entry.getKey()), new Text(entry.getValue()));
        }
        out.close();
    }

    /**
     * 写入到sequence file
     *
     * @param filePath
     * @param conf
     * @param datas
     */
    public static void write2SequenceFile(String filePath,
                                          Configuration conf, Collection<UserWritable> datas){
        FileSystem fs = null;
        SequenceFile.Writer writer=null;
        Path path = null;
        LongWritable idKey=new LongWritable(0);

        try {
            fs=FileSystem.get(conf);
            path=new Path(filePath);
            writer=SequenceFile.createWriter(fs, conf, path,
                    LongWritable.class, UserWritable.class);

            for(UserWritable user : datas){
                idKey.set(user.getUserId());  // userID为Key
                writer.append(idKey, user);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            IOUtils.closeStream(writer);
        }
    }

    /**
     * 从sequence file文件中读取数据
     *
     * @param sequeceFilePath
     * @param conf
     * @return
     */
    public static List<UserWritable> readSequenceFile(String sequeceFilePath,
                                                      Configuration conf){
        List<UserWritable> result = null;
        FileSystem fs = null;
        SequenceFile.Reader reader=null;
        Path path = null;
        Writable key = null;
        UserWritable value = new UserWritable();

        try {
            fs = FileSystem.get(conf);
            result = new ArrayList<UserWritable>();
            path = new Path(sequeceFilePath);
            reader = new SequenceFile.Reader(fs, path, conf);
            key = (Writable)
                    ReflectionUtils.newInstance(reader.getKeyClass(), conf); // 获得Key，也就是之前写入的userId
            while(reader.next(key, value)){
                result.add(value);
                value = new UserWritable();
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }finally{
            IOUtils.closeStream(reader);
        }
        return result;
    }

    /**
     * 显示SequenceFile文件里面的内容
     * @param vectorPath 路径
     * @param configuration
     * @return
     */
    public static Map<String, String> printVectorSequenceFile(
            String vectorPath, Configuration configuration) {
        Map<String, String> map = new HashMap<String, String>();
        FileSystem fs = null;
        SequenceFile.Reader reader = null;
        Path path = null;

        try {
            fs = FileSystem.get(configuration);
            path = new Path(vectorPath);
            reader = new SequenceFile.Reader(fs, path, configuration);
            Text key = ReflectionUtils.newInstance(Text.class, configuration); // 获得Key，也就是之前写入的userId
            Text value = new Text();
            while(reader.next(key, value)){
//                map.put(key.toString(), value.toString());
//                System.out.println(key.toString() + "\t" + value.toString());
                map.put(key.toString(), value.toString());
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }finally{
            IOUtils.closeStream(reader);
        }

        return map;
    }

    public static Map<Integer, String> readIndexSequenceFile(
            String vectorPath, Configuration configuration) {
        Map<Integer, String> map = new HashMap<Integer, String>();
        FileSystem fs = null;
        SequenceFile.Reader reader=null;
        Path path = null;

        try {
            fs = FileSystem.get(configuration);
            path = new Path(vectorPath);
            reader = new SequenceFile.Reader(fs, path, configuration);
            IntWritable key = ReflectionUtils.newInstance(IntWritable.class,
                    configuration); // 获得Key，也就是之前写入的userId
            Text value = new Text();
            while(reader.next(key, value)){
                map.put(Integer.valueOf(key.toString()), value.toString());
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }finally{
            IOUtils.closeStream(reader);
        }
        return map;
    }

    public static Map<String, String> readDefaultVectorSequenceFile(
            String vectorPath, Configuration configuration) {
        Map<String, String> map = new HashMap<String, String>();
        FileSystem fs = null;
        SequenceFile.Reader reader=null;
        Path path = null;

        try {
            fs = FileSystem.get(configuration);
            path = new Path(vectorPath);
            reader = new SequenceFile.Reader(fs, path, configuration);
            Text key = ReflectionUtils.newInstance(Text.class, configuration); // 获得Key，也就是之前写入的userId
            Text value = new Text();
            while(reader.next(key, value)){
                map.put(key.toString(), value.toString());
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }finally{
            IOUtils.closeStream(reader);
        }
        return map;
    }

    /**
     *
     * @param dictPath
     * @param configuration
     * @return
     */
    public static List<String> readDictionarySequenceFile(String dictPath, Configuration configuration) {
        List<String> dictionary = new ArrayList<String>();
        FileSystem fs = null;
        SequenceFile.Reader reader=null;
        Path path = null;

        try {
            fs = FileSystem.get(configuration);
            path = new Path(dictPath);
            reader = new SequenceFile.Reader(fs, path, configuration);
            Text key = ReflectionUtils.newInstance(Text.class, configuration); // 获得Key，也就是之前写入的userId
            LongWritable value = new LongWritable();
            while(reader.next(key, value)){
                dictionary.add(key.toString());
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }finally{
            IOUtils.closeStream(reader);
        }

        return dictionary;
    }

    private  static Configuration getDefaultConf(){
        Configuration conf = new Configuration();
//        conf.set("mapred.job.tracker", "local");
        conf.set("fs.default.name", "hdfs://hadoop-server:9000");
        //conf.set("io.compression.codecs", "com.hadoop.compression.lzo.LzoCodec");
        return conf;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        String filePath = "data/user.sequence"; // 文件路径
        Set<UserWritable> users = new HashSet<UserWritable>();
        UserWritable user = null;
        // 生成数据
        for(int i=1;i<=10;i++){
            user = new UserWritable(
                    i+(int)(Math.random()*100000),
                    "name-"+(i+1),
                    (int)(Math.random()*50)+10);
            users.add(user);
        }
        // 写入到sequence file
        write2SequenceFile(filePath, getDefaultConf(), users);
        //从sequence file中读取
        List<UserWritable> readDatas = readSequenceFile(filePath, getDefaultConf());

        readDictionarySequenceFile("/dictionary.txt", getDefaultConf());
        // 对比数据是否正确并输出
        for(UserWritable u : readDatas){
            if(users.contains(u)){
                System.out.println(u.toString());
            }else{
                System.err.println("Error data:"+u.toString());
            }
        }
    }
}
