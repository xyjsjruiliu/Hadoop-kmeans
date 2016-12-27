package com.xy.lr.java.ml.mutilate;

import com.xy.lr.java.tools.file.JFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 生成聚类标题
 * Created by hadoop on 12/23/16.
 */
public class ClusterName {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void init(){
        configuration = HBaseConfiguration.create();
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭连接
     */
    public static  void close(){
        try {
            if(null != admin)
                admin.close();
            if(null != connection)
                connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws IOException {
        init();

        Table table = connection.getTable(TableName.valueOf("Paper"));
        Scan scan = new Scan();

        Map<String, Integer>  map = new HashMap<String, Integer>();

        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result : resultScanner){
            String documentID = "";
            String clusterID = "";
            String keyword = "";

            Cell[] cells = result.rawCells();
            for(Cell cell:cells){
                documentID = new String(CellUtil.cloneRow(cell));
                if (new String(CellUtil.cloneQualifier(cell)).equals("newCluster")) {
                    clusterID = new String(CellUtil.cloneValue(cell));
                }
                if (new String(CellUtil.cloneQualifier(cell)).equals("keywords")) {
                    keyword = new String(CellUtil.cloneValue(cell));
                }
            }

            for (String word : keyword.split(",")) {
//                JFile.appendFile("clusterName.txt", clusterID + "\t" + word);

                if (map.containsKey(clusterID+","+word)) {
                    int a = map.get(clusterID + "," + word);
                    map.put(clusterID+","+word, a+1);
                }else {
                    map.put(clusterID + "," + word, 1);
                }
            }

//            map.put(clusterID, keyword);
        }

        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            if (entry.getValue() > Integer.valueOf(args[0])) {
                JFile.appendFile("clusterName.txt", entry.getKey() + "," + entry.getValue());
            }

        }

        table.close();
    }
}