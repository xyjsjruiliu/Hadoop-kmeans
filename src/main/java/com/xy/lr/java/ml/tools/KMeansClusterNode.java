package com.xy.lr.java.ml.tools;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by hadoop on 12/13/16.
 */
public class KMeansClusterNode {
    //节点编号
    private int clusterNumber;

    //节点属性
    private double[] properties;

    //聚类中其他节点
    private List<KMeansClusterNode> kMeansClusterNodes;

    /**
     * 默认构造函数
     */
    public KMeansClusterNode() {
        this.clusterNumber = -1;
        this.properties = new double[]{-1};
        this.kMeansClusterNodes = null;
    }

    /**
     * 构造函数
     * @param clusterNumber 节点编号
     */
    public KMeansClusterNode(int clusterNumber) {
        this.clusterNumber = clusterNumber;
    }

    /**
     * 构造函数
     * @param clusterNumber 节点编号
     * @param properties 属性
     */
    public KMeansClusterNode(int clusterNumber, double[] properties) {
        this.clusterNumber = clusterNumber;
        this.properties = properties;
    }

    /**
     * 设置节点编号
     * @param clusterNumber 编号
     */
    public void setClusterNumber(int clusterNumber) {
        this.clusterNumber = clusterNumber;
    }

    /**
     * 设置节点属性
     * @param properties 属性
     */
    public void setProperties(double[] properties) {
        this.properties = properties;
    }

    /**
     * 添加节点
     * @param kMeansCluster 新的节点
     * @return 插入是否正确
     */
    public boolean addCluster(KMeansClusterNode kMeansCluster) {
        if (this.kMeansClusterNodes == null) {
            if (this == kMeansCluster) {
                return false;
            } else {
                this.kMeansClusterNodes = new LinkedList<KMeansClusterNode>();
                this.kMeansClusterNodes.add(kMeansCluster);
            }
        } else {
            //如果本身存在，则不插入
            if (this.kMeansClusterNodes.contains(kMeansCluster) || this == kMeansCluster) {
                return false;
            }else {
                this.kMeansClusterNodes.add(kMeansCluster);
            }
        }
        return true;
    }

    /**
     * 节点编号
     * @return 编号
     */
    public int getClusterNumber() {
        return this.clusterNumber;
    }

    /**
     * 节点属性
     * @return 属性
     */
    public double[] getProperties() {
        return this.properties;
    }

    /**
     *
     * @return
     */
    public List<KMeansClusterNode> getkMeansClusters () {
        return this.kMeansClusterNodes;
    }

    public static void main(String[] args) {
        KMeansClusterNode kMeansCluster = new KMeansClusterNode();
        KMeansClusterNode kMeansCluster1 = new KMeansClusterNode();
        KMeansClusterNode kMeansCluster2 = new KMeansClusterNode();
        KMeansClusterNode kMeansCluster3 = new KMeansClusterNode();

        kMeansCluster.setProperties(new double[]{1,2,3,4});

        System.out.println(kMeansCluster.getClusterNumber());
        for (int i = 0;i < kMeansCluster.properties.length;i++) {
            System.out.println(kMeansCluster.getProperties()[i]);
        }

        kMeansCluster.addCluster(kMeansCluster);
        kMeansCluster.addCluster(kMeansCluster1);
        kMeansCluster.addCluster(kMeansCluster2);
        kMeansCluster.addCluster(kMeansCluster3);

        if (kMeansCluster.getkMeansClusters() == null) {
            System.err.println("null");
        } else {
            System.out.println(kMeansCluster.getkMeansClusters().size());
        }
    }
}
