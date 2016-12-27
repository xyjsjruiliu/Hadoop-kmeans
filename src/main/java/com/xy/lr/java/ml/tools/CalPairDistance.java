package com.xy.lr.java.ml.tools;

/**
 * 计算节点距离
 * Created by hadoop on 12/13/16.
 */
public interface CalPairDistance {
    /**
     * 计算两者之间的距离
     * @param kMeansCluster 节点1
     * @param otherCluster 节点2
     * @return 距离
     */
    public double calDistance(KMeansClusterNode kMeansCluster, KMeansClusterNode otherCluster);
}
