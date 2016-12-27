package com.xy.lr.java.ml.tools;

/**
 * Created by hadoop on 12/13/16.
 */
public class CosinePairDistance implements CalPairDistance {
    /**
     * cosine pair distance
     * @param kMeansCluster 节点1
     * @param otherCluster 节点2
     * @return 距离
     */
    public double calDistance(KMeansClusterNode kMeansCluster, KMeansClusterNode otherCluster) {
        double[] cluster1 = kMeansCluster.getProperties();
        double[] cluster2 = otherCluster.getProperties();
        double distance = 0.0;

        for (int i = 0;i < cluster1.length;i++) {
            for (int j = 0;j < cluster2.length;j++) {
                distance += cluster1[i] * cluster2[j];
            }
        }
        return Math.sqrt(distance);
    }
}
