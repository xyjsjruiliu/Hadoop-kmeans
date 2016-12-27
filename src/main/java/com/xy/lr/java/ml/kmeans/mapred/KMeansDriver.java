package com.xy.lr.java.ml.kmeans.mapred;

/**
 * Created by hadoop on 12/22/16.
 */
public class KMeansDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("Usage : " +
                    "hadoop jar hadoop-newKmeansCluster.jar " +
                    "/kmeansInput /finalOutput /default_vector iter");
            System.exit(1);
        }

        int iter = Integer.valueOf(args[3]);

        String[] kmeansClusterArgs = new String[]{args[0], "/tempKmeans", args[2]};
        String[] kmeansNewClusterArgs = new String[]{"/tempKmeans", "/newCluster_Vector1"};

        for (int i = 1;i <= iter;i++){
            if (i == iter) {
                kmeansClusterArgs[1] = args[1];
                kmeansNewClusterArgs[0] = kmeansClusterArgs[1];
            } else {
                //
                kmeansClusterArgs[1] = "/tempKmeans" + i;
                kmeansNewClusterArgs[0] = kmeansClusterArgs[1];
            }

            System.out.println("第" + i + "次聚类开始，下面是配置文件: ");
            System.out.println(kmeansClusterArgs[0] + "," + kmeansClusterArgs[1] + "," + kmeansClusterArgs[2]);
            System.out.println(kmeansNewClusterArgs[0] + "," + kmeansNewClusterArgs[1]);

            //给每个文档计算聚类编号
            KMeansCluster.start(kmeansClusterArgs);
            //计算新的聚类中心向量
            KmeansNewCluster.start(kmeansNewClusterArgs);

            //新聚类中型地址
            kmeansClusterArgs[2] = "/newCluster_Vector" + i;
            kmeansNewClusterArgs[1] = "/newCluster_Vector" + (i + 1);
            kmeansClusterArgs[0] = kmeansNewClusterArgs[0];
        }
    }
}
