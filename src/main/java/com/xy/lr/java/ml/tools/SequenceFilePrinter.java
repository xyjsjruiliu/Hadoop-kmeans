package com.xy.lr.java.ml.tools;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * Created by hadoop on 12/18/16.
 */
public class SequenceFilePrinter {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage : java -jar hadoop-sequenceFilePrinter.jar type file limit\n" +
                    "type(vector, normal)");
            System.exit(1);
        }

        if (args[0].equals("vector")) {
            printVector(args);
        } else if(args[0].equals("normal")) {
            Configuration configuration = new Configuration();

            Map<String, String> map = SequenceFileBuilder.printVectorSequenceFile(args[1], configuration);

            int i = 0;
            for(Map.Entry<String, String> entry : map.entrySet()) {
                System.out.println(entry.getKey() + "," + entry.getValue());
                i++;
                if(i >= Integer.valueOf(args[2])) {
                    break;
                }
            }
        } else if(args[0].equals("index")) {
            Configuration configuration = new Configuration();

            Map<Integer, String> map = SequenceFileBuilder.readIndexSequenceFile(args[1], configuration);
            int i = 0;
            for(Map.Entry<Integer, String> entry : map.entrySet()) {
                System.out.println(entry.getKey() + "," + entry.getValue());
                i++;
                if(i >= Integer.valueOf(args[2])) {
                    break;
                }
            }
        }
        else {
            System.out.println("type(vector, normal)");
        }
    }

    private static void printVector(String[] args) {
        Configuration configuration = new Configuration();

        Map<String, String> map = SequenceFileBuilder.printVectorSequenceFile(args[1], configuration);

        for(Map.Entry<String, String> entry : map.entrySet()) {
            String val = entry.getValue();

            System.out.print(entry.getKey() + "\t");

            for (String vec : val.split(",")) {
                String word = vec.split(":")[0];
                String tfidf = vec.split(":")[1];

                if (Double.valueOf(tfidf) > Double.valueOf(args[2])) {
                    System.out.print(word + ":" + tfidf + ",");
                }
            }
            System.out.println();
        }
    }
}
