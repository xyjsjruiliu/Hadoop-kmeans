package com.xy.lr.java.ml.tfidf.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * map reduce tf-idf all steps
 * Created by hadoop on 12/13/16.
 */
public class TFIDF {
    private static final String wordFrequenceInDocument = "/wordFrequenceInDocument";
    private static final String wordCountsInDocument = "/wordCountsInDocument";

    public static void main(String[] args) throws Exception {
        if(args.length != 5) {
            System.err.println("Usage : hadoop jar hadoop-tfidf.jar " +
                    "hbase-table /outputPath documentCount dictionary_outputPath number");
            System.exit(1);
        }

        String[] argsFrequence = new String[]{args[0], wordFrequenceInDocument};
        String[] argsFrequenceHBase = new String[]{args[0], wordFrequenceInDocument, args[4]};
        String[] argsCounts = new String[]{wordFrequenceInDocument, wordCountsInDocument};
        String[] argsTFIDF = new String[]{wordCountsInDocument, args[1], args[2], args[3]};

        WordFrequenceInDocumentHBase.start(argsFrequenceHBase);
//        WordFrequenceInDocument.start(argsFrequence);
        WordCountsInDocuments.start(argsCounts);
        WordsInCorpusTFIDF.start(argsTFIDF);
    }
}