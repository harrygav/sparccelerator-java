package com.sparccelerator;/* SimpleApp.java */

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

public class SimpleApp {
    static native int testC();

    static {
        File lib = new File(System.mapLibraryName("simplec"));
        System.load(lib.getAbsolutePath());

    }


    public static void main(String[] args) {


        String logFile = "/home/harry/spark-2.2.1-bin-hadoop2.7/README.md"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();

        List<Integer> intList = new LinkedList<>();
        intList.add(5);
        intList.add(3);
        //DataSet<Integer>  intDataSet= spark.createDataset(intList.iterator(),Encoders.INT());

        Dataset<String> logData = spark.read().textFile(logFile).cache();

        /*long numAs = logData.filter(i -> {
            int x = 2;
            int y = testC();
            return x==y;
        }).count();*/

        Dataset<String> numBs = logData.mapPartitions((MapPartitionsFunction<String, String>) it ->  {


            List<String> ls = new LinkedList<>();
            ls.add(testC()+"");
            ls.add(testC()+"");
            ls.add(testC()+2+"");

            return ls.iterator();
        }, Encoders.STRING());


        System.out.println(numBs.collectAsList().toString());
        //System.out.println("Lines with a: " + numBs );


        spark.stop();
    }
}
