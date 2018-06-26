package com.sparccelerator;/* SimpleApp.java */

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple5;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class CustomMapPart {
    static native int testC(ByteBuffer buf1, int buf1Off, int buf1Len);

    static {
        File lib = new File(System.mapLibraryName("simplec"));
        System.load(lib.getAbsolutePath());

    }


    public static void main(String[] args) {


        String logFile = "/home/harry/spark-2.2.1-bin-hadoop2.7/README.md"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();


        List<Tuple5<Integer, Integer, Integer, Integer, Integer>> tuples =
                Arrays.asList(
                        new Tuple5<>(1, 1, 1, 1, 1),
                        new Tuple5<>(2, 2, 2, 2, 2),
                        new Tuple5<>(3, 3, 3, 3, 3),
                        new Tuple5<>(4, 4, 4, 4, 4),
                        new Tuple5<>(5, 5, 5, 5, 5)

                );

        Encoder<Tuple5<Integer, Integer, Integer, Integer, Integer>> encoder =
                Encoders.tuple(Encoders.INT(), Encoders.INT(), Encoders.INT(), Encoders.INT(), Encoders.INT());

        Dataset<Tuple5<Integer, Integer, Integer, Integer, Integer>> tupleDS =
                spark.createDataset(tuples, encoder);


        Dataset<Integer> numBs = tupleDS.mapPartitions((MapPartitionsFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Integer>) it -> {

            //DirectBuffer byteBuffer = (DirectBuffer) ByteBuffer.allocateDirect((4 * 4) * 5).order(ByteOrder.nativeOrder());

            ByteBuffer testBuffer = ByteBuffer.allocateDirect((4 * 5) * 6).order(ByteOrder.nativeOrder());

            while (it.hasNext()) {


                Tuple5 curTuple = it.next();

                testBuffer.putInt((int) curTuple._1());
                testBuffer.putInt((int) curTuple._2());
                testBuffer.putInt((int) curTuple._3());
                testBuffer.putInt((int) curTuple._4());
                testBuffer.putInt((int) curTuple._5());

            }

            int sum = testC(testBuffer, 0, 4);

            List<Integer> ls = new LinkedList<>();

            ls.add(sum);

            return ls.iterator();
        }, Encoders.INT());

        int sum = numBs.reduce((a,b) -> a+b);
        System.out.println("SUM: "+ sum);

        System.out.println(numBs.collectAsList().toString());
        //System.out.println("Lines with a: " + numBs );


        spark.stop();
    }
}
