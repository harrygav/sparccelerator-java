package com.sparccelerator;/* SimpleApp.java */

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import scala.Tuple5;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class CSVInput {

    public static void main(String[] args) {


        String tupleInput = "/home/harry/Desktop/semester11/thesis/arrow/tuple5.csv"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();


        Encoder<Tuple5<Integer, Integer, Integer, Integer, Integer>> encoder =
                Encoders.tuple(Encoders.INT(), Encoders.INT(), Encoders.INT(), Encoders.INT(), Encoders.INT());


        Dataset<Tuple5<Integer, Integer, Integer, Integer, Integer>> tuples = spark.read().text(tupleInput).map(
                (MapFunction<Row, Tuple5<Integer, Integer, Integer, Integer, Integer>>) (Row i)-> {
            String[] values = i.get(0).toString().split(",");
            Integer val1 = Integer.parseInt(values[0]);
            Integer val2 = Integer.parseInt(values[1]);
            Integer val3 = Integer.parseInt(values[2]);
            Integer val4 = Integer.parseInt(values[3]);
            Integer val5 = Integer.parseInt(values[4]);

            return new Tuple5<Integer, Integer, Integer, Integer, Integer>(val1, val2, val3, val4, val5);
        }, encoder);



        Dataset<Integer> numBs = tuples.mapPartitions((MapPartitionsFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Integer>) it -> {

            //DirectBuffer byteBuffer = (DirectBuffer) ByteBuffer.allocateDirect((4 * 4) * 5).order(ByteOrder.nativeOrder());

            ByteBuffer testBuffer = ByteBuffer.allocateDirect((4 * 5) * 6).order(ByteOrder.nativeOrder());

            int sum = 0;
            while (it.hasNext()) {


                Tuple5 curTuple = it.next();

                sum += (int) curTuple._1();
                sum += (int) curTuple._2();

            }


            List<Integer> ls = new LinkedList<>();

            ls.add(sum);

            return ls.iterator();
        }, Encoders.INT());


        int sum = numBs.reduce((a, b) -> a + b);
        System.out.println("SUM: " + sum);

        System.out.println(numBs.collectAsList().toString());


        spark.stop();
    }
}
