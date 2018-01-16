package com.rama.bigdata;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class FilterExample {
    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("JavaWordCount")
                .getOrCreate();

        JavaSparkContext sc =  new JavaSparkContext(spark.sparkContext());

        //Filter Predicate
        Function<Integer, Boolean> filterPredicate = e -> e % 2 == 0;

        // Parallelized with 2 partitions
        JavaRDD<Integer> rddX = sc.parallelize(
                Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
                2);

        // filter operation will return List of Array in following case
        JavaRDD<Integer> rddY = rddX.filter(filterPredicate);
        List<Integer> filteredList = rddY.collect();

        System.out.println(filteredList.size());

    }
}