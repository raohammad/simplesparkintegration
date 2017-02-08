/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.simplejavaspark;

import java.util.ArrayList;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 *
 * @author hammadakhan
 */
public class RDDforCollections {
    
    public static void main(String[] args){
        
        ArrayList data = new ArrayList();
        data.add(1);data.add(2);
        data.add(3);data.add(4);data.add(5);        
        
        //String logFile = "/Users/hammadakhan/Documents/development/BigData/tools/spark-1.4.0-hadoop2.6/README.md"; // Should be some file on your system
        JavaSparkContext sc = new JavaSparkContext("local", "Simple App",
                "/Users/hammadakhan/Documents/development/BigData/tools/spark-1.4.0-hadoop2.6",
                new String[]{"target/SimpleJavaSpark-1.0-SNAPSHOT.jar"});
        //JavaRDD<String> logData = sc.textFile(logFile).cache();
        JavaRDD<Integer> rDD = sc.parallelize(data, 3);
        
        //map(function) => returns a new RDD (each element of source RDD passed through function)
        //filter(funtoin) => returns new RDD (for which function returns 0)
        //flatmap(function) => return a sequence against each RDD (input element can be returned as zero or more elements) returns a new RDD
        //distinct(function) => removes duplicates and returns new RDD
        
        JavaRDD<Integer> doubled = rDD.map(new Function<Integer, Integer>(){

            @Override
            public Integer call(Integer t) throws Exception {
                return t*2;
            }
        });
        
        JavaRDD<Integer> filtered = rDD.filter(new Function<Integer, Boolean>(){

            @Override
            public Boolean call(Integer t) throws Exception {
                return t%2==0;
            }
        });
        
        //ACTIONS 
        // reduce(func): aggregates a dataset elements to reduce it take two arhuments and return 1
        // take(n): returns an array of first n elements (from RDD)
        // collection() : returns all elements as array of RDD
        // takeordered (n, (opt)keyfunc): returns n elements ordered in asc as specified by optional key function
        
        //Cache
        // RDD can be cached - rDD.cache()
        
        //KeyValue RDDs
        //similar to MapyReduce
        //reduceByKey(func): returns a new KV RDD where value of each K are aggregated through function provided
        //sortByKey(): returns sorted by Key in ascending order
        //groupByKey(): returns a new RDD with (K, Iterable<V>) pairs
    }
    
}
