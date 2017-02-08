/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.simplejavaspark;

/**
 * * SimpleApp.java **
 */
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;

public class SimpleApp {

    public static void main(String[] args) {
        String logFile = "/spark-1.4.0-hadoop2.6/README.md"; // Should be some file on your system
        JavaSparkContext sc = new JavaSparkContext("local", "Simple App",
                "/spark-1.4.0-hadoop2.6",
                new String[]{"target/SimpleJavaSpark-1.0-SNAPSHOT.jar"});
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    
        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("a");
            }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                System.out.println("b       :"+s);
                return s.contains("b");
            }
        }).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    }
}
