package sdia.bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class exo1 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("tp 1 RDD").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> rdd1=sc.parallelize(Arrays.asList("Haytam","Asmae","Anwar","rochdi","karima","Achraf","Nour","Malika","Mojtaba"));
        JavaRDD<String> rdd2 = rdd1.flatMap(word -> Arrays.asList(word, word).iterator());
        rdd2.persist(StorageLevel.MEMORY_ONLY());
        JavaRDD<String> rdd3 = rdd2.filter(w-> w.length()>5);
        JavaRDD<String> rdd4 = rdd2.filter(w-> w.startsWith("A"));
        JavaRDD<String> rdd5 = rdd2.filter(w-> w.contains("m"));
        JavaRDD<String> rdd6=rdd3.union(rdd4);
        JavaPairRDD<String, Integer> rdd71 = rdd5.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> rdd81 = rdd6.mapToPair(word -> new Tuple2<>(word, 2));
        JavaPairRDD<String,Integer> rdd7=rdd71.reduceByKey(Integer::sum);
        JavaPairRDD<String,Integer> rdd8=rdd71.reduceByKey((v1,v2)->v1-v2);
        JavaPairRDD<String ,Integer> rdd9=rdd7.union(rdd8);
        JavaPairRDD<String ,Integer> rdd10=rdd9.sortByKey();


        // Collect the result and print
        List<Tuple2<String, Integer>> result = rdd10.collect();
        result.forEach(System.out::println);
        sc.stop();
    }
}
