import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class SparkTest {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkFileSumApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile("C:\\Users\\nagar\\Desktop\\BigData\\Hbase-Notes.txt");
        List<String> output = input.collect();
        System.out.println(output);
    }
}
