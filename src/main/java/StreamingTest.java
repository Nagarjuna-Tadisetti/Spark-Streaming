import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class StreamingTest {
    public static void main(String[] args) throws StreamingQueryException {

        SparkConf conf= new SparkConf().setAppName("Java-SparkStreaming").setMaster("local[*]");

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test")
                .load();

        StreamingQuery query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .writeStream()
                .outputMode("append")
                .format("console")
                .start();

        query.awaitTermination();
    }
}
