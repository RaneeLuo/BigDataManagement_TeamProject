import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class Q6MostPlayedSongFinder {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Q6MostPlayedSongFinder")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        String path = "C:\\Users\\Calan\\Documents\\CodeProjects\\2AMD15 Big Data Management\\Team_Project\\plays.csv";

        Dataset<String> rawLines = spark.read().textFile(path);

        JavaRDD<Row> parsed = rawLines.javaRDD().map(line -> {
            String[] parts = line.split(",");
            int userId = Integer.parseInt(parts[0]);
            int songId = Integer.parseInt(parts[1]);
            Integer rating = parts.length == 3 ? Integer.parseInt(parts[2]) : null;
            return RowFactory.create(userId, songId, rating);
        });

        StructType schema = new StructType()
                .add("userid", "int", false)
                .add("songid", "int", false)
                .add("rating", "int", true);

        Dataset<Row> df = spark.createDataFrame(parsed, schema);
        df.cache();

        Dataset<Row> songCounts = df.groupBy("songid").agg(count("*").alias("play_count"));
        long maxCount = songCounts.agg(max("play_count")).first().getLong(0);
        Dataset<Row> mostPlayed = songCounts.filter(col("play_count").equalTo(maxCount));

        mostPlayed.show();

        spark.stop();
    }
}
