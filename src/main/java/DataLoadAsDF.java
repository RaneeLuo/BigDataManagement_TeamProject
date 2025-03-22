
import org.apache.spark.sql.*;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import org.apache.spark.sql.catalyst.encoders.RowEncoder;


public class DataLoadAsDF {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("YfitopS Load DataFrame")
                .master("local[*]")
                .getOrCreate();

        Dataset<String> rawLines = spark.read().textFile("C:\\Users\\Calan\\Documents\\CodeProjects\\2AMD15 Big Data Management\\Team_Project\\plays.csv");

        // Split and parse rows into Row objects with schema: (userId, songId, rating)
        Dataset<Row> df = rawLines.map((MapFunction<String, Row>) line -> {
            String[] parts = line.split(",");
            int userId = Integer.parseInt(parts[0].trim());
            int songId = Integer.parseInt(parts[1].trim());
            Integer rating = parts.length == 3 ? Integer.parseInt(parts[2].trim()) : null;
            return RowFactory.create(userId, songId, rating);
        }, RowEncoder.apply(new StructType()
                .add("userId", "int")
                .add("songId", "int")
                .add("rating", "integer") // nullable
        ));

        df.show(); // Optional: just to verify
    }
}

