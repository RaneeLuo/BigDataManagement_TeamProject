import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import scala.Tuple3;

public class DataLoadAsRDD {
    public static void main(String[] args) {
        // Create a Spark session
        SparkSession spark = SparkSession.builder()
                .appName("YfitopS Load RDD")
                .master("local[*]")
                .getOrCreate();

        // Create a JavaSparkContext from the SparkSession
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // Load raw lines from CSV file
        JavaRDD<String> rawLines = sc.textFile("C:\\Users\\Calan\\Documents\\CodeProjects\\2AMD15 Big Data Management\\Team_Project\\plays.csv");

        // Skip the header if necessary (optional, uncomment if there's a header)
        String header = rawLines.first();
        JavaRDD<String> dataLines = rawLines.filter(line -> !line.equals(header));

        // Parse each line into a Tuple3 of userId, songId, rating (nullable)
        JavaRDD<Tuple3<Integer, Integer, Integer>> parsedRDD = dataLines.map(
                (Function<String, Tuple3<Integer, Integer, Integer>>) line -> {
                    String[] parts = line.split(",");
                    int userId = Integer.parseInt(parts[0].trim());
                    int songId = Integer.parseInt(parts[1].trim());
                    Integer rating = (parts.length == 3 && !parts[2].trim().isEmpty()) ?
                            Integer.parseInt(parts[2].trim()) : null;
                    return new Tuple3<>(userId, songId, rating);
                });

        // Print the first 20 records
        parsedRDD.take(20).forEach(System.out::println);

        // Stop the Spark context
        sc.stop();
    }
}
