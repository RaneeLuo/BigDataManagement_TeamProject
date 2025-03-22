//Write code in Spark to find the id of the user that gave at least 10 ratings,
// and has the highest average rating.
// If more than one such users exist with equal average rating,
// you can break the ties arbitrarily (i.e., return any one of them).
// You are not allowed to use SparkSQL or dataframes/datasets for this exercise. You are only allowed to use RDDs.
// The format of the answer should be a pair of <id, averageRating>
//Solution: reducebykey sum, where value is <cnt,sumofratings>, and then reduce

import org.apache.spark.api.java.*;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Comparator;


public class Q2HighestRatingUser {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("YfitopS Question 2")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // Load and parse
        JavaRDD<String> rawLines = sc.textFile("C:\\Users\\Calan\\Documents\\CodeProjects\\2AMD15 Big Data Management\\Team_Project\\plays.csv");

        JavaRDD<Tuple3<Integer, Integer, Integer>> parsedRDD = rawLines
                .map(line -> {
                    String[] parts = line.split(",");
                    int userId = Integer.parseInt(parts[0].trim());
                    int songId = Integer.parseInt(parts[1].trim());
                    Integer rating = (parts.length == 3 && !parts[2].trim().isEmpty()) ? Integer.parseInt(parts[2].trim()) : null;
                    return new Tuple3<>(userId, songId, rating);
                });

        // Step 1: Remove null ratings
        JavaRDD<Tuple3<Integer, Integer, Integer>> filteredRDD = parsedRDD
                .filter(tuple -> tuple._3() != null);

        // Step 2 & 3: Map to (userId, (rating, 1)) and reduceByKey to sum ratings & count
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> userRatingSums = filteredRDD
                .mapToPair(tuple -> new Tuple2<>(tuple._1(), new Tuple2<>(tuple._3(), 1)))
                .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

        // Step 4: Filter users with at least 10 ratings
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> usersWith10Ratings = userRatingSums
                .filter(tuple -> tuple._2._2 >= 10);

        // Step 5: Map to (userId, average)
        JavaPairRDD<Integer, Double> userAverages = usersWith10Ratings
                .mapToPair(tuple -> {
                    int userId = tuple._1;
                    double avg = (double) tuple._2._1 / tuple._2._2;
                    return new Tuple2<>(userId, avg);
                });

        // Step 6: Find max average rating
        Tuple2<Integer, Double> topUser = userAverages
                .max(new SerializableComparator());

        System.out.println("User with highest average (≥10 ratings): " + topUser._1 + " (avg: " + topUser._2 + ")");
    }

    // Needed to compare tuples by value
    static class SerializableComparator implements Comparator<Tuple2<Integer, Double>>, java.io.Serializable {
        public int compare(Tuple2<Integer, Double> o1, Tuple2<Integer, Double> o2) {
            return Double.compare(o1._2, o2._2);
        }
    }
}
