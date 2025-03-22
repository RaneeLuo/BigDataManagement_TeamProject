import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.Comparator;

public class Question3 {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: Question3 <file>");
            System.exit(1);
        }

        String path = args[0];

        SparkConf conf = new SparkConf().setAppName("YfitopS Question 3").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(path);

        // Remove header and filter bad lines
        JavaRDD<String> filtered = lines
                .filter(line -> !line.startsWith("userId")) // skip header
                .filter(line -> line.split(",").length >= 3); // ensure 3 fields exist

        // Create pairs: (userId, rating)
        JavaPairRDD<String, Tuple2<Double, Integer>> userRatings = filtered
                .mapToPair(line -> {
                    String[] fields = line.split(",");
                    String userId = fields[0];
                    double rating = Double.parseDouble(fields[2]);
                    return new Tuple2<>(userId, new Tuple2<>(rating, 1)); // (rating, count)
                });

        // Reduce: sum ratings and counts per user
        JavaPairRDD<String, Tuple2<Double, Integer>> userTotals = userRatings
                .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

        // Filter users with at least 10 ratings and map to (userId, average)
        JavaPairRDD<String, Double> userAverages = userTotals
                .filter(user -> user._2._2 >= 10) // only users with at least 10 ratings
                .mapToPair(user -> {
                    String userId = user._1;
                    double average = user._2._1 / user._2._2;
                    return new Tuple2<>(userId, average);
                });

        // Find max average rating (with smallest user ID in case of tie)
        Tuple2<String, Double> topUser = userAverages
                .max(new Comparator<Tuple2<String, Double>>() {
                    @Override
                    public int compare(Tuple2<String, Double> t1, Tuple2<String, Double> t2) {
                        int avgCompare = Double.compare(t1._2, t2._2);
                        if (avgCompare != 0) {
                            return avgCompare;
                        } else {
                            return t2._1.compareTo(t1._1); // tiebreak: user with smaller ID wins
                        }
                    }
                });

        // Output result
        System.out.println("Top user: <" + topUser._1 + ", " + topUser._2 + ">");

        sc.stop();
    }
}
