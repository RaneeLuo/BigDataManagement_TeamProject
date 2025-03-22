//Question 5 - Using Spark for ML (8 points)
//You want to build a recommendation model with Spark, for recommending new songs to users
// based on their ratings on past songs.
// You are free to choose, and experiment with different ones.
// One possible approach is to model the users/songs ratings as a sparse matrix,
// and then run classical data mining/machine learning algorithms,
// such as K-Means or K-nearest neighbor, to find possible answers.
// Mllib is a Spark library that can be used for this exercise.

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SongRecommender {

    public static <Vector> void main(String[] args) {

        // Turn off logging clutter
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getRootLogger().setLevel(Level.OFF);

        // Spark setup
        SparkConf conf = new SparkConf()
                .setAppName(SongRecommender.class.getName())
                .setMaster("local[*]")
                .set("spark.driver.host", "localhost");

        SparkSession spark = SparkSession.builder()
                .appName("YfitopS Song Recommendation")
                .config(conf)
                .getOrCreate();

        // Load CSV path
        String transactionsPath = "C:\\Users\\Calan\\Documents\\CodeProjects\\2AMD15 Big Data Management\\Team_Project\\plays.csv";

        // Define schema: userID, songID, rating
        StructType schema = new StructType()
                .add("userID", DataTypes.StringType)
                .add("songID", DataTypes.StringType)
                .add("rating", DataTypes.DoubleType); // Use DoubleType for KMeans features

        // Load data as DataFrame
        Dataset<Row> rawDF = spark.read()
                .option("header", "false")
                .schema(schema)
                .csv(transactionsPath);

        // Fill missing ratings with 0
        Dataset<Row> filledDF = rawDF.na().fill(0);

        // Convert 'rating' column into a vector column called "features"
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"rating"}) // use more features here if you have them
                .setOutputCol("features");

        Dataset<Row> dataset = assembler.transform(filledDF).select("userID", "features");

        // Apply KMeans clustering
        KMeans kmeans = new KMeans()
                .setK(3) // number of clusters
                .setSeed(1L)
                .setFeaturesCol("features")
                .setPredictionCol("prediction");

        KMeansModel model = kmeans.fit(dataset);
        Dataset<Row> predictions = model.transform(dataset);

        // Evaluate clustering using Silhouette score
        ClusteringEvaluator evaluator = new ClusteringEvaluator()
                .setFeaturesCol("features")
                .setPredictionCol("prediction");

        double silhouette = evaluator.evaluate(predictions);
        System.out.println("Silhouette score = " + silhouette);

        // Show cluster centers
        System.out.println("\nCluster Centers:");
        Vector[] centers = (Vector[]) model.clusterCenters();
        for (Vector center : centers) {
            System.out.println(center);
        }

        // Show predictions
        System.out.println("\nCluster assignments:");
        predictions.select("userID", "features", "prediction").show(20, false);

        // Stop Spark
        spark.stop();
    }
}