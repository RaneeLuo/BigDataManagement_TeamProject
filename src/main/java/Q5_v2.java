//SongRecommenderALS ALS (Alternating Least Squares) Setup in Spark MLlib
//ALS is a collaborative filtering algorithm — great for user-song rating data, like what you're using in

//ow ALS does this:
//The ALSModel.recommendForAllUsers(numItems) method:
//Looks at the matrix of predicted ratings between all users and all items (songs).
//Excludes songs the user has already rated (i.e. those that are in your training data).
//Returns the top-N highest predicted songs they haven't interacted with yet.

//here is the result of my code, can you interpret it?:
//25/03/22 15:25:44 INFO CodeGenerator: Code generated in 5.2142 ms
//+------+-----------------------------------------------------------------------------------------------------+
//|userID|recommendations                                                                                      |
//+------+-----------------------------------------------------------------------------------------------------+
//|31    |[{86194, 16.437407}, {36691, 14.841657}, {44527, 14.599353}, {45507, 14.067493}, {7698, 13.990633}]  |
//|34    |[{45652, 15.16923}, {66170, 14.396514}, {5988, 14.018287}, {43751, 13.831491}, {16854, 13.79875}]    |
//|53    |[{4712, 15.119495}, {37689, 14.334068}, {54972, 14.0500345}, {44579, 13.9510765}, {53036, 13.586189}]|
//|65    |[{5086, 6.177325}, {38603, 5.945909}, {41446, 5.8106155}, {539, 5.791908}, {79617, 5.730726}]        |
//|78    |[{6737, 13.730366}, {76562, 13.263081}, {82294, 13.212503}, {40441, 13.147652}, {47090, 13.055231}]  |
//|85    |[{58372, 14.013316}, {83775, 13.611958}, {72694, 13.592477}, {54537, 13.500195}, {26117, 13.337602}] |
//|108   |[{12103, 12.192807}, {75775, 10.7516575}, {26110, 10.513113}, {34564, 10.366322}, {88878, 10.334654}]|
//|133   |[{80887, 15.136329}, {80939, 14.907362}, {18923, 13.597922}, {96003, 13.425494}, {45802, 13.275388}] |
//|137   |[{54828, 17.85684}, {61801, 15.374056}, {94606, 14.979812}, {71405, 14.060291}, {8766, 13.979822}]   |
//|148   |[{72481, 8.011121}, {52634, 7.5476174}, {86663, 7.296729}, {38645, 7.2622385}, {37981, 7.262024}]    |
//|155   |[{54330, 15.058912}, {85953, 14.935245}, {81993, 14.86018}, {37013, 13.568478}, {45231, 13.509101}]  |
//|193   |[{36255, 11.811684}, {12682, 11.690567}, {14684, 11.591602}, {25162, 11.0733185}, {43789, 10.95156}] |
//|211   |[{2011, 14.733608}, {8425, 13.948214}, {39342, 13.651333}, {54971, 13.453555}, {30920, 13.253131}]   |
//|243   |[{8508, 12.339485}, {26310, 11.171628}, {33281, 11.156355}, {20340, 10.827627}, {55894, 10.790087}]  |
//|251   |[{82901, 11.974998}, {97818, 11.9089575}, {19923, 11.907559}, {96758, 11.590333}, {96096, 11.531288}]|
//|255   |[{66451, 11.732485}, {68948, 11.374183}, {23592, 11.1013975}, {4422, 10.905496}, {36287, 10.806741}] |
//|296   |[{19694, 13.048896}, {40433, 13.006341}, {15838, 12.816597}, {93874, 12.760082}, {59321, 12.601455}] |
//|321   |[{67477, 9.953994}, {1720, 9.320102}, {45613, 9.224055}, {67378, 9.146202}, {49186, 9.028906}]       |
//|322   |[{8499, 13.863223}, {7944, 13.583828}, {66000, 13.5429945}, {6498, 13.5152445}, {33239, 13.401255}]  |
//|362   |[{80201, 13.465756}, {79440, 13.117899}, {77071, 12.567421}, {36205, 12.517237}, {14684, 12.499629}] |
//+------+-----------------------------------------------------------------------------------------------------+
//only showing top 20

//|31    |[{86194, 16.437407}, {36691, 14.841657}, {44527, 14.599353}, {45507, 14.067493}, {7698, 13.990633}]  |
//This means:
//
//For userID = 31, the model predicts they would enjoy:
//Song 86194 the most, with a predicted rating of 16.44
//Followed by song 36691 with a predicted rating of 14.84
//…and so on down to song 7698 with a rating of 13.99


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.types.*;



public class Q5_v2 {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: Q5_v2 <C:\\Users\\Calan\\Documents\\CodeProjects\\2AMD15 Big Data Management\\Team_Project\\plays.csv>");
            System.exit(1);
        }

        String inputPath = args[0];

        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("ALS Song Recommender")
                .master("local[*]")
                .getOrCreate();

        // Explicitly define schema
        StructType schema = new StructType()
                .add("userID", DataTypes.IntegerType)
                .add("songID", DataTypes.IntegerType)
                .add("rating", DataTypes.DoubleType); // Use Double or Float

        // Load CSV with schema
        Dataset<Row> raw = spark.read()
                .option("header", "true") // assumes first row has column names
                .option("mode", "DROPMALFORMED") // skip problematic rows
                .schema(schema)
                .csv(inputPath);

        // Drop null ratings and cast to float
        Dataset<Row> ratings = raw
                .filter(functions.col("rating").isNotNull())
                .withColumn("rating", functions.col("rating").cast("float"));

        // Debug print
        ratings.printSchema();
        ratings.show(5);

        // ALS model setup
        ALS als = new ALS()
                .setUserCol("userID")
                .setItemCol("songID")
                .setRatingCol("rating")
                .setMaxIter(10)
                .setRank(10)
                .setRegParam(0.1)
                .setColdStartStrategy("drop");  // avoids NaNs in predictions

        // Fit model
        ALSModel model = als.fit(ratings);

        // Recommend top 5 songs per user
        Dataset<Row> userRecs = model.recommendForAllUsers(5);
        userRecs.show(false);

        spark.stop();
    }
}
