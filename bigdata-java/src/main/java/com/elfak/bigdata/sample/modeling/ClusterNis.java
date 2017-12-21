/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.elfak.bigdata.sample.modeling;

import java.io.IOException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.linalg.Vector;
import static org.apache.spark.sql.types.DataTypes.*;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author igord
 */
public class ClusterNis {

    static String warehouseDir;
    static String dataPath;
    static String modelPath = "data\\savemodel";

    static int numClasters = 6;

    private static void processParamteres(String[] args) {
        warehouseDir = "file:///tmp/spark-warehouse";
        if (args.length >= 2) {
            warehouseDir = args[1];
        }

        dataPath = "data\\nis.csv";
    }

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {

        processParamteres(args);

        SparkSession spark = SparkSession.builder()
                .appName("uber")
                .master("local[*]")
                //.config("spark.sql.warehouse.dir", warehouseDir)
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");
        
        StructType schema = new StructType(
                new StructField[]{
                    new StructField("id", StringType, true, Metadata.empty()),
                    new StructField("wait", DoubleType, true, Metadata.empty()),
                    new StructField("lat", DoubleType, true, Metadata.empty()),
                    new StructField("lon", DoubleType, true, Metadata.empty()),}
        );

        Dataset<Row> df = spark.read()
                .format("com.databricks.spark.csv")
                .schema(schema)
                .load(dataPath);

        df.printSchema();

        df.cache();
        df.show();
        df.schema();

        String[] featureCols = new String[]{"lat", "lon"};
        VectorAssembler assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features");
        Dataset<Row> df2 = assembler.transform(df);

        Dataset<Row>[] rows = df2.randomSplit(new double[]{0.7, 0.3}, 5043);
        Dataset<Row> trainingData = rows[0];
        Dataset<Row> testData = rows[1];

        KMeans kmeans = new KMeans().setK(numClasters).setFeaturesCol("features").setMaxIter(5);
        KMeansModel model = kmeans.fit(trainingData);
        System.out.println("Final Centers: ");
        for (Vector clusterCenter : model.clusterCenters()) {
            System.out.println(clusterCenter);
        }

        Dataset<Row> categories = model.transform(testData);
        categories.show();
        //categories.createOrReplaceTempView("nis");

//        spark.sql("select id, max(wait) from nis group by id").show();
//        spark.sql("select prediction, max(wait) from nis group by prediction").show();
//        categories.select(month(new Column("dt")).alias("month"), dayofmonth(new Column("dt")).alias("day"), hour(new Column("dt")).alias("hour"), new Column("prediction"))
//                .groupBy("month", "day", "hour", "prediction")
//                .agg(count("prediction").alias("count"))
//                .orderBy("day", "hour", "prediction")
//                .show();
//
//        categories.select(hour(new Column("dt")).alias("hour"), new Column("prediction"))
//                .groupBy("hour", "prediction")
//                .agg(count("prediction").alias("count"))
//                .orderBy(desc("count"))
//                .show();
//
//        categories.groupBy("prediction").count().show();
        // raw sql
        //spark.sql("select prediction, count(prediction) as count from nis group by prediction").show();
        //spark.sql("SELECT hour(nis.dt) as hr,count(prediction) as ct FROM nis group By hr").show();
        //  to save the model 
        model.write().overwrite().save(modelPath);

        // to save the categories dataframe as json data
        //Dataset<Row> res = spark.sql("select dt, lat, lon, base, prediction as cid FROM uber order by dt");
        //res.write().format("json").save("clusterstest");
    }
}
