package com.elfak.bigdata.sample.offline;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;

public class BatchProcessing {

    public static class KeyValue implements Serializable {

        private String key;
        private float value;

        public KeyValue() {
        }

        public KeyValue(String key, float value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public float getValue() {
            return value;
        }

        public void setValue(float value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.key + ";" + String.valueOf(this.value);
        }
    }

    static String master;
    static String modelPath;
    static String savePath;

    private static void processParamteres(String[] args) {
        master = "local[*]";
        if (args.length >= 1) {
            master = args[0];
        }
        modelPath = "data\\offline.csv";
        if (args.length >= 2) {
            modelPath = args[1];
        }
        savePath = "output.csv";
        if (args.length >= 3) {
            savePath = args[2];
        }
    }

    public static void main(String[] args) throws IOException {

        processParamteres(args);

        SparkSession spark = SparkSession.builder()
                .appName("BatchProcessing")
                .master(master)
                //.config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
                .getOrCreate();

        StructType schema = new StructType(
                new StructField[]{
                    new StructField("car_id", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("edge_id", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("edge_name", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("tick", DataTypes.IntegerType, true, Metadata.empty())
                }
        );

        spark.sparkContext().setLogLevel("WARN");

        try {
            Dataset<Row> df = spark.read().format("com.databricks.spark.csv").schema(schema).load(modelPath);

            df.printSchema();

            df.cache();
            df.show();
            df.schema();

            JavaPairRDD<String, Long> javaPairRDD = df.toJavaRDD().mapToPair((row) -> {
                int tick = row.getInt(3) / 60;
                return new Tuple2<>(row.getString(0) + ";" + row.getString(1) + ";" + row.getString(2) + ";" + String.valueOf(tick), 1L);
            }).reduceByKey((s, s2) -> {
                return s + s2;
            });

            JavaPairRDD<String, Long> total_vehicles_per_min = javaPairRDD.mapToPair((row) -> {
                String key_splits[] = row._1.split(";");
                //(car-247;140789744#3;Књажевачка;5,11)
                return new Tuple2<>(key_splits[1] + ";" + key_splits[2] + ";" + key_splits[3], 1L);
            }).reduceByKey((s1, s2) -> {
                return s1 + s2;
            });

            JavaPairRDD<String, Long> total_waitings_per_min = javaPairRDD.mapToPair((row) -> {
                String key_splits[] = row._1.split(";");
                //(car-247;140789744#3;Књажевачка;5,        11)
                return new Tuple2<>(key_splits[1] + ";" + key_splits[2] + ";" + key_splits[3], row._2);
            }).reduceByKey((s1, s2) -> {
                return s1 + s2;
            });

            JavaPairRDD<String, Tuple2<Long, Long>> joined_per_min = total_vehicles_per_min.join(total_waitings_per_min);

            JavaRDD<KeyValue> javaRDD = joined_per_min.map((row) -> {
                float value = row._2._2 / (float) row._2._1;
                return new KeyValue(row._1, value);
            });

            //Encoder<Tuple2<String, Float>> encoder2 = Encoders.tuple(Encoders.STRING(), Encoders.FLOAT());
            Dataset<KeyValue> dataset = spark.createDataset(javaRDD.rdd(), Encoders.bean(KeyValue.class));

            dataset.createOrReplaceTempView("average_view");

            Dataset<Row> new_ds = dataset.sqlContext().sql("Select key,value from average_view order by value desc");

            //dataset.select("key", "value").orderBy(functions.desc("value")).limit(100);
            new_ds.show();

            //new_ds.toJavaRDD().saveAsTextFile("output");
            new_ds.repartition(1)
                    .write()
                    .format("com.databricks.spark.csv")
                    .option("header", true)
                    .save(savePath);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
