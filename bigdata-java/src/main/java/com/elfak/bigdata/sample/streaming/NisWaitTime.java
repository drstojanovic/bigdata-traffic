/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.elfak.bigdata.sample.streaming;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

/**
 *
 * @author igord
 */
public class NisWaitTime implements Serializable {

    public static class Center implements Serializable {

        public int cid;
        public double clat;
        public double clon;

        public Center(int cid, double clat, double clon) {
            this.cid = cid;
            this.clat = clat;
            this.clon = clon;
        }

        public int getCid() {
            return cid;
        }

        public void setCid(int cid) {
            this.cid = cid;
        }

        public double getClat() {
            return clat;
        }

        public void setClat(double clat) {
            this.clat = clat;
        }

        public double getClon() {
            return clon;
        }

        public void setClon(double clon) {
            this.clon = clon;
        }

    }

    public static class WaitTime implements Serializable {

        public String id;
        public double wait;
        public double lat;
        public double lon;

        public WaitTime(String id, double wait, double lat, double lon) {
            this.id = id;
            this.wait = wait;
            this.lat = lat;
            this.lon = lon;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public double getWait() {
            return wait;
        }

        public void setWait(double wait) {
            this.wait = wait;
        }

        public double getLat() {
            return lat;
        }

        public void setLat(double lat) {
            this.lat = lat;
        }

        public double getLon() {
            return lon;
        }

        public void setLon(double lon) {
            this.lon = lon;
        }

    }

    static String master;
    static String checkpointPath;
    static String modelPath;

    private static void processParamteres(String[] args) {
        master = "local[*]";
        if (args.length >= 1) {
            master = args[0];
        }
        checkpointPath = "d:\\temp";
        if (args.length >= 2) {
            checkpointPath = args[1];
        }
        modelPath = "data\\savemodel";
        if (args.length >= 3) {
            modelPath = args[2];
        }
    }

    public static void main(String[] args) throws InterruptedException {
        String topics = "nis-waittime";
        String topicp = "nis-out";

        processParamteres(args);

        System.out.println("Use model " + modelPath + " Subscribe to : " + topics + " Publish to: " + topicp);

        String brokers = "kafka:9092";
        String groupId = "sparkApplication";
        int batchInterval = 1000;
        String pollTimeout = "10000";

        SparkSession spark = SparkSession.builder().master(master).appName("NisWaitTime").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, new Duration(batchInterval));
        jssc.checkpoint(checkpointPath);

        List<String> topicSet = Arrays.asList(topics);
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        kafkaParams.put("spark.kafka.poll.time", pollTimeout);
        kafkaParams.put("spark.streaming.kafka.consumer.poll.ms", "8192");

        // load model for getting clusters
        KMeansModel model = KMeansModel.load(modelPath);
        //print out cluster centers
        System.out.println("Final Centers: ");
        for (Vector clusterCenter : model.clusterCenters()) {
            System.out.println(clusterCenter);
        }

        // create a dataframe with cluster centers to join with stream
        List<Center> ac = new ArrayList<>(20);
        int index = 0;
        for (Vector clusterCenter : model.clusterCenters()) {
            ac.add(new Center(index, clusterCenter.toArray()[0], clusterCenter.toArray()[1]));
            index++;
        }

        JavaRDD<Center> cc = jsc.parallelize(ac);
        Dataset<Row> ccdf = spark.createDataFrame(cc, Center.class);

        ConsumerStrategy consumerStrategy = ConsumerStrategies.Subscribe(topicSet, kafkaParams);
        JavaInputDStream<ConsumerRecord<String, String>> messageDStream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), consumerStrategy);

        JavaDStream<String> valuesDStream = messageDStream.map(x -> x.value());

        JavaPairDStream<String, Long> mapPair = valuesDStream.mapToPair(str -> {
            str = str.substring(1, str.length() - 1);
            String[] p = str.split(",");
            return new Tuple2(p[0] + "," + p[2] + "," + p[3], 1L);
        });
        JavaPairDStream<String, Long> reducePair = mapPair.reduceByKeyAndWindow((arg0, arg1) -> {
            return arg0 + arg1;
        }, Durations.seconds(60), Durations.seconds(1));

        reducePair.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                long count = rdd.count();
                System.out.println("count received " + count);

                JavaRDD<WaitTime> waitTimes = rdd
                        .filter((arg0) -> {
                            return arg0._2 >= 50;
                        })
                        .map(str -> {
                            String[] p = str._1.split(",");
                            if (p.length < 3) {
                                System.out.println("null");
                                return new WaitTime("car-00", 0.0, 3.2, 2.0);
                            }
                            return new WaitTime(p[0], str._2, Double.valueOf(p[1]), Double.valueOf(p[2]));
                        });

                waitTimes.foreach((arg0) -> {
                    System.out.println("found car ! " + arg0.id + " wait time: " + arg0.wait);
                });

                if (waitTimes.count() > 0) {

                    Dataset<Row> df = spark.createDataFrame(waitTimes, WaitTime.class);

                    // get features to pass to model
                    String[] featureCols = new String[]{"lat", "lon"};
                    VectorAssembler assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features");
                    Dataset<Row> df2 = assembler.transform(df);

                    Dataset<Row> categories = model.transform(df2);

                    if (categories.count() > 0) {

                        categories.createOrReplaceTempView("nis");

                        // select values to join with cluster centers
                        // convert results to JSON string to send to topic
                        Dataset<Row> clust = categories.select(new Column("id"), new Column("wait"), new Column("lat"), new Column("lon"), new Column("prediction").alias("cid"))
                                .orderBy("wait");

                        Dataset<Row> res = clust.join(ccdf, "cid");
                        res.show();

                        Dataset<String> tRDD = res.toJSON();

                        tRDD.foreach(record -> {
                            KafkaProducer producer = new KafkaProducer<>(configureProducer());
                            ProducerRecord<String, String> message = new ProducerRecord<>(topicp, record);
                            producer.send(message);
                            producer.close();
                        });
                    }
                }
            }
        });

        // Start the computation
        System.out.println("start streaming");
        jssc.start(); // Wait for the computation to terminate
        jssc.awaitTermination();
    }

    public static Properties configureProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }

}
