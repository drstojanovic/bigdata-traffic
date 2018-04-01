/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.elfak.bigdata.sample.streaming;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.json.JSONObject;
import scala.Tuple2;
import twitter4j.Status;

/**
 *
 * @author igord
 */
public class NisTwitter {

    public static class Location implements Serializable {

        double latitude;
        double longitude;

        public Location(double latitude, double longitude) {
            this.latitude = latitude;
            this.longitude = longitude;
        }

        public double getLatitute() {
            return latitude;
        }

        public void setLatitute(double latitute) {
            this.latitude = latitute;
        }

        public double getLongitude() {
            return longitude;
        }

        public void setLongitude(double longitude) {
            this.longitude = longitude;
        }

        @Override
        public String toString() {
            return "Location{" + "lat=" + latitude + ", lon=" + longitude + '}';
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 23 * hash + (int) (Double.doubleToLongBits(this.latitude) ^ (Double.doubleToLongBits(this.latitude) >>> 32));
            hash = 23 * hash + (int) (Double.doubleToLongBits(this.longitude) ^ (Double.doubleToLongBits(this.longitude) >>> 32));
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Location other = (Location) obj;
            if (Double.doubleToLongBits(this.latitude) != Double.doubleToLongBits(other.latitude)) {
                return false;
            }
            if (Double.doubleToLongBits(this.longitude) != Double.doubleToLongBits(other.longitude)) {
                return false;
            }
            return true;
        }
        
        public double getDistanceFrom(Location location) {
            return Math.hypot(location.latitude - latitude, location.longitude - longitude);
        }
    }

    static String master;
    static String checkpointPath;
    static double minDistance;

    private static void processParamteres(String[] args) {
        master = "local[*]";
        if (args.length >= 1) {
            master = args[0];
        }
        checkpointPath = "d:\\temp2";
        if (args.length >= 2) {
            checkpointPath = args[1];
        }
        minDistance = 0.01;
        if (args.length >= 3) {
            minDistance = Double.parseDouble(args[2]);
        }
    }

    public static void main(String[] args) throws InterruptedException {

        processParamteres(args);

        SparkConf conf = new SparkConf().setMaster(master).setAppName("DelayTwitter");
        SparkContext sc = new SparkContext(conf);
        sc.setLogLevel("WARN");
        StreamingContext ssc = new StreamingContext(sc, Durations.seconds(2));
        JavaStreamingContext jssc = new JavaStreamingContext(ssc);
        jssc.checkpoint(checkpointPath);

        System.setProperty("twitter4j.oauth.consumerKey", "pvzJ8JQzF7SwEYBmqZ839b3gC");
        System.setProperty("twitter4j.oauth.consumerSecret", "xnkoM4hqlpMmAvx99T025ucHONEVXVygfirS94e3gx82lW8nnP");
        System.setProperty("twitter4j.oauth.accessToken", "878712209131663360-mK4VTloFwy1o2KXXhEWdvnUAqMkV2Ow");
        System.setProperty("twitter4j.oauth.accessTokenSecret", "Z2g2CZ8yj89uaCJKOvm8FkbbwuPtmZnWx7vz4H17qniNL");

        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc);

        // extract only longer tweets
        JavaDStream<String> tweets = stream.map(s -> s.getText()).filter(s -> s.length() > 50);

        // get Bounding Box coordinates from www.mapdevelopers.com/geocode_bounding_box.php
        double minLat = 43.283501;
        double maxLat = 43.348861;

        double minLon = 21.826487;
        double maxLon = 21.960232;

        // assign tweets random geo location
        JavaPairDStream<String, Tuple2<Location, String>> geoTweets = tweets.mapToPair(tweet -> {
            double latitude = ThreadLocalRandom.current().nextDouble(minLat, maxLat);
            double longitude = ThreadLocalRandom.current().nextDouble(minLon, maxLon);

            return new Tuple2<>("TW", new Tuple2<>(new Location(latitude, longitude), tweet));
        });

        // include Delays from Kafka
        List<String> topicSet = Arrays.asList("nis-out");
        Map<String, Object> kafkaParams = getKafkaParams();
        ConsumerStrategy consumerStrategy = ConsumerStrategies.Subscribe(topicSet, kafkaParams);
        JavaInputDStream<ConsumerRecord<String, String>> messageDStream
                = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), consumerStrategy);

        // extract location from Kafka message and repack it to Location structure
        JavaPairDStream<String, Tuple2<Location, String>> delays = messageDStream
                .mapToPair(x -> {
                    String message = x.value();

                    JSONObject jsonObj = new JSONObject(message);
                    double latitude = Double.parseDouble(jsonObj.get("lat").toString());
                    double longiute = Double.parseDouble(jsonObj.get("lon").toString());
                    Location location = new Location(latitude, longiute);

                    return new Tuple2<>("DL", new Tuple2<>(location, message));
                });

        // join tweets and kafka delays
        JavaPairDStream<String, Tuple2<Location, String>> union = delays
                .union(geoTweets)
                .window(Durations.seconds(6), Durations.seconds(2));

        union.foreachRDD(rddItem -> {
            // filter only twitter and only delay data
            JavaPairRDD<Location, String> kafkaTwitter = rddItem
                    .filter(item -> item._1.compareTo("TW") == 0)
                    .mapToPair(item -> item._2);

            JavaPairRDD<Location, String> kafkaDelays = rddItem
                    .filter(item -> item._1.compareTo("DL") == 0)
                    .mapToPair(item -> item._2);

            // join twitter with nerby cars
            JavaPairRDD<Tuple2<Location, String>, Tuple2<Location, String>> dekart = kafkaTwitter
                    .cartesian(kafkaDelays)
                    .filter(rdd -> {
                        Location kafkaLocation = rdd._1._1;
                        Location delayLocation = rdd._2._1;
                        double distance = kafkaLocation.getDistanceFrom(delayLocation);

                        return distance < minDistance;
                    });

            // get only distinct tweets
            JavaRDD<Tuple2<Location, String>> distinctTweets = dekart.keys().distinct();

            distinctTweets.foreach(rdd -> {
                System.out.println(rdd.toString());
            });
        });
        //union.print();
        jssc.start();

        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static Map<String, Object> getKafkaParams() {
        String groupId = "testgroup";
        String offsetReset = "earliest";
        String pollTimeout = "5000";
        String brokers = "kafka:9092";

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        kafkaParams.put("spark.kafka.poll.time", pollTimeout);
        return kafkaParams;
    }
}
