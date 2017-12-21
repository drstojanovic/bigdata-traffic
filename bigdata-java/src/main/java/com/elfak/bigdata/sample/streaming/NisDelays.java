/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.elfak.bigdata.sample.streaming;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.JSONObject;
import scala.Tuple2;
import scala.Tuple5;

/**
 *
 * @author igord
 */
public class NisDelays implements Serializable {

    public static class Delay implements Serializable {

        public int cid;
        public Date startTime;
        public Date endTime;
        public int maxVehicles;

        public Delay(int cid, Date startTime, Date endTime, int maxVehicles) {
            this.cid = cid;
            this.startTime = startTime;
            this.endTime = endTime;
            this.maxVehicles = maxVehicles;
        }

        public int getCid() {
            return cid;
        }

        public void setCid(int cid) {
            this.cid = cid;
        }

        public Date getStartTime() {
            return startTime;
        }

        public void setStartTime(Date startTime) {
            this.startTime = startTime;
        }

        public Date getEndTime() {
            return endTime;
        }

        public void setEndTime(Date endTime) {
            this.endTime = endTime;
        }

        public int getMaxVehicles() {
            return maxVehicles;
        }

        public void setMaxVehicles(int maxVehicles) {
            this.maxVehicles = maxVehicles;
        }

    }

    static String master;
    static String checkpointPath;

    private static void processParamteres(String[] args) {
        master = "local[*]";
        if (args.length >= 1) {
            master = args[0];
        }
        checkpointPath = "d:\\temp2";
        if (args.length >= 2) {
            checkpointPath = args[1];
        }
    }

    public static void main(String[] args) throws InterruptedException {
        String groupId = "testgroup";
        String offsetReset = "earliest";
        String pollTimeout = "5000";
        String topicc = "nis-out";
        String topicp = "nis-delays";
        String brokers = "kafka:9092";
        
        processParamteres(args);
        
        SparkSession sparkMain = SparkSession.builder().master(master).appName("NisDelays").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkMain.sparkContext());
        jsc.setLogLevel("WARN");
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, new Duration(5000));
        jssc.checkpoint(checkpointPath);

        List<String> topicSet = Arrays.asList(topicc);
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        kafkaParams.put("spark.kafka.poll.time", pollTimeout);

        ConsumerStrategy consumerStrategy = ConsumerStrategies.Subscribe(topicSet, kafkaParams);
        JavaInputDStream<ConsumerRecord<String, String>> messageDStream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), consumerStrategy);

        JavaDStream<String> valuesDStream = messageDStream.map(x -> x.value());

        JavaPairDStream<String, Long> distinctCars = valuesDStream.mapToPair((arg0) -> {
            JSONObject jsonObj = new JSONObject(arg0);
            String cid = jsonObj.get("cid").toString();
            String carid = jsonObj.get("id").toString();
            return new Tuple2<>(cid + ";" + carid, 1L);
        }).reduceByKey((arg0, arg1) -> {
            return 0L;
        });

        JavaPairDStream<Integer, Tuple5<Integer, Date, Integer, Date, Boolean>> groupedWaitingTime = distinctCars.mapToPair((arg0) -> {
            String[] str = arg0._1.split(";");
            int cid = Integer.valueOf(str[0]);
            return new Tuple2<>(cid, new Tuple5<>(1, null, 0, null, false));
        }).reduceByKey((arg0, arg1) -> {
            return new Tuple5<>(arg0._1() + arg1._1(), null, 0, null, false);
        }).updateStateByKey((nums, current) -> {
            Tuple5<Integer, Date, Integer, Date, Boolean> currentState = current.or(new Tuple5(0, null, 0, null, false));

            // ako je fleg true, onda je vec odstampano, pa skini fleg i anuliraj podatke
            if (currentState._5()) {
                return Optional.of(new Tuple5(0, null, 0, null, false));
            }

            // ako je zastoj, setuj fleg i postavi krajnji datum
            if (currentState._2() != null && nums.isEmpty()) {
                return Optional.of(new Tuple5(0, currentState._2(), currentState._3(), Calendar.getInstance().getTime(), true));
            }

            // pocni
            if (currentState._2() == null && !nums.isEmpty()) {
                return Optional.of(new Tuple5(nums.get(0)._1(), Calendar.getInstance().getTime(), nums.get(0)._1(), null, false));
            }

            // u suprotnom nastavi
            if (nums.isEmpty()) {
                return Optional.of(currentState);
            }

            int max = Math.max(nums.get(0)._1(), currentState._3());
            return Optional.of(new Tuple5(nums.get(0)._1(), currentState._2(), max, null, false));

        });

        groupedWaitingTime.print();
        groupedWaitingTime.foreachRDD((rdd) -> {
            rdd.foreach((arg0) -> {
                // if flag set to true
                if (arg0._2._5()) {
                    // prepare object for serialization to JSON
                    Delay delay = new Delay(arg0._1, arg0._2._2(), arg0._2._4(), arg0._2._3());
                    JSONObject jSONObject = new JSONObject(delay);
                    String record = jSONObject.toString();
                    
                    // send delays to Kafka
                    System.out.println("zastoj u cid " + arg0._1 + " poceo u " + arg0._2._2() + " zavrsen u " + arg0._2._4());
                    KafkaProducer producer = new KafkaProducer<>(configureProducer());
                    ProducerRecord<String, String> message = new ProducerRecord<>(topicp, record);
                    producer.send(message);
                    producer.close();
                }
            });
        });

        // Start the computation
        System.out.println("start streaming");
        jssc.start(); // Wait for the computation to terminate
        jssc.awaitTermination();

        jssc.stop(true, true);
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
