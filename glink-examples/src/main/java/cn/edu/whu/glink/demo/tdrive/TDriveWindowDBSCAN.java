package cn.edu.whu.glink.demo.tdrive;

import cn.edu.whu.glink.core.analysis.WindowDBSCAN;
import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.geom.Point2;
import cn.edu.whu.glink.core.index.GeographicalGridIndex;
import cn.edu.whu.glink.demo.tdrive.kafka.TDriveDeserializer;
import cn.edu.whu.glink.demo.tdrive.kafka.TDriveSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.locationtech.jts.geom.Envelope;

import java.util.List;
import java.util.Properties;

/**
 * @author Yu Liebing
 * */
public class TDriveWindowDBSCAN {

  public static void main(String[] args) throws Exception {
    final ParameterTool params = ParameterTool.fromArgs(args);
    String bootstrapServer = params.get("bootstrap.server");
    String groupId = params.get("group.id");
    String inputTopic = params.get("input.topic");
    String outputTopic = params.get("output.topic");
    int windowSize = Integer.parseInt(params.get("window.size"));

    Envelope beijingBound = new Envelope(115.41, 117.51, 39.44, 41.06);
    SpatialDataStream.gridIndex = new GeographicalGridIndex(beijingBound, 40, 10);
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // create kafka consumer source function
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    FlinkKafkaConsumer<Point2> kafkaConsumer = new FlinkKafkaConsumer<>(
            inputTopic, new TDriveDeserializer(true), properties);
    kafkaConsumer.setStartFromEarliest();
    kafkaConsumer.assignTimestampsAndWatermarks(WatermarkStrategy
            .<Point2>forMonotonousTimestamps()
            .withTimestampAssigner((p, time) -> p.getTimestamp()));

    SpatialDataStream<Point2> pointStream = new SpatialDataStream<>(env, kafkaConsumer);

    DataStream<Tuple2<Integer, List<Point2>>> dbscanStream = WindowDBSCAN.dbscan(
            pointStream,
            0.5,
            20,
            TumblingEventTimeWindows.of(Time.seconds(windowSize)));
    // flat the dbscan result
    DataStream<Point2> flatDbscanStream = dbscanStream.flatMap(new DBSCANFlat());

    FlinkKafkaProducer<Point2> kafkaProducer = new FlinkKafkaProducer<>(
            outputTopic,
            new TDriveSerializer(outputTopic, true),
            properties,
            FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
    flatDbscanStream.addSink(kafkaProducer);

    env.execute();
  }

  public static class DBSCANFlat
          implements FlatMapFunction<Tuple2<Integer, List<Point2>>, Point2> {

    @Override
    public void flatMap(Tuple2<Integer, List<Point2>> t,
                        Collector<Point2> collector) throws Exception {
      int clusterId = t.f0;
      for (Point2 p : t.f1) {
        Tuple2<String, Long> userData = (Tuple2<String, Long>) p.getUserData();
        Tuple3<String, Long, Integer> newUserData = new Tuple3<>(userData.f0, userData.f1, clusterId);
        p.setUserData(newUserData);
        collector.collect(p);
      }
    }
  }
}
