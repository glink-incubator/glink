package cn.edu.whu.glink.demo.tdrive;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.geom.Point2;
import cn.edu.whu.glink.core.process.SpatialWindowKNN;
import cn.edu.whu.glink.demo.tdrive.kafka.TDriveSerializer;
import cn.edu.whu.glink.demo.tdrive.kafka.TDriveDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;

import java.util.Properties;

/**
 * @author Yu Liebing
 * */
public class TDriveSpatialWindowKNN {

  public static void main(String[] args) throws Exception {
    final ParameterTool params = ParameterTool.fromArgs(args);
    String bootstrapServer = params.get("bootstrap.server");
    String groupId = params.get("group.id");
    String inputTopic = params.get("input.topic");
    String outputTopic = params.get("output.topic");
    int k = Integer.parseInt(params.get("k"));
    int windowSize = Integer.parseInt(params.get("window.size"));

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // create kafka consumer source function
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    FlinkKafkaConsumer<Point2> kafkaConsumer = new FlinkKafkaConsumer<>(
            inputTopic, new TDriveDeserializer(), properties);
    kafkaConsumer.setStartFromEarliest();
    kafkaConsumer.assignTimestampsAndWatermarks(WatermarkStrategy
            .<Point2>forMonotonousTimestamps()
            .withTimestampAssigner((p, time) -> p.getTimestamp()));
    // do the knn
    // location of Tian an men Square
    Point queryPoint = SpatialDataStream.geometryFactory.createPoint(new Coordinate(116.403963, 39.915119));
    SpatialDataStream<Point2> tDriveStream = new SpatialDataStream<>(env, kafkaConsumer);
    DataStream<Point2> knnStream = SpatialWindowKNN.knn(
            tDriveStream,
            queryPoint,
            k,
            Double.MAX_VALUE,
            TumblingEventTimeWindows.of(Time.minutes(windowSize)));
    // sink
    FlinkKafkaProducer<Point2> kafkaProducer = new FlinkKafkaProducer<>(
            outputTopic, new TDriveSerializer(outputTopic), properties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
    knnStream.addSink(kafkaProducer);

    env.execute("T-Drive Spatial Window KNN");
  }
}
