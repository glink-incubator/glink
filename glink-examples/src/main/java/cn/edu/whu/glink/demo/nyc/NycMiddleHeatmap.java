package cn.edu.whu.glink.demo.nyc;

import cn.edu.whu.glink.core.datastream.TileGridDataStream;
import cn.edu.whu.glink.core.datastream.TrajectoryDataStream;
import cn.edu.whu.glink.core.enums.TileFlatMapType;
import cn.edu.whu.glink.core.process.SpatialHeatMap;
import cn.edu.whu.glink.core.tile.TileResult;
import cn.edu.whu.glink.demo.nyc.kafka.NycDeserializer;
import cn.edu.whu.glink.demo.nyc.kafka.NycHeatmapSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.locationtech.jts.geom.Point;

import java.time.Duration;
import java.util.Properties;

/**
 * @author Xu Qi
 */
public class NycMiddleHeatmap {
  public static void main(String[] args) throws Exception {
//    String bootstrapServer = "172.28.239.216:9092";
//    String groupId = "cdata";
//    String inputTopic = "nyc_throughput_in";
//    String outputTopic = "nyc_throughput_out";
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String bootstrapServer = parameterTool.get("bootstrap.server");
    String groupId = parameterTool.get("group.id");
    String inputTopic = parameterTool.get("input.topic");
    String outputTopic = parameterTool.get("output.topic");
    int windowSize = Integer.parseInt(parameterTool.get("window.size"));

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // create kafka consumer source function
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    FlinkKafkaConsumer<Tuple2<Point, String>> kafkaConsumer = new FlinkKafkaConsumer<>(
        inputTopic, new NycDeserializer(), properties);
    kafkaConsumer.setStartFromEarliest();

    // heatmap calculate
    TrajectoryDataStream<Point> nycStream = new TrajectoryDataStream<>(env, kafkaConsumer)
        .assignTimestampsAndWatermarks(WatermarkStrategy
            .<Tuple2<Point, String>>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner(
                (event, time) -> ((Tuple) event.f0.getUserData()).getField(1)));

    TileGridDataStream<Point, Double> pointTileGridDataStream = new TileGridDataStream<>(
        nycStream, TileFlatMapType.SUM, 12
    );
    DataStream<TileResult<Double>> heatmapStream = SpatialHeatMap.middleHeatmap(
        pointTileGridDataStream,
        TumblingEventTimeWindows.of(Time.seconds(windowSize)),
        -1,
        15,
        18);

    // sink
    FlinkKafkaProducer<TileResult<Double>> kafkaProducer = new FlinkKafkaProducer<>(
        outputTopic, new NycHeatmapSerializer(outputTopic), properties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
    heatmapStream.addSink(kafkaProducer);
//    heatmapStream.print();

    env.execute("nyc heatmap");
  }
}
