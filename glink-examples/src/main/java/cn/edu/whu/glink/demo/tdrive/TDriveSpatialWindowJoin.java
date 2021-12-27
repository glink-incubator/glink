package cn.edu.whu.glink.demo.tdrive;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.enums.TopologyType;
import cn.edu.whu.glink.core.geom.Point2;
import cn.edu.whu.glink.core.index.GeographicalGridIndex;
import cn.edu.whu.glink.core.process.SpatialWindowJoin;
import cn.edu.whu.glink.demo.tdrive.kafka.TDriveDeserializer;
import cn.edu.whu.glink.demo.tdrive.kafka.TDriveJoinSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.locationtech.jts.geom.Envelope;

import java.util.Properties;

/**
 * A demo of how to perform spatial window join on t-drive dataset.
 * For simplicity, here we let t-drive's trajectory point itself with its own jon.
 * There should be two different streams in a real-world scenario.
 *
 * @author Yu Liebing
 * */
public class TDriveSpatialWindowJoin {

  public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String bootstrapServer = parameterTool.get("bootstrap.server");
    String inputTopic = parameterTool.get("input.topic");
    String outputTopic = parameterTool.get("output.topic");
    double distance = Double.parseDouble(parameterTool.get("distance"));
    int windowSize = Integer.parseInt(parameterTool.get("window.size"));

    Envelope beijingBound = new Envelope(115.41, 117.51, 39.44, 41.06);
    SpatialDataStream.gridIndex = new GeographicalGridIndex(beijingBound, 5, 5);
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    SpatialDataStream<Point2> tDriveStream1 = new SpatialDataStream<>(
            env, new FlinkKafkaConsumer<>(inputTopic, new TDriveDeserializer(), props))
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Point2>forMonotonousTimestamps()
                    .withTimestampAssigner((p, time) -> p.getTimestamp()));
    SpatialDataStream<Point2> tDriveStream2 = new SpatialDataStream<>(
            env, new FlinkKafkaConsumer<>(inputTopic, new TDriveDeserializer(), props))
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Point2>forMonotonousTimestamps()
                    .withTimestampAssigner((p, time) -> p.getTimestamp()));

    DataStream<Tuple2<Point2, Point2>> dataStream = SpatialWindowJoin.join(
            tDriveStream1,
            tDriveStream2,
            TopologyType.WITHIN_DISTANCE.distance(distance),
            TumblingEventTimeWindows.of(Time.seconds(windowSize)));
    // add sink
    FlinkKafkaProducer<Tuple2<Point2, Point2>> kafkaProducer = new FlinkKafkaProducer<>(
            outputTopic, new TDriveJoinSerializer(outputTopic), props, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
    dataStream.addSink(kafkaProducer);

    env.execute("T-Drive Spatial Window Join");
  }
}
