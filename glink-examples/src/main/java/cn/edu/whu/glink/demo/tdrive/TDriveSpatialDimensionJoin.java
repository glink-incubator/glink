package cn.edu.whu.glink.demo.tdrive;

import cn.edu.whu.glink.core.datastream.BroadcastSpatialDataStream;
import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.enums.TopologyType;
import cn.edu.whu.glink.core.geom.Point2;
import cn.edu.whu.glink.core.process.SpatialDimensionJoin;
import cn.edu.whu.glink.demo.tdrive.kafka.TDriveDeserializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.locationtech.jts.geom.Geometry;

import java.util.Properties;

/**
 * A demo of how to perform spatial dimension join on t-drive dataset with beijing district.
 *
 * @author Yu Liebing
 * */
public class TDriveSpatialDimensionJoin {

  public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String bootstrapServer = parameterTool.get("bootstrap.server");
    String path = parameterTool.get("path");
    String inputTopic = parameterTool.get("input.topic");
    String outputTopic = parameterTool.get("output.topic");

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // create t-drive trajectory SpatialDataStream
    SpatialDataStream<Point2> tDriveStream = new SpatialDataStream<>(
            env, new FlinkKafkaConsumer<>(inputTopic, new TDriveDeserializer(), props));
    // create beijing district BroadcastSpatialDataStream
    BroadcastSpatialDataStream<Geometry> beijingDistrictStream = new BroadcastSpatialDataStream<>(
            env, path, new Utils.BeijingDistrictFlatMapper());

    DataStream<String> joinStream = SpatialDimensionJoin.join(
            tDriveStream,
            beijingDistrictStream,
            TopologyType.N_CONTAINS,  // district contains trajectory point
            new Utils.SpatialDimensionJoinFunction(),
            new TypeHint<String>() { });
    // add sink
    FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
            outputTopic, new SimpleStringSchema(), props);
    joinStream.addSink(kafkaProducer);

    env.execute("T-Drive Spatial Dimension Join");
  }
}
