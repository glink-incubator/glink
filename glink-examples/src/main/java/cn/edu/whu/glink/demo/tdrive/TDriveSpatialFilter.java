package cn.edu.whu.glink.demo.tdrive;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.enums.TopologyType;
import cn.edu.whu.glink.core.geom.Point2;
import cn.edu.whu.glink.core.process.SpatialFilter;
import cn.edu.whu.glink.demo.tdrive.kafka.TDriveSerializer;
import cn.edu.whu.glink.demo.tdrive.kafka.TDriveDeserializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.locationtech.jts.geom.Geometry;

import java.util.List;
import java.util.Properties;

/**
 * @author Yu Liebing
 * */
public class TDriveSpatialFilter {

  public static void main(String[] args) throws Exception {
    final ParameterTool params = ParameterTool.fromArgs(args);
    String queryPath = params.get("query.path");
    String bootstrapServer = params.get("bootstrap.server");
    String groupId = params.get("group.id");
    String inputTopic = params.get("input.topic");
    String outputTopic = params.get("output.topic");

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // create kafka consumer source function
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    FlinkKafkaConsumer<Point2> kafkaConsumer = new FlinkKafkaConsumer<>(
            inputTopic, new TDriveDeserializer(), properties);
    kafkaConsumer.setStartFromEarliest();
    // do the filter
    List<Geometry> beijingDistricts = Utils.readBeijingDistricts(queryPath);
    SpatialDataStream<Point2> tDriveStream = new SpatialDataStream<>(env, kafkaConsumer);
    DataStream<Point2> filteredStream = SpatialFilter.filter(tDriveStream, beijingDistricts, TopologyType.CONTAINS);
    // sink
    FlinkKafkaProducer<Point2> kafkaProducer = new FlinkKafkaProducer<>(
            outputTopic, new TDriveSerializer(outputTopic), properties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
    filteredStream.addSink(kafkaProducer);

    env.execute("T-Drive Spatial Filter");
  }
}
