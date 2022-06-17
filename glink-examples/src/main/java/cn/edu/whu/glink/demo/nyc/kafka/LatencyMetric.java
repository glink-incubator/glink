package cn.edu.whu.glink.demo.nyc.kafka;

import cn.edu.whu.glink.core.datastream.TileGridDataStream;
import cn.edu.whu.glink.core.datastream.TrajectoryDataStream;
import cn.edu.whu.glink.core.enums.TileFlatMapType;
import cn.edu.whu.glink.core.tile.Pixel;
import cn.edu.whu.glink.demo.nyc.tile.Tile2;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.locationtech.jts.geom.Point;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * @author Xu Qi
 */
public class LatencyMetric {
  private static int totaldata = 0;
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("bootstrapServer", "s", true, "kafka broker");
    options.addOption("tileTopic", "t", true, "The tile topic to read");
    options.addOption("pointTopic", "p", true, "The point topic to read");
    options.addOption("tileGroupId", "tg", true, "group id");
    options.addOption("pointGroupId", "pg", true, "group id");

    CommandLine cliParser = new DefaultParser().parse(options, args);
//    String bootstrapServer = cliParser.getOptionValue("bootstrapServer");
//    String tileTopic = cliParser.getOptionValue("topic");
//    String pointTopic = cliParser.getOptionValue("groupId");
//    String tileGroupId = cliParser.getOptionValue("tileGroupId");
//    String pointGroupId = cliParser.getOptionValue("pointGroupId");

    String bootstrapServer = "172.21.184.80:9092";
    String tileTopic = "nyc_throughput_out";
    String pointTopic = "nyc_throughput_in";
    String tileGroupId = "ntLdata";
    String pointGroupId = "npLdata";

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // create point kafka consumer source function
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, pointGroupId);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    FlinkKafkaConsumer<Tuple2<Point, String>> kafkaConsumer = new FlinkKafkaConsumer<>(
        pointTopic, new NycDeserializer(), properties);
    kafkaConsumer.setStartFromEarliest();

    // heatmap calculate
    TrajectoryDataStream<Point> nycStream = new TrajectoryDataStream<>(env, kafkaConsumer)
        .assignTimestampsAndWatermarks(WatermarkStrategy
            .<Tuple2<Point, String>>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner(
                (event, time) -> ((Tuple) event.f0.getUserData()).getField(1)));

//    nycStream.getDataStream().map(new CountMapFunction()).print("map");
//    nycStream.getDataStream().map((t) -> ((Tuple) t.f0.getUserData()).getField(2)).print();
    TileGridDataStream<Point, Double> pointTileGridDataStream = new TileGridDataStream<>(
        nycStream, TileFlatMapType.SUM, 13
    );

    // create tile kafka consumer source function
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, tileGroupId);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    FlinkKafkaConsumer<Tile2> kafkaConsumer2 = new FlinkKafkaConsumer<>(
        tileTopic, new NycHeatmapDeserializer(), props);
    kafkaConsumer2.setStartFromEarliest();

    DataStream<Tile2> tileStream = env.addSource(kafkaConsumer2)
        .assignTimestampsAndWatermarks(WatermarkStrategy
            .<Tile2>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner(
                (event, time) -> (Timestamp.valueOf((String) ((Tuple) event.getUserData()).getField(1))).getTime()));
//                (event, time) -> ((LocalDateTime.parse(((Tuple) event.getUserData()).getField(2), formatter)
//          .toInstant(ZoneOffset.of("+8")).toEpochMilli()))));
//    tileStream.print("comno");
//    tileStream.map((t) -> (Timestamp.valueOf((String) ((Tuple) t.getUserData()).getField(1))).getTime()).print();
    DataStream<Tuple2<String, Long>> longDataStream = pointTileGridDataStream.getTileWithIDDataStream()
        .join(tileStream)
        .where(t1 -> t1.f0.getTile().toLong())
        .equalTo(t2 -> Long.valueOf(t2.getTileID()))
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .apply(new LatencyJoinFunction());
    longDataStream.keyBy(0).sum(1).print("Latency");


    env.execute("nyc heatmap latency");
  }

  public static class LatencyJoinFunction implements JoinFunction<Tuple3<Pixel, Point, String>, Tile2, Tuple2<String, Long>> {
    @Override
    public Tuple2<String, Long> join(Tuple3<Pixel, Point, String> pixelPointStringTuple3, Tile2 tile2) throws Exception {
      Tuple pointUserData = (Tuple) pixelPointStringTuple3.f1.getUserData();
      Long dataField = Long.parseLong(pointUserData.getField(pointUserData.getArity() - 1));
      Tuple tile2UserData = (Tuple) tile2.getUserData();
      Long tile2UserDataField = Long.parseLong(tile2UserData.getField(tile2UserData.getArity() - 1));
      totaldata++;
      System.out.println(totaldata);
//      System.out.println(LocalDateTime.ofInstant(Instant.ofEpochMilli(dataField), ZoneOffset.of("+8")));
//      System.out.println(LocalDateTime.ofInstant(Instant.ofEpochMilli(tile2UserDataField), ZoneOffset.of("+8")));
      return new Tuple2<>("0", tile2UserDataField - dataField);
    }
  }

  private static class CountMapFunction implements MapFunction<Tuple2<Point, String>, Integer>  {
    public int totalRecords = 0;

    @Override
    public Integer map(Tuple2<Point, String> pointStringTuple2) throws Exception {
      return totalRecords++;
    }
  }
}
