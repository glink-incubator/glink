package cn.edu.whu.glink.demo.nyc.hbase;

import cn.edu.whu.glink.connector.geomesa.options.param.GeoMesaDataStoreParam;
import cn.edu.whu.glink.connector.geomesa.options.param.GeoMesaDataStoreParamFactory;
import cn.edu.whu.glink.connector.geomesa.sink.GeoMesaSinkFunction;
import cn.edu.whu.glink.connector.geomesa.util.GeoMesaStreamTableSchema;
import cn.edu.whu.glink.connector.geomesa.util.GeoMesaTableSchema;
import cn.edu.whu.glink.connector.geomesa.util.GeoMesaType;
import cn.edu.whu.glink.core.datastream.TileGridDataStream;
import cn.edu.whu.glink.core.datastream.TrajectoryDataStream;
import cn.edu.whu.glink.core.enums.TileFlatMapType;
import cn.edu.whu.glink.core.process.SpatialHeatMap;
import cn.edu.whu.glink.core.tile.TileResult;
import cn.edu.whu.glink.demo.nyc.kafka.NycDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.locationtech.jts.geom.Point;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * @author Xu Qi
 */
public class HeatmapSinkHbase {
  public static final String ZOOKEEPERS = "u0:2181,u1:2181,u2:2181";
  public static final String KAFKA_BOOSTRAP_SERVERS = "u0:9092";
  public static final String KAFKA_GROUP_ID = "TWOJOBSA";
  public static final String CATALOG_NAME = "Xiamen";
  public static final String TILE_SCHEMA_NAME = "Heatmap";
  public static final String POINTS_SCHEMA_NAME = "JoinedPoints";
  public static final String INPUT_TOPIC = "nyc_throughput_in";
  public static final long WIN_LEN = 5L;
  public static final int PARALLELISM = 16;
  public static void main(String[] args) throws Exception {
    Time windowLength = Time.minutes(WIN_LEN);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setAutoWatermarkInterval(1000L);
    env.setParallelism(PARALLELISM);
    env.disableOperatorChaining();

    // Get point sink function
    Configuration confForOutputPoints = new Configuration();
    confForOutputPoints.setString("geomesa.schema.name", POINTS_SCHEMA_NAME);
    confForOutputPoints.setString("geomesa.spatial.fields", "point2:Point");
    confForOutputPoints.setString("hbase.zookeepers", ZOOKEEPERS);
    confForOutputPoints.setString("geomesa.data.store", "hbase");
    confForOutputPoints.setString("hbase.catalog", CATALOG_NAME);
    confForOutputPoints.setString("geomesa.primary.field.name", "pid");
    List<Tuple2<String, GeoMesaType>> fieldNamesToTypesForPoints = new LinkedList<>();
    fieldNamesToTypesForPoints.add(new Tuple2<>("tid", GeoMesaType.STRING));
    fieldNamesToTypesForPoints.add(new Tuple2<>("ts", GeoMesaType.DATE));
    fieldNamesToTypesForPoints.add(new Tuple2<>("point2", GeoMesaType.POINT));
    fieldNamesToTypesForPoints.add(new Tuple2<>("point2", GeoMesaType.POINT));
    fieldNamesToTypesForPoints.add(new Tuple2<>("point2", GeoMesaType.POINT));
    fieldNamesToTypesForPoints.add(new Tuple2<>("point2", GeoMesaType.POINT));
    fieldNamesToTypesForPoints.add(new Tuple2<>("point2", GeoMesaType.POINT));
    fieldNamesToTypesForPoints.add(new Tuple2<>("point2", GeoMesaType.POINT));
    fieldNamesToTypesForPoints.add(new Tuple2<>("point2", GeoMesaType.POINT));
    fieldNamesToTypesForPoints.add(new Tuple2<>("processTS", GeoMesaType.DATE));
    GeoMesaDataStoreParam pointDataStoreParam = GeoMesaDataStoreParamFactory.createGeomesaDataStoreParam("HBase");
    pointDataStoreParam.initFromConfigOptions(confForOutputPoints);
    GeoMesaStreamTableSchema pointSchema = new GeoMesaStreamTableSchema(fieldNamesToTypesForPoints, confForOutputPoints);
    GeoMesaSinkFunction<Tuple2<Point, String>> pointSink = new GeoMesaSinkFunction<>(pointDataStoreParam, pointSchema, new PointToSimpleFeatureConverter(pointSchema));

    // Get heatmap sink function
    Configuration confForTiles = new Configuration();
    confForTiles.setString("geomesa.data.store", "hbase");
    confForTiles.setString("hbase.catalog", CATALOG_NAME);
    confForTiles.setString("geomesa.schema.name", TILE_SCHEMA_NAME);
    confForTiles.setString("hbase.zookeepers", ZOOKEEPERS);
    confForTiles.setString("geomesa.primary.field.name", "id");
    confForTiles.setString("geomesa.indices.enabled", "id");
    List<Tuple2<String, GeoMesaType>> fieldNamesToTypesForTile = new LinkedList<>();
    fieldNamesToTypesForTile.add(new Tuple2<>("pk", GeoMesaType.STRING));
    fieldNamesToTypesForTile.add(new Tuple2<>("tile_id", GeoMesaType.LONG));
    fieldNamesToTypesForTile.add(new Tuple2<>("windowStartTime", GeoMesaType.DATE));
    fieldNamesToTypesForTile.add(new Tuple2<>("windowEndTime", GeoMesaType.DATE));
    fieldNamesToTypesForTile.add(new Tuple2<>("tile_result", GeoMesaType.STRING));
    GeoMesaDataStoreParam tileDataStoreParam = GeoMesaDataStoreParamFactory.createGeomesaDataStoreParam("HBase");
    tileDataStoreParam.initFromConfigOptions(confForTiles);
    GeoMesaStreamTableSchema heatMapSchema = new GeoMesaStreamTableSchema(fieldNamesToTypesForTile, confForTiles);
    GeoMesaSinkFunction<TileResult<Double>> heatMapSink = new GeoMesaSinkFunction<>(tileDataStoreParam, heatMapSchema, new AvroStringTileResultToSimpleFeatureConverter(heatMapSchema));


    // Kafka properties
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", KAFKA_BOOSTRAP_SERVERS);
    props.put("zookeeper.connect", ZOOKEEPERS);
    props.setProperty("group.id", KAFKA_GROUP_ID);
    // 模拟流
    // create kafka consumer source function
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOSTRAP_SERVERS);
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP_ID);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    FlinkKafkaConsumer<Tuple2<Point, String>> kafkaConsumer = new FlinkKafkaConsumer<>(
        INPUT_TOPIC, new NycDeserializer(), properties);
    kafkaConsumer.setStartFromEarliest();

    // heatmap calculate
    TrajectoryDataStream<Point> nycStream = new TrajectoryDataStream<>(env, kafkaConsumer)
        .assignTimestampsAndWatermarks(WatermarkStrategy
            .<Tuple2<Point, String>>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner(
                (event, time) -> ((Tuple) event.f0.getUserData()).getField(1)));
    //原始出租车轨迹入库
    nycStream.getDataStream()
        .addSink(pointSink);
    // 热力图生成
    TileGridDataStream<Point, Double> pointTileGridDataStream = new TileGridDataStream<>(
        nycStream, TileFlatMapType.SUM, 18
    );
    DataStream<TileResult<Double>> heatmapStream = SpatialHeatMap.heatmap(
        pointTileGridDataStream,
        TumblingEventTimeWindows.of(Time.seconds(5)),
        -1,
        12);
    heatmapStream.addSink(heatMapSink);
    env.execute();
  }
}
