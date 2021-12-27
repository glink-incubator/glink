package cn.edu.whu.glink.examples.datastream;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.distance.GeographicalDistanceCalculator;
import cn.edu.whu.glink.core.index.GeographicalGridIndex;
import cn.edu.whu.glink.core.process.SpatialWindowKNN;
import cn.edu.whu.glink.examples.datagen.GenWindowKNNData;
import cn.edu.whu.glink.examples.utils.SimpleSTPointFlatMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;

/**
 * A simple example for showing how to perform window knn.
 * This example read the point data from a csv file.
 * See {@link GenWindowKNNData} for how to generate example point data.
 *
 * @author Yu Liebing
 * */
public class SpatialWindowKNNExample {

  public static void main(String[] args) throws Exception {
    String pointTextPath = args[0];

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    SpatialDataStream.gridIndex = new GeographicalGridIndex(
            100, 101, 30, 31, 2, 2);
    SpatialDataStream.distanceCalculator = new GeographicalDistanceCalculator();
    Point queryPoint = SpatialDataStream.geometryFactory.createPoint(new Coordinate(100.5, 30.5));

    SpatialDataStream<Point> pointDataStream = new SpatialDataStream<>(
            env, pointTextPath, new SimpleSTPointFlatMapper());
    pointDataStream.assignTimestampsAndWatermarks(WatermarkStrategy
            .<Point>forMonotonousTimestamps()
            .withTimestampAssigner((point, time) -> {
              Tuple2<String, Long> userData = (Tuple2<String, Long>) point.getUserData();
              return userData.f1;
            }));
    DataStream<Point> knnStream = SpatialWindowKNN.knn(
            pointDataStream,
            queryPoint,
            3,
            Double.MAX_VALUE,
            TumblingEventTimeWindows.of(Time.seconds(5)));

    knnStream
            .map(p -> {
              Tuple2<String, Long> userData = (Tuple2<String, Long>) p.getUserData();
              return new Tuple2<>(userData.f0, p);
            })
            .returns(TypeInformation.of(new TypeHint<Tuple2<String, Point>>() { }))
            .print();

    env.execute("Glink Spatial Window KNN Example");
  }
}
