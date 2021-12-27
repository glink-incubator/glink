package cn.edu.whu.glink.examples.datastream;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.enums.TopologyType;
import cn.edu.whu.glink.core.process.SpatialWindowJoin;
import cn.edu.whu.glink.examples.utils.SimpleSTPointFlatMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.Point;

import java.time.Duration;

/**
 * A simple example of how to use glink to perform spatial window join.
 *
 * <p>
 * Emit the flowing lines in the first terminal:
 * 00,114,34,2021-12-11 20:00:01
 * 02,114,34,2021-12-11 20:00:06
 *
 * Emit the flowing lines in the other terminal:
 * 10,113.99005581373353,34.00230051917847,2021-12-11 20:00:01
 * 11,114.00076820536366,33.9928652774826,2021-12-11 20:00:02
 * 13,113.9933956639633,34.008135813627185,2021-12-11 20:00:03
 * 13,113.9933956639633,34.008135813627185,2021-12-11 20:00:06
 *
 * Then you'll se the output:
 * (POINT (114 34),POINT (114.00076820536366 33.9928652774826))
 * (POINT (114 34),POINT (113.99005581373353 34.00230051917847))
 *
 * @author Yu Liebing
 * */
public class SpatialWindowJoinExample {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    SpatialDataStream<Point> pointSpatialDataStream1 =
            new SpatialDataStream<>(env, "localhost", 9000, new SimpleSTPointFlatMapper())
                    .assignTimestampsAndWatermarks(WatermarkStrategy
                            .<Point>forBoundedOutOfOrderness(Duration.ZERO)
                            .withTimestampAssigner(
                                    (event, time) -> ((Tuple2<String, Long>) event.getUserData()).f1));
    SpatialDataStream<Point> pointSpatialDataStream2 =
            new SpatialDataStream<>(env, "localhost", 9001, new SimpleSTPointFlatMapper())
                    .assignTimestampsAndWatermarks(WatermarkStrategy
                            .<Point>forBoundedOutOfOrderness(Duration.ZERO)
                            .withTimestampAssigner(
                                    (event, time) -> ((Tuple2<String, Long>) event.getUserData()).f1));

    DataStream<Tuple2<Point, Point>> windowJoinStream = SpatialWindowJoin.join(
            pointSpatialDataStream1,
            pointSpatialDataStream2,
            TopologyType.WITHIN_DISTANCE.distance(1),
            TumblingEventTimeWindows.of(Time.seconds(5)));
    windowJoinStream.print();

    env.execute("Glink Spatial Window Join Example");
  }
}
