package cn.edu.whu.glink.examples.datastream;

import cn.edu.whu.glink.core.datastream.TileGridDataStream;
import cn.edu.whu.glink.core.datastream.TrajectoryDataStream;
import cn.edu.whu.glink.core.enums.TileFlatMapType;
import cn.edu.whu.glink.core.process.SpatialHeatMap;
import cn.edu.whu.glink.core.tile.TileResult;
import cn.edu.whu.glink.examples.utils.SimpleSTDTPointFlatMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.Point;

import java.time.Duration;

/**
 * data example:
 * <ul>
 *   <li>1,113.99005581373353,34.00230051917847,2021-12-11 19:59:57,1,1</li>
 *   <li>2,113.99005581373353,34.00230051917847,2021-12-11 19:59:58,2,3</li>
 *   <li>3,113.99005581373353,34.00230051917847,2021-12-11 19:59:59,1,1</li>
 *   <li>4,113.99005581373353,34.00230051917847,2021-12-11 19:59:59,4,4</li>
 *   <li>5,113.99005581373353,34.00234051917847,2021-12-11 19:59:59,1,1</li>
 *   <li>6,113.99005581373353,34.00234051917847,2021-12-11 19:59:59,2,2</li>
 *   <li>7,114.00076820536366,33.9928652774826,2021-12-11 19:59:59,2,1</li>
 *   <li>8,114.00076820536366,33.9928652774826,2021-12-11 20:00:01,2,1</li>
 *   <li>9,113.9933956639633,34.008135813627185,2021-12-11 20:00:02,3,1</li>
 *   <li>10,113.9933956639633,34.008135813627185,2021-12-11 20:00:03,4,1</li>
 *   <li>11,113.9933956639633,34.008135813627185,2021-12-11 20:00:05,5,2</li>
 * </ul>
 *
 * @author Xu Qi
 */
public class SpatialIncrementalHeatmap {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    TrajectoryDataStream<Point> trajectoryDataStream = new TrajectoryDataStream<>(
        env, "localhost", 8888, new SimpleSTDTPointFlatMapper())
        .assignTimestampsAndWatermarks(WatermarkStrategy
            .<Tuple2<Point, String>>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner(
                (event, time) -> ((Tuple) event.f0.getUserData()).getField(1)));

    TileGridDataStream<Point, Double> pointTileGridDataStream = new TileGridDataStream<>(
        trajectoryDataStream, TileFlatMapType.SUM, 15
    );

    pointTileGridDataStream.print();
    DataStream<TileResult<Double>> heatmapStream = SpatialHeatMap.incrementalHeatmap(
        pointTileGridDataStream,
        TumblingEventTimeWindows.of(Time.seconds(5)),
        3,
        13);

    heatmapStream.print();

    env.execute("GLink Trajectory HeatMap Example");
  }
}