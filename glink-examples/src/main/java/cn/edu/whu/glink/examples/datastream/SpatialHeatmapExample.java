package cn.edu.whu.glink.examples.datastream;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.datastream.TileGridDataStream;
import cn.edu.whu.glink.core.enums.PyramidTileAggregateType;
import cn.edu.whu.glink.core.enums.TileFlatMapType;
import cn.edu.whu.glink.core.process.SpatialHeatMap;
import cn.edu.whu.glink.core.tile.TileResult;
import cn.edu.whu.glink.examples.utils.SimpleSTDPointFlatMapper;
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
 *   <li>10,113.99005581373353,34.00230051917847,2021-12-11 19:59:57,1,1</li>
 *   <li>10,113.99005581373353,34.00230051917847,2021-12-11 19:59:58,2,3</li>
 *   <li>10,113.99005581373353,34.00230051917847,2021-12-11 19:59:59,1,1</li>
 *   <li>10,113.99005581373353,34.00230051917847,2021-12-11 19:59:59,4,4</li>
 *   <li>10,113.99005581373353,34.00234051917847,2021-12-11 19:59:59,1,1</li>
 *   <li>10,113.99005581373353,34.00234051917847,2021-12-11 19:59:59,2,2</li>
 *   <li>11,114.00076820536366,33.9928652774826,2021-12-11 19:59:59,2,1</li>
 *   <li>11,114.00076820536366,33.9928652774826,2021-12-11 20:00:01,2,1</li>
 *   <li>13,113.9933956639633,34.008135813627185,2021-12-11 20:00:02,3,1</li>
 *   <li>13,113.9933956639633,34.008135813627185,2021-12-11 20:00:03,4,1</li>
 *   <li>13,113.9933956639633,34.008135813627185,2021-12-11 20:00:05,5,2</li>
 * </ul>
 *
 * @author Xu Qi
 */

public class SpatialHeatmapExample {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    SpatialDataStream<Point> spatialDataStream = new SpatialDataStream<>(
        env, "localhost", 8888, new SimpleSTDPointFlatMapper())
        .assignTimestampsAndWatermarks(WatermarkStrategy
            .<Point>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner(
                (event, time) -> ((Tuple) event.getUserData()).getField(1)));

    TileGridDataStream<Point, Double> pointTileGridDataStream = new TileGridDataStream<>(
        spatialDataStream, TileFlatMapType.SUM, 15
    );

    pointTileGridDataStream.print();
    DataStream<TileResult<Double>> heatmapStream = SpatialHeatMap.heatmap(
        pointTileGridDataStream,
        TumblingEventTimeWindows.of(Time.seconds(5)),
        2,
        3,
        13);

    heatmapStream.print();

    env.execute("Glink Spatial HeatMap Example");
  }
}
