package cn.edu.whu.glink.core.process;

import cn.edu.whu.glink.core.datastream.TileGridDataStream;
import cn.edu.whu.glink.core.operator.grid.WindowAggeFunction;
import cn.edu.whu.glink.core.tile.Pixel;
import cn.edu.whu.glink.core.tile.TileResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.locationtech.jts.geom.Geometry;

/**
 * @author Xu Qi
 */
public class SpatialHeatMap {
  public static <T extends Geometry, V, W extends TimeWindow> DataStream<TileResult<V>> heatmap(
      TileGridDataStream<T, V> tileGridDataStream,
      WindowAssigner<? super Tuple2<Pixel, T>, W> windowAssigner,
      int carIDIndex,
      int weightIndex,
      int hLevel) {
    DataStream<TileResult<V>> tileResultDataStream = tileGridDataStream
        .getTileDataStream()
        .keyBy(t -> t.f0.getTile())
        .window(windowAssigner)
        .aggregate(new WindowAggeFunction.WindowAggregate<>(tileGridDataStream.getTileFlatMapType(),
                tileGridDataStream.getSmoothOperator(), carIDIndex, weightIndex),
            new WindowAggeFunction.AddWindowTime<>());

    TileGridDataStream<T, V> tileGridDataStream1 = new <W>TileGridDataStream(
        tileGridDataStream.tileLevel,
        tileGridDataStream.getTileFlatMapType(),
        tileGridDataStream.getPyramidTileAggregateType(),
        tileResultDataStream,
        windowAssigner,
        hLevel);
    return tileGridDataStream1.getAggregateDataStream();
  }
}
