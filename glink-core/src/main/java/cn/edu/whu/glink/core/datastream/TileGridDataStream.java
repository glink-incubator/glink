package cn.edu.whu.glink.core.datastream;

import cn.edu.whu.glink.core.enums.PyramidAggregateType;
import cn.edu.whu.glink.core.enums.SmoothOperatorType;
import cn.edu.whu.glink.core.enums.TileAggregateType;
import cn.edu.whu.glink.core.operator.grid.AddUpperKeyMapper;
import cn.edu.whu.glink.core.operator.grid.WindowAggeFunction;
import cn.edu.whu.glink.core.tile.*;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

/**
 * @author Xu Qi
 */
public class TileGridDataStream<T extends Geometry, V> {

  /**
   * Tile data flow and aggregate tile data flow
   */
  private DataStream<Tuple2<Pixel, T>> tileDataStream;

  private DataStream<TileResult<V>> tileResultDataStream;

  private DataStream<TileResult<V>> aggregateDataStream;

  /**
   * Initial level
   */
  public int tileLevel;

  /**
   * Pixel aggregation logic and hierarchical aggregation logic enumeration
   */
  public PyramidAggregateType pyramidAggregateType;

  public TileAggregateType tileAggregateType;

  /**
   * Window smoothing operator
   */
  protected SmoothOperatorType smoothOperator;

  public SmoothOperatorType getSmoothOperator() {
    return smoothOperator;
  }

  public PyramidAggregateType getPyramidAggregateType() {
    return pyramidAggregateType;
  }

  public TileAggregateType getTileAggregateType() {
    return tileAggregateType;
  }

  public DataStream<Tuple2<Pixel, T>> getTileDataStream() {
    return tileDataStream;
  }

  public DataStream<TileResult<V>> getAggregateDataStream() {
    return aggregateDataStream;
  }

  public DataStream<TileResult<V>> getTileResultDataStream() {
    return tileResultDataStream;
  }

  public TileGridDataStream(
          final SpatialDataStream<T> spatialDataStream,
          TileAggregateType tileFlatMapType,
          PyramidAggregateType pyramidAggregateType,
          final int level) {
    this.pyramidAggregateType = pyramidAggregateType;
    this.tileAggregateType = tileFlatMapType;
    this.tileLevel = level;

    tileDataStream = spatialDataStream
            .getDataStream()
            .flatMap(new LowestPixelGenerateFlatMap(level));
  }

  public TileGridDataStream(
      final SpatialDataStream<T> spatialDataStream,
      TileAggregateType tileFlatMapType,
      final int level) {
    this.tileAggregateType = tileFlatMapType;
    this.tileLevel = level;

    tileDataStream = spatialDataStream
        .getDataStream()
        .flatMap(new LowestPixelGenerateFlatMap(level));
  }

  public TileGridDataStream(
          final SpatialDataStream<T> spatialDataStream,
          TileAggregateType tileFlatMapType,
          PyramidAggregateType pyramidAggregateType,
          final int level,
          SmoothOperatorType smoothOperator) {
    this.tileAggregateType = tileFlatMapType;
    this.pyramidAggregateType = pyramidAggregateType;
    this.smoothOperator = smoothOperator;
    this.tileLevel = level;

    tileDataStream = spatialDataStream
            .getDataStream()
            .flatMap(new LowestPixelGenerateFlatMap(level));
  }

  public TileGridDataStream(
      final SpatialDataStream<T> spatialDataStream,
      TileAggregateType tileFlatMapType,
      final int level,
      SmoothOperatorType smoothOperator) {
    this.tileAggregateType = tileFlatMapType;
    this.smoothOperator = smoothOperator;
    this.tileLevel = level;

    tileDataStream = spatialDataStream
        .getDataStream()
        .flatMap(new LowestPixelGenerateFlatMap(level));
  }

  public <W extends TimeWindow> TileGridDataStream(
          int tileLevel,
          TileAggregateType tileFlatMapType,
          PyramidAggregateType pyramidTileAggregateType,
          DataStream<TileResult<V>> tileDataStream,
          WindowAssigner<? super Tuple2<TileResult<V>, Tile>, W> windowAssigner,
          Integer hLevel) {
    this.aggregateDataStream = tileDataStream;
    for (int fileNumber = tileLevel - 1; fileNumber >= hLevel; fileNumber--) {
      if (fileNumber == tileLevel - 1) {
        tileResultDataStream = tileDataStream
                .flatMap(new AddUpperKeyMapper<>())
                .keyBy(t -> t.f1)
                .window(windowAssigner)
                .aggregate(new WindowAggeFunction.LevelUpAggregate<>(tileFlatMapType, pyramidTileAggregateType),
                    new WindowAggeFunction.AddWindowTime<>());
      } else {
        assert tileResultDataStream != null;
        tileResultDataStream = tileResultDataStream
                .flatMap(new AddUpperKeyMapper<>())
                .keyBy(t -> t.f1)
                .window(windowAssigner)
                .aggregate(new WindowAggeFunction.LevelUpAggregate<>(tileFlatMapType, pyramidTileAggregateType),
                    new WindowAggeFunction.AddWindowTime<>());
      }
      aggregateDataStream = aggregateDataStream.union(tileResultDataStream);
    }
  }

  private class LowestPixelGenerateFlatMap extends RichFlatMapFunction<T, Tuple2<Pixel, T>> {

    private final int fileNumber;
    private transient TileGrid tileGrid;

    LowestPixelGenerateFlatMap(int fileNumber) {
      this.fileNumber = fileNumber;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      tileGrid = new TileGrid(fileNumber);
    }

    @Override
    public void flatMap(T geom, Collector<Tuple2<Pixel, T>> collector) throws Exception {
      if (geom instanceof Point) {
        Point point = (Point) geom;
        collector.collect(new Tuple2<>(tileGrid.getPixel(point), geom));
      } else {
        throw new IllegalArgumentException("Unsupported geom type");
      }
    }
  }

  public void print() {
    tileDataStream
            .print();
  }
}
