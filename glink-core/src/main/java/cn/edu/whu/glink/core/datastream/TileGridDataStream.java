package cn.edu.whu.glink.core.datastream;

import cn.edu.whu.glink.core.enums.PyramidTileAggregateType;
import cn.edu.whu.glink.core.enums.SmoothOperatorType;
import cn.edu.whu.glink.core.enums.TileFlatMapType;
import cn.edu.whu.glink.core.operator.grid.AddUpperKeyMapper;
import cn.edu.whu.glink.core.operator.grid.WindowAggeFunction;
import cn.edu.whu.glink.core.tile.*;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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

  private DataStream<Tuple3<Pixel, T, String>> tileWithIDDataStream;

  private DataStream<TileResult<V>> tileResultDataStream;

  private DataStream<TileResult<V>> aggregateDataStream;

  /**
   * Initial level
   */
  public Integer tileLevel;

  /**
   * Pixel aggregation logic and hierarchical aggregation logic enumeration
   */
  public PyramidTileAggregateType pyramidTileAggregateType;

  public TileFlatMapType tileFlatMapType;

  /**
   * Window smoothing operator
   */
  protected SmoothOperatorType smoothOperator;

  public SmoothOperatorType getSmoothOperator() {
    return smoothOperator;
  }

  public PyramidTileAggregateType getPyramidTileAggregateType() {
    return pyramidTileAggregateType;
  }

  public TileFlatMapType getTileFlatMapType() {
    return tileFlatMapType;
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

  public DataStream<Tuple3<Pixel, T, String>> getTileWithIDDataStream() {
    return tileWithIDDataStream;
  }

  public void setTileWithIDDataStream(DataStream<Tuple3<Pixel, T, String>> tileWithIDDataStream) {
    this.tileWithIDDataStream = tileWithIDDataStream;
  }

  public TileGridDataStream(
          final SpatialDataStream<T> spatialDataStream,
          TileFlatMapType tileFlatMapType,
          PyramidTileAggregateType pyramidTileAggreType,
          final int level) {
    this.tileFlatMapType = tileFlatMapType;
    this.pyramidTileAggregateType = pyramidTileAggreType;
    this.tileLevel = level;

    tileDataStream = spatialDataStream
            .getDataStream()
            .flatMap(new LowestPixelGenerateFlatMap(level));
  }

  public TileGridDataStream(
      final SpatialDataStream<T> spatialDataStream,
      TileFlatMapType tileFlatMapType,
      final int level) {
    this.tileFlatMapType = tileFlatMapType;
    this.tileLevel = level;

    tileDataStream = spatialDataStream
        .getDataStream()
        .flatMap(new LowestPixelGenerateFlatMap(level));
  }

  public TileGridDataStream(
          final SpatialDataStream<T> spatialDataStream,
          TileFlatMapType tileFlatMapType,
          PyramidTileAggregateType pyramidTileAggregateType,
          final int level,
          SmoothOperatorType smoothOperator
  ) {
    this.tileFlatMapType = tileFlatMapType;
    this.pyramidTileAggregateType = pyramidTileAggregateType;
    this.smoothOperator = smoothOperator;
    this.tileLevel = level;

    tileDataStream = spatialDataStream
            .getDataStream()
            .flatMap(new LowestPixelGenerateFlatMap(level));
  }

  public TileGridDataStream(
      final SpatialDataStream<T> spatialDataStream,
      TileFlatMapType tileFlatMapType,
      final int level,
      SmoothOperatorType smoothOperator
  ) {
    this.tileFlatMapType = tileFlatMapType;
    this.smoothOperator = smoothOperator;
    this.tileLevel = level;

    tileDataStream = spatialDataStream
        .getDataStream()
        .flatMap(new LowestPixelGenerateFlatMap(level));
  }

  public TileGridDataStream(
      final TrajectoryDataStream<T> trajectoryDataStream,
      TileFlatMapType tileFlatMapType,
      PyramidTileAggregateType pyramidTileAggreType,
      final int level) {
    this.tileFlatMapType = tileFlatMapType;
    this.pyramidTileAggregateType = pyramidTileAggreType;
    this.tileLevel = level;

    tileWithIDDataStream = trajectoryDataStream
        .getDataStream()
        .flatMap(new TrajectoryLowestPixelGenerateFlatMap(level));
  }

  public TileGridDataStream(
      final TrajectoryDataStream<T> trajectoryDataStream,
      TileFlatMapType tileFlatMapType,
      final int level) {
    this.tileFlatMapType = tileFlatMapType;
    this.tileLevel = level;

    tileWithIDDataStream = trajectoryDataStream
        .getDataStream()
        .flatMap(new TrajectoryLowestPixelGenerateFlatMap(level));
  }

  public TileGridDataStream(
      final TrajectoryDataStream<T> trajectoryDataStream,
      TileFlatMapType tileFlatMapType,
      PyramidTileAggregateType pyramidTileAggregateType,
      final int level,
      SmoothOperatorType smoothOperator
  ) {
    this.tileFlatMapType = tileFlatMapType;
    this.pyramidTileAggregateType = pyramidTileAggregateType;
    this.smoothOperator = smoothOperator;
    this.tileLevel = level;

    tileWithIDDataStream = trajectoryDataStream
        .getDataStream()
        .flatMap(new TrajectoryLowestPixelGenerateFlatMap(level));
  }

  public TileGridDataStream(
      final TrajectoryDataStream<T> trajectoryDataStream,
      TileFlatMapType tileFlatMapType,
      final int level,
      SmoothOperatorType smoothOperator
  ) {
    this.tileFlatMapType = tileFlatMapType;
    this.smoothOperator = smoothOperator;
    this.tileLevel = level;

    tileWithIDDataStream = trajectoryDataStream
        .getDataStream()
        .flatMap(new TrajectoryLowestPixelGenerateFlatMap(level));
  }

  public <W extends TimeWindow> TileGridDataStream(
          int tileLevel,
          TileFlatMapType tileFlatMapType,
          PyramidTileAggregateType pyramidTileAggregateType,
          DataStream<TileResult<V>> tileDataStream,
          WindowAssigner<? super Tuple2<TileResult<V>, Tile>, W> windowAssigner,
          Integer hLevel) {
    this.aggregateDataStream = tileDataStream;
    for (int fileNumber = tileLevel - 1; fileNumber >= hLevel; fileNumber--) {
      if (fileNumber == tileLevel - 1) {
        tileResultDataStream = tileDataStream
                .flatMap(new AddUpperKeyMapper<>())
                .keyBy(t -> t.f1)
                //层级瓦片重采样
                .window(windowAssigner)
                .aggregate(new WindowAggeFunction.LevelUpAggregate<>(tileFlatMapType, pyramidTileAggregateType),
                    new WindowAggeFunction.AddWindowTime<>());
      } else {
        assert tileResultDataStream != null;
        tileResultDataStream = tileResultDataStream
                .flatMap(new AddUpperKeyMapper<>())
                .keyBy(t -> t.f1)
                //层级瓦片重采样
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

  private class TrajectoryLowestPixelGenerateFlatMap extends RichFlatMapFunction<Tuple2<T, String>, Tuple3<Pixel, T, String>> {

    private final int fileNumber;
    private transient TileGrid tileGrid;

    TrajectoryLowestPixelGenerateFlatMap(int fileNumber) {
      this.fileNumber = fileNumber;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      tileGrid = new TileGrid(fileNumber);
    }

    @Override
    public void flatMap(Tuple2<T, String> geom, Collector<Tuple3<Pixel, T, String>> collector) throws Exception {
      if (geom.f0 instanceof Point) {
        Point point = (Point) geom.f0;
        collector.collect(new Tuple3<>(tileGrid.getPixel(point), geom.f0, geom.f1));
      } else {
        throw new IllegalArgumentException("Unsupported geom type");
      }
    }
  }

  public void print() {
    if (tileDataStream != null) {
      tileDataStream
          .print();
    } else {
      tileWithIDDataStream
          .print();
    }
  }
}
