package cn.edu.whu.glink.core.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.locationtech.jts.geom.Geometry;

/**
 * @author Yu Liebing
 */
public class BroadcastSpatialDataStream<T extends Geometry> {

  private final DataStream<Tuple2<Boolean, T>> dataStream;

  public BroadcastSpatialDataStream(final StreamExecutionEnvironment env,
                                    final Source<Tuple2<Boolean, T>, ?, ?> source,
                                    WatermarkStrategy<Tuple2<Boolean, T>> watermarkStrategy,
                                    String sourceName) {
    dataStream = env.fromSource(source, watermarkStrategy, sourceName);
  }

  public BroadcastSpatialDataStream(final StreamExecutionEnvironment env,
                                    final SourceFunction<Tuple2<Boolean, T>> sourceFunction) {
    dataStream = env.addSource(sourceFunction);
  }

  public BroadcastSpatialDataStream(final SpatialDataStream<T> spatialDataStream) {
    dataStream = spatialDataStream.getDataStream().map(new MapFunction<T, Tuple2<Boolean, T>>() {
      @Override
      public Tuple2<Boolean, T> map(T value) throws Exception {
        return new Tuple2<>(true, value);
      }
    });
  }

  public BroadcastSpatialDataStream(final StreamExecutionEnvironment env,
                                    final String path,
                                    final FlatMapFunction<String, Tuple2<Boolean, T>> flatMapFunction) {
    dataStream = env
            .readTextFile(path)
            .flatMap(flatMapFunction);
  }

  public BroadcastSpatialDataStream(final StreamExecutionEnvironment env,
                                    final String host,
                                    final int port,
                                    final FlatMapFunction<String, Tuple2<Boolean, T>> flatMapFunction) {
    dataStream = env
            .socketTextStream(host, port)
            .flatMap(flatMapFunction);
  }

  public DataStream<Tuple2<Boolean, T>> getDataStream() {
    return dataStream;
  }
}
