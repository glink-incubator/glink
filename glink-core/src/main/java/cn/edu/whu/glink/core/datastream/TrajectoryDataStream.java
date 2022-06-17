package cn.edu.whu.glink.core.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.locationtech.jts.geom.Geometry;

import java.time.Duration;

/**
 * @author Xu Qi
 */
public class TrajectoryDataStream<T extends Geometry> {

  protected StreamExecutionEnvironment env;

  private DataStream<Tuple2<T, String>> trajectoryDataStream;

  public TrajectoryDataStream(final StreamExecutionEnvironment env,
                              final String host,
                              final int port,
                              final String delimiter,
                              final FlatMapFunction<String, Tuple2<T, String>> flatMapFunction) {
    this.env = env;
    trajectoryDataStream = env
        .socketTextStream(host, port, delimiter)
        .flatMap(flatMapFunction);
  }

  public TrajectoryDataStream(final StreamExecutionEnvironment env,
                              final String host,
                              final int port,
                              final FlatMapFunction<String, Tuple2<T, String>> flatMapFunction) {
    this(env, host, port, "\n", flatMapFunction);
  }

  public TrajectoryDataStream(final StreamExecutionEnvironment env,
                              final SourceFunction<Tuple2<T, String>> sourceFunction) {
    this.env = env;
    trajectoryDataStream = env
        .addSource(sourceFunction);
  }

  public DataStream<Tuple2<T, String>> getDataStream() {
    return trajectoryDataStream;
  }

  public TrajectoryDataStream<T> assignTimestampsAndWatermarks(WatermarkStrategy<Tuple2<T, String>> watermarkStrategy) {
    trajectoryDataStream = trajectoryDataStream.assignTimestampsAndWatermarks(watermarkStrategy);
    return this;
  }

  public TrajectoryDataStream<T> assignBoundedOutOfOrderNessWatermarks(Duration maxOutOfOrderNess, int field) {
    trajectoryDataStream = trajectoryDataStream
        .assignTimestampsAndWatermarks(WatermarkStrategy
            .<Tuple2<T, String>>forBoundedOutOfOrderness(maxOutOfOrderNess)
            .withTimestampAssigner((event, time) -> {
              Tuple t = (Tuple) event.f0.getUserData();
              return t.getField(field);
            }));
    return this;
  }
}
