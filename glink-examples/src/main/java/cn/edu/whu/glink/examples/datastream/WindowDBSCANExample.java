package cn.edu.whu.glink.examples.datastream;

import cn.edu.whu.glink.core.analysis.WindowDBSCAN;
import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.geom.Point2;
import cn.edu.whu.glink.core.index.GeographicalGridIndex;
import cn.edu.whu.glink.core.util.GeoUtils;
import cn.edu.whu.glink.examples.utils.SimpleSTPoint2FlatMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.Envelope;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Yu Liebing
 * */
public class WindowDBSCANExample {

  public static void main(String[] args) throws Exception {
    String path = args[0];

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    double centerLng = 100, centerLat = 30;
    Envelope envelope = GeoUtils.calcEnvelopeByDis(centerLng, centerLat, 1);
    SpatialDataStream.gridIndex = new GeographicalGridIndex(envelope, 2, 2);

    SpatialDataStream<Point2> pointDataStream = new SpatialDataStream<>(
            env, path, new SimpleSTPoint2FlatMapper())
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Point2>forMonotonousTimestamps()
                    .withTimestampAssigner((p, time) -> p.getTimestamp()));
    DataStream<Tuple2<Integer, List<Point2>>> dbscanStream = WindowDBSCAN.dbscan(
            pointDataStream,
            0.15,
            3,
            TumblingEventTimeWindows.of(Time.minutes(5)));
    dbscanStream
            .map(new SerializeMapper())
            .print();

    env.execute();
  }

  public static class SerializeMapper implements
          MapFunction<Tuple2<Integer, List<Point2>>, Tuple2<Integer, List<String>>> {

    @Override
    public Tuple2<Integer, List<String>> map(Tuple2<Integer, List<Point2>> t) throws Exception {
      List<String> ids = new ArrayList<>(t.f1.size());
      for (Point2 p : t.f1) {
        ids.add(p.getId());
      }
      return new Tuple2<>(t.f0, ids);
    }
  }
}
