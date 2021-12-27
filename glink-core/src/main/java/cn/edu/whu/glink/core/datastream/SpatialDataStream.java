package cn.edu.whu.glink.core.datastream;

import cn.edu.whu.glink.core.distance.GeographicalDistanceCalculator;
import cn.edu.whu.glink.core.distance.GeometricDistanceCalculator;
import cn.edu.whu.glink.core.index.GeographicalGridIndex;
import cn.edu.whu.glink.core.index.GridIndex;
import cn.edu.whu.glink.core.distance.DistanceCalculator;
import cn.edu.whu.glink.core.enums.GeometryType;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import java.time.Duration;

/**
 * @author Yu Liebing
 */
public class SpatialDataStream<T extends Geometry> {

  /**
   * The stream execution environment
   * */
  protected StreamExecutionEnvironment env;

  protected GeometryType geometryType;

  /**
   * The origin flink {@link DataStream}, be private so the subclasses will not see
   * and need to maintain their own {@link DataStream}.
   * <p>
   * In the {@link SpatialDataStream}, generic type {@link T} is a subclass of {@link Geometry}.
   * If it has non-spatial attributes, it can be obtained through {@link Geometry#getUserData}.
   * It is a flink {@link org.apache.flink.api.java.tuple.Tuple} type.
   * */
  private DataStream<T> spatialDataStream;

  /**
   * Grid index, default is {@link GeographicalGridIndex}
   * */
  public static GridIndex gridIndex = new GeographicalGridIndex(15);

  /**
   * Distance calculator, default is {@link GeometricDistanceCalculator}.
   * */
  public static DistanceCalculator distanceCalculator = new GeographicalDistanceCalculator();

  public static GeometryFactory geometryFactory = new GeometryFactory();
  public static WKTReader wktReader = new WKTReader(geometryFactory);
  public static WKTWriter wktWriter = new WKTWriter();
  public static WKBReader wkbReader = new WKBReader(geometryFactory);
  public static WKBWriter wkbWriter = new WKBWriter();

  protected SpatialDataStream() { }

  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final SourceFunction<T> sourceFunction) {
    this.env = env;
    spatialDataStream = env
            .addSource(sourceFunction);
  }

  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final SourceFunction<T> sourceFunction,
                           GeometryType geometryType) {
    this.env = env;
    spatialDataStream = env
            .addSource(sourceFunction)
            .returns(geometryType.getTypeInformation());
  }

  /**
   * Init a {@link SpatialDataStream} from a text file.
   * */
  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final String path,
                           final FlatMapFunction<String, T> flatMapFunction) {
    this.env = env;
    spatialDataStream = env
            .readTextFile(path)
            .flatMap(flatMapFunction);
  }

  /**
   * Init a {@link SpatialDataStream} from socket.
   * */
  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final String host,
                           final int port,
                           final String delimiter,
                           final FlatMapFunction<String, T> flatMapFunction) {
    this.env = env;
    spatialDataStream = env
            .socketTextStream(host, port, delimiter)
            .flatMap(flatMapFunction);
  }

  /**
   * Init a {@link SpatialDataStream} from socket.
   * */
  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final String host,
                           final int port,
                           final FlatMapFunction<String, T> flatMapFunction) {
    this(env, host, port, "\n", flatMapFunction);
  }

  public SpatialDataStream<T> assignTimestampsAndWatermarks(WatermarkStrategy<T> watermarkStrategy) {
    spatialDataStream = spatialDataStream.assignTimestampsAndWatermarks(watermarkStrategy);
    return this;
  }

  public SpatialDataStream<T> assignMonotonousWatermarks(int field) {
    spatialDataStream = spatialDataStream
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<T>forMonotonousTimestamps()
                    .withTimestampAssigner((event, time) -> {
                      Tuple t = (Tuple) event.getUserData();
                      return t.getField(field);
                    }));
    return this;
  }

  public SpatialDataStream<T> assignBoundedOutOfOrdernessWatermarks(Duration maxOutOfOrderness, int field) {
    spatialDataStream = spatialDataStream
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<T>forBoundedOutOfOrderness(maxOutOfOrderness)
                    .withTimestampAssigner((event, time) -> {
                      Tuple t = (Tuple) event.getUserData();
                      return t.getField(field);
                    }));
    return this;
  }

  /**
   * CRS transform.
   *
   * @param sourceEpsgCRSCode the source epsg CRS code
   * @param targetEpsgCRSCode the target epsg CRS code
   * @param lenient consider the difference of the geodetic datum between the two coordinate systems,
   *                if {@code true}, never throw an exception "Bursa-Wolf Parameters Required", but not
   *                recommended for careful analysis work
   */
  public SpatialDataStream<T> crsTransform(String sourceEpsgCRSCode, String targetEpsgCRSCode, boolean lenient) {
    try {
      CoordinateReferenceSystem sourceCRS = CRS.decode(sourceEpsgCRSCode);
      CoordinateReferenceSystem targetCRS = CRS.decode(targetEpsgCRSCode);
      final MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);
      spatialDataStream = spatialDataStream.map(r -> (T) JTS.transform(r, transform));
      return this;
    } catch (FactoryException e) {
      e.printStackTrace();
    }
    return null;
  }

  public DataStream<T> getDataStream() {
    return spatialDataStream;
  }

  public GeometryType getGeometryType() {
    return geometryType;
  }

  public void print() {
    spatialDataStream
            .map(r -> r + " " + r.getUserData())
            .print();
  }
}
