package cn.edu.whu.glink.core.serialize;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.geom.Circle;
import cn.edu.whu.glink.core.geom.Point2;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

/**
 * Provides methods to efficiently serialize and deserialize geometry types.
 * <p>
 * Supports Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon,
 * GeometryCollection, Circle and Envelope types.
 *
 * @author Yu Liebing
 */
public class GeometryKryoSerializer extends JavaSerializer<Geometry> {

  private final WKBReader wkbReader = SpatialDataStream.wkbReader;
  private final WKBWriter wkbWriter = SpatialDataStream.wkbWriter;

  @Override
  public void write(Kryo kryo, Output output, Geometry geometry) {
    // TODO
    if (geometry instanceof Point2) {
      byte[] geometryData = wkbWriter.write(geometry);
      output.writeInt(geometryData.length);
      output.write(geometryData);
      writeUserData(kryo, output, geometry);
    } else {
      byte[] geometryData = wkbWriter.write(geometry);
      output.writeInt(geometryData.length);
      output.write(geometryData);
      writeUserData(kryo, output, geometry);
    }
  }

  @Override
  public Geometry read(Kryo kryo, Input input, Class aClass) {
    try {
      int geometryLen = input.readInt();
      byte[] data = new byte[geometryLen];
      input.readBytes(data);
      Geometry g = wkbReader.read(data);
      Object userData = readUserData(kryo, input);
      g.setUserData(userData);
      return g;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  private void writeUserData(Kryo kryo, Output out, Geometry geometry) {
    out.writeBoolean(geometry.getUserData() != null);
    if (geometry.getUserData() != null) {
      kryo.writeClass(out, geometry.getUserData().getClass());
      kryo.writeObject(out, geometry.getUserData());
    }
  }

  private Object readUserData(Kryo kryo, Input input) {
    Object userData = null;
    if (input.readBoolean()) {
      Registration clazz = kryo.readClass(input);
      userData = kryo.readObject(input, clazz.getType());
    }
    return userData;
  }
}
