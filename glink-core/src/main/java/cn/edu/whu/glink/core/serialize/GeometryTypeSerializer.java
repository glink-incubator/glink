package cn.edu.whu.glink.core.serialize;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;

import java.io.IOException;

public class GeometryTypeSerializer extends TypeSerializer<Geometry> {

  public static final GeometryTypeSerializer INSTANCE = new GeometryTypeSerializer();

  public GeometryTypeSerializer() { }

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public TypeSerializer<Geometry> duplicate() {
    return this;
  }

  @Override
  public Point createInstance() {
    return SpatialDataStream.geometryFactory.createPoint();
  }

  @Override
  public Geometry copy(Geometry from) {
    return from.copy();
  }

  @Override
  public Geometry copy(Geometry from, Geometry reuse) {
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(Geometry geom, DataOutputView target) throws IOException {
    target.writeUTF(SpatialDataStream.wktWriter.write(geom));
  }

  @Override
  public Geometry deserialize(DataInputView source) throws IOException {
    try {
      return SpatialDataStream.wktReader.read(source.readUTF());
    } catch (ParseException e) {
      throw new IOException();
    }
  }

  @Override
  public Geometry deserialize(Geometry geom, DataInputView source) throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    target.writeUTF(source.readUTF());
  }

  @Override
  public boolean equals(Object o) {
    return o.getClass() == this.getClass();
  }

  @Override
  public int hashCode() {
    return this.getClass().hashCode();
  }

  @Override
  public TypeSerializerSnapshot<Geometry> snapshotConfiguration() {
    return new GeometrySerializerSnapshot();
  }

  public static final class GeometrySerializerSnapshot
          extends SimpleTypeSerializerSnapshot<Geometry> {
    public GeometrySerializerSnapshot() {
      super(() -> GeometryTypeSerializer.INSTANCE);
    }
  }
}
