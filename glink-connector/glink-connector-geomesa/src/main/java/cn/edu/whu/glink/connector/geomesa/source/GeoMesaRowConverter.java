package cn.edu.whu.glink.connector.geomesa.source;

import org.apache.flink.annotation.Internal;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.Serializable;

/**
 * A converter used to convert the Geomesa {@link org.opengis.feature.simple.SimpleFeature} into a SQL row record.
 * @param <T> type of input record.
 *
 * @author Yu Liebing
 */
@Internal
public interface GeoMesaRowConverter<T> extends Serializable {

  /**
   * Initialization method for the function. It is called once before conversion method.
   */
  void open();

  /**
   * Converts the Geomesa {@link SimpleFeature} into a SQL row record.
   */
  T convertToRow(SimpleFeature sf);
}
