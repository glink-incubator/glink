package cn.edu.whu.glink.connector.geomesa.source;

import cn.edu.whu.glink.connector.geomesa.options.param.GeoMesaDataStoreParam;
import cn.edu.whu.glink.connector.geomesa.util.GeoMesaTableSchema;
import cn.edu.whu.glink.connector.geomesa.util.TemporalJoinPredict;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.geotools.data.*;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The GeoMesaRowDataLookupFunction is a standard user-defined table function, it can be used in
 * tableAPI and also useful for temporal table join plan in SQL. It looks up the result as {@link
 * RowData}.
 *
 * @author Yu Liebing
 */
@Internal
public class GeoMesaRowDataLookupFunction extends TableFunction<RowData> {

  private static final Logger LOG = LoggerFactory.getLogger(GeoMesaRowDataLookupFunction.class);
  private static final long serialVersionUID = 1L;

  private transient WKTReader wktReader;
  private transient WKTWriter wktWriter;
  private transient FilterFactory2 filterFactory2;

  private final GeoMesaDataStoreParam geoMesaDataStoreParam;
  private final GeoMesaTableSchema geoMesaTableSchema;
  private final GeoMesaRowConverter<RowData> geoMesaRowConverter;
  private final String queryField;

  private transient DataStore dataStore;

  private transient String typeName;

  public GeoMesaRowDataLookupFunction(GeoMesaDataStoreParam geoMesaDataStoreParam,
                                      GeoMesaTableSchema geoMesaTableSchema,
                                      GeoMesaRowConverter<RowData> geoMesaRowConverter,
                                      String queryField) {
    this.geoMesaDataStoreParam = geoMesaDataStoreParam;
    this.geoMesaTableSchema = geoMesaTableSchema;
    this.geoMesaRowConverter = geoMesaRowConverter;
    this.queryField = queryField;
  }

  public void eval(Object object) throws ParseException, IOException, CQLException {
    Geometry geometry = wktReader.read(object.toString());
    Query query = createQuery(geometry);
    FeatureReader<SimpleFeatureType, SimpleFeature> featureReader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
    while (featureReader.hasNext()) {
      SimpleFeature sf = featureReader.next();
      Geometry resultGeometry = (Geometry) sf.getDefaultGeometry();
      if (geoMesaTableSchema.getTemporalJoinPredict() == TemporalJoinPredict.P_CONTAINS) {
        if (geometry.contains(resultGeometry)) {
          collect(geoMesaRowConverter.convertToRow(sf));
        }
      } else if (geoMesaTableSchema.getTemporalJoinPredict() == TemporalJoinPredict.N_CONTAINS) {
        if (resultGeometry.contains(geometry)) {
          collect(geoMesaRowConverter.convertToRow(sf));
        }
      } else {
        collect(geoMesaRowConverter.convertToRow(sf));
      }
    }
  }

  @SuppressWarnings("checkstyle:OperatorWrap")
  @Override
  public void open(FunctionContext context) throws Exception {
    dataStore = DataStoreFinder.getDataStore(geoMesaDataStoreParam.getParams());
    if (dataStore == null) {
      throw new RuntimeException("Could not create data store with provided parameters.");
    }
    SimpleFeatureType providedSft = geoMesaTableSchema.getSchema();
    typeName = providedSft.getTypeName();
    SimpleFeatureType sft = dataStore.getSchema(typeName);
    if (sft == null) {
      throw new RuntimeException("GeoMesa schema doesn't exist, create it first.");
    } else {
      String providedSchema = DataUtilities.encodeType(providedSft);
      String existsSchema = DataUtilities.encodeType(sft);
      if (!providedSchema.equals(existsSchema)) {
        throw new RuntimeException("GeoMesa schema " + sft.getTypeName() + " was already exists, " +
                "but the schema you provided is different with the exists one. You provide " + providedSchema +
                ", exists: " + existsSchema);
      }
    }
    wktReader = new WKTReader();
    wktWriter = new WKTWriter();
    filterFactory2 = CommonFactoryFinder.getFilterFactory2();
  }

  @Override
  public void close() throws Exception {

  }

  private Query createQuery(Geometry geometry) throws CQLException {
    if (geoMesaTableSchema.getTemporalJoinPredict() == TemporalJoinPredict.RADIUS) {
      Point centerPoint = geometry.getCentroid();
      String dWithIn = String.format("DWITHIN(%s, %s, %f, meters)",
              queryField, wktWriter.write(centerPoint), geoMesaTableSchema.getJoinDistance());
      Filter spatialFilter = ECQL.toFilter(dWithIn);
      return new Query(typeName, spatialFilter);
    } else {
      Envelope envelope = geometry.getEnvelopeInternal();
      Filter spatialFilter = filterFactory2.bbox(queryField,
              envelope.getMinX(), envelope.getMinY(),
              envelope.getMaxX(), envelope.getMaxY(),
              "EPSG:4326");
      return new Query(typeName, spatialFilter);
    }
  }
}
