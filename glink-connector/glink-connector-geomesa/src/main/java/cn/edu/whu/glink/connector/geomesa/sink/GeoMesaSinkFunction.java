package cn.edu.whu.glink.connector.geomesa.sink;

import cn.edu.whu.glink.connector.geomesa.options.param.GeoMesaDataStoreParam;
import cn.edu.whu.glink.connector.geomesa.util.AbstractGeoMesaTableSchema;
import cn.edu.whu.glink.connector.geomesa.util.GeoMesaTableSchema;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.geotools.data.*;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.util.factory.Hints;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The sink function for GeoMesa.
 *
 * @author Yu Liebing
 * */
@Internal
public class GeoMesaSinkFunction<T>
        extends RichSinkFunction<T>
        implements CheckpointedFunction {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(GeoMesaSinkFunction.class);

  private final GeoMesaDataStoreParam params;
  private GeoMesaTableSchema schema;
  private AbstractGeoMesaTableSchema absSchema;
  private final GeoMesaSimpleFeatureConverter<T> geomesaSimpleFeatureConverter;
  private transient DataStore dataStore;
  private transient FeatureWriter<SimpleFeatureType, SimpleFeature> featureWriter;

  public GeoMesaSinkFunction(GeoMesaDataStoreParam params,
                             GeoMesaTableSchema schema,
                             GeoMesaSimpleFeatureConverter<T> geomesaSimpleFeatureConverter) {
    this.params = params;
    this.schema = schema;
    this.geomesaSimpleFeatureConverter = geomesaSimpleFeatureConverter;
  }
  public GeoMesaSinkFunction(GeoMesaDataStoreParam params,
                             AbstractGeoMesaTableSchema schema,
                             GeoMesaSimpleFeatureConverter<T> geomesaSimpleFeatureConverter) {
    this.params = params;
    this.absSchema = schema;
    this.geomesaSimpleFeatureConverter = geomesaSimpleFeatureConverter;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    LOG.info("Start open GeoMesa table");
    dataStore = DataStoreFinder.getDataStore(params.getParams());
    if (dataStore == null) {
      LOG.error("Could not create data store with provided parameters");
      throw new RuntimeException("Could not create data store with provided parameters.");
    }
    SimpleFeatureType createSft;
    if (schema != null) {
       createSft = schema.getSchema();
    } else {
       createSft = absSchema.getSimpleFeatureType();
    }
    String name = createSft.getTypeName();
    SimpleFeatureType existSft = dataStore.getSchema(name);
    if (existSft == null) {
      throw new RuntimeException(
              String.format("GeoMesa table %s doesn't exist, create it first", name));
    } else {
      String createSchema = DataUtilities.encodeType(createSft);
      String existSchema = DataUtilities.encodeType(existSft);
      if (!createSchema.equals(existSchema)) {
        throw new RuntimeException("GeoMesa schema " + name + " was already exists, "
                + "but the schema you provided is different with the exists one. "
                + "You provide:\n" + createSchema
                + "\nexists:\n" + existSchema);
      }
    }
    featureWriter = dataStore.getFeatureWriterAppend(name, Transaction.AUTO_COMMIT);
    geomesaSimpleFeatureConverter.open();
    LOG.info("End open GeoMesa table");
  }

  @Override
  public void invoke(T value, Context context) throws Exception {
    SimpleFeature sf = geomesaSimpleFeatureConverter.convertToSimpleFeature(value);
    SimpleFeature toWrite = featureWriter.next();
    toWrite.setAttributes(sf.getAttributes());
    ((FeatureIdImpl) toWrite.getIdentifier()).setID(sf.getID());
    toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
    toWrite.getUserData().putAll(sf.getUserData());
    featureWriter.write();
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

  }

  @Override
  public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
    // nothing to do.
  }

  @Override
  public void close() throws Exception {
    if (featureWriter != null) {
      featureWriter.close();
    }
    if (dataStore != null) {
      dataStore.dispose();
    }
  }
}
