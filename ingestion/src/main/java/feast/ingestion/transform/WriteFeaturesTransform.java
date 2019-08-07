package feast.ingestion.transform;

import com.google.inject.Inject;
import feast.specs.FeatureSpecProto;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.store.serving.redis.RedisCustomIO;
import feast.types.FeatureRowProto.FeatureRow;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.util.HashMap;
import java.util.Map;

import static feast.specs.FeatureSpecProto.*;

public class WriteFeaturesTransform extends PTransform<PCollection<FeatureRow>, PDone> {
  private ImportJobSpecs importJobSpecs;
  private String sinkStorageSpecType;
  private StorageSpec sinkStorageSpec;
  private Map<String, FeatureSpec> featureSpecByFeatureId = new HashMap<>();

  @Inject
  public WriteFeaturesTransform(ImportJobSpecs importJobSpecs) {
    this.importJobSpecs = importJobSpecs;
    this.sinkStorageSpec = importJobSpecs.getSinkStorageSpec();
    this.sinkStorageSpecType = importJobSpecs.getSinkStorageSpec().getType();
    for (FeatureSpec featureSpec : importJobSpecs.getFeatureSpecsList()) {
      featureSpecByFeatureId.put(featureSpec.getId(), featureSpec);
    }

    importJobSpecs.getSinkStorageSpec()
  }

  @Override
  public PDone expand(PCollection<FeatureRow> input) {
    PDone pDone;
    switch (sinkStorageSpecType) {
      case "REDIS":
        input
                .apply("Convert FeatureRow to RedisMutation", ParDo.of(new FeatureRow()))
        RedisCustomIO.write(
            sinkStorageSpec.getOptionsOrThrow("host"),
            Integer.parseInt(sinkStorageSpec.getOptionsOrDefault("port", "6379")));
      case "BIGQUERY":
        break;
      default:
        break;
    }
  }
}
