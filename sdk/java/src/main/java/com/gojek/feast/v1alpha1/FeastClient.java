package com.gojek.feast.v1alpha1;

import com.gojek.feast.v1alpha1.models.Row;
import feast.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.FeatureSet;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeastClient implements AutoCloseable {
  Logger logger = LoggerFactory.getLogger(FeastClient.class);

  private static final int CHANNEL_SHUTDOWN_TIMEOUT_SEC = 5;

  private final ManagedChannel channel;
  private final ServingServiceGrpc.ServingServiceBlockingStub stub;

  public static FeastClient create(String host, int port) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    return new FeastClient(channel);
  }

  public GetFeastServingInfoResponse getFeastServingInfo() {
    return stub.getFeastServingInfo(GetFeastServingInfoRequest.newBuilder().build());
  }

  public List<Row> getOnlineFeatures(
      List<String> featureIds, List<Row> rows, boolean omitEntitiesInResponse) {
    // featureSetMap is a map of pair of feature set name and version -> a list of feature names
    Map<Pair<String, Integer>, List<String>> featureSetMap = new HashMap<>();
    for (String featureId : featureIds) {
      String[] parts = featureId.split(":");
      if (parts.length < 3) {
        throw new IllegalArgumentException(
            String.format(
                "Feature id '%s' has invalid format. Expected format: <feature_set_name>:<version>:<feature_name>.",
                featureId));
      }
      String featureSetName = parts[0];
      int featureSetVersion = -1;
      try {
        featureSetVersion = Integer.parseInt(parts[1]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            String.format("Feature id '%s' contains invalid version", parts[1]));
      }

      Pair<String, Integer> key = new Pair<>(featureSetName, featureSetVersion);
      if (!featureSetMap.containsKey(key)) {
        featureSetMap.put(key, new ArrayList<>());
      }
      String featureName = parts[2];
      featureSetMap.get(key).add(featureName);
    }
    List<FeatureSet> featureSets =
        featureSetMap.entrySet().stream()
            .map(
                entry ->
                    FeatureSet.newBuilder()
                        .setName(entry.getKey().getKey())
                        .setVersion(entry.getKey().getValue())
                        .addAllFeatureNames(entry.getValue())
                        .build())
            .collect(Collectors.toList());

    List<EntityRow> entityRows =
        rows.stream()
            .map(
                row ->
                    EntityRow.newBuilder()
                        .setEntityTimestamp(row.getEntityTimestamp())
                        .putAllFields(row.getFields())
                        .build())
            .collect(Collectors.toList());

    GetOnlineFeaturesResponse response =
        stub.getOnlineFeatures(
            GetOnlineFeaturesRequest.newBuilder()
                .addAllFeatureSets(featureSets)
                .addAllEntityRows(entityRows)
                .setOmitEntitiesInResponse(omitEntitiesInResponse)
                .build());

    return response.getFieldValuesList().stream()
        .map(
            field -> {
              Row row = Row.create();
              field.getFieldsMap().forEach(row::set);
              return row;
            })
        .collect(Collectors.toList());
  }

  private FeastClient(ManagedChannel channel) {
    this.channel = channel;
    stub = ServingServiceGrpc.newBlockingStub(channel);
  }

  public void close() throws Exception {
    if (channel != null) {
      channel.shutdown().awaitTermination(CHANNEL_SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS);
    }
  }
}
