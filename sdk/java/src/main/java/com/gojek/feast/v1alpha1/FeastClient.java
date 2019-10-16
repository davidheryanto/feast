package com.gojek.feast.v1alpha1;

import com.gojek.feast.v1alpha1.models.Row;
import feast.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.serving.ServingServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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
    Map<Pair<String, String>, String> featureSetMap = new HashMap<>();
    for (String featureId : featureIds) {
      String[] parts = featureId.split(":");
      if (parts.length < 3) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid format for feature id: %s. "
                    + "Expected format: feature_set_name:version:feature_name.",
                featureId));
      }
      featureSetMap.put(new Pair<>(parts[0], parts[1]), parts[2]);
    }
    featureSetMap.

    List<EntityRow> entityRows =
        rows.stream()
            .map(
                row ->
                    EntityRow.newBuilder()
                        .setEntityTimestamp(row.getEntityTimestamp())
                        .putAllFields(row.getFields())
                        .build())
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
