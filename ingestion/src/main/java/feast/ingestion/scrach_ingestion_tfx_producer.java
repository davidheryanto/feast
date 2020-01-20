package feast.ingestion;

import com.google.protobuf.util.Timestamps;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class scrach_ingestion_tfx_producer {

  private static final String TOPIC = "feast";

  public static void main(String[] args) throws InterruptedException {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    Producer<Integer, byte[]> producer = new KafkaProducer<>(props);

    for (int i = 0; i < 50; i++) {
      FeatureRow featureRow = FeatureRow.newBuilder()
          .setFeatureSet("project1/featureset1:1")
          .setEventTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
          .addFields(
              Field.newBuilder().setName("entity1").setValue(Value.newBuilder().setStringVal("1")))
          .addFields(Field.newBuilder().setName("feature1").setValue(Value.newBuilder().setInt64Val(
              (long) (Math.random() * 10))))
          .build();
      producer.send(new ProducerRecord<>(TOPIC, featureRow.toByteArray()));
      Thread.sleep(50);
    }


    producer.flush();
  }
}
