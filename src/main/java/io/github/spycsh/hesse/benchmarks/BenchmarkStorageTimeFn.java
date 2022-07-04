package io.github.spycsh.hesse.benchmarks;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.slf4j.LoggerFactory;

/**
 * this function receives and aggregates the storage time of records it will output the overall time
 * of storing all records, the number of records and average time one record takes
 */
public class BenchmarkStorageTimeFn implements StatefulFunction {

  static final TypeName TYPE_NAME =
      TypeName.typeNameOf("hesse.benchmarks", "benchmark-storage-time");
  private static final ValueSpec<Long> OVERALL_TIME = ValueSpec.named("overallTime").withLongType();
  private static final ValueSpec<Long> RECORD_NUMBER =
      ValueSpec.named("recordNumber").withLongType();
  public static final StatefulFunctionSpec SPEC =
      StatefulFunctionSpec.builder(TYPE_NAME)
          .withSupplier(BenchmarkStorageTimeFn::new)
          .withValueSpecs(OVERALL_TIME, RECORD_NUMBER)
          .build();

  private static final TypeName KAFKA_EGRESS = TypeName.typeNameOf("hesse.io", "storage-time");

  private static final org.slf4j.Logger LOGGER =
      LoggerFactory.getLogger(BenchmarkStorageTimeFn.class);

  @Override
  public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
    if (message.isLong()) {
      long time = message.asLong();
      long overallTime = context.storage().get(OVERALL_TIME).orElse(0L);
      overallTime += time;
      long recordNumber = context.storage().get(RECORD_NUMBER).orElse(0L);
      recordNumber += 1;

      double averageTimeForEachRecord = (double) overallTime / recordNumber;

      context.storage().set(OVERALL_TIME, overallTime);
      context.storage().set(RECORD_NUMBER, recordNumber);
      LOGGER.trace(
          "[BenchmarkStorageTimeFn {}] overall time in nano seconds: {}, record number: {}, average time for each record in nano seconds: {}",
          context.self().id(),
          overallTime,
          recordNumber,
          String.format("%.2f", averageTimeForEachRecord));

      // egress current time to Kafka topic
      context.send(
          KafkaEgressMessage.forEgress(KAFKA_EGRESS)
              .withTopic("storage-time")
              .withUtf8Key(String.valueOf(recordNumber))
              .withUtf8Value(
                  String.format(
                      "{\"time\":\"%s\", \"overall_time\":\"%s\", \"average_time\":\"%.1f\"}",
                      time, overallTime, averageTimeForEachRecord))
              .build());
    }

    return context.done();
  }
}
