package io.github.spycsh.hesse.aws;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.Message;

public class RemoteUDFFn implements StatefulFunction {
  static final TypeName TYPENAME = TypeName.typeNameOf("hesse.aws", "remote");
  static final StatefulFunctionSpec SPEC =
      StatefulFunctionSpec.builder(TYPENAME).withSupplier(RemoteUDFFn::new).build();

  @Override
  public CompletableFuture<Void> apply(Context context, Message message) {
    System.out.println("Welcome,!");
    return context.done();
  }
}
