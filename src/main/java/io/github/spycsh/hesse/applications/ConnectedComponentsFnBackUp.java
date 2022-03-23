package io.github.spycsh.hesse.applications;

import io.github.spycsh.hesse.types.Types;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;

//import java.util.concurrent.CompletableFuture;
//
//public class ConnectedComponentsFn implements StatefulFunction {
//
//    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.applications", "connected-components");
//    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
//            .withSupplier(ConnectedComponentsFn::new)
//            .withValueSpecs()
//            .build();
//
//
//    @Override
//    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
//
//
//        return context.done();
//    }
//}
