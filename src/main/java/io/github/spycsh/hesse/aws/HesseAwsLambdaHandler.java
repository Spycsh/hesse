package io.github.spycsh.hesse.aws;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2ProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2ProxyResponseEvent;
import org.apache.flink.statefun.sdk.java.StatefulFunctions;

// AWS Lambda functions serve as remote functions and are different from embedded functions
// We may deploy some UDF functions as remote functions in AWS Lambda way
// Current embedded functions, including ConnectedComponentsFn... should be managed using AWS EKS
// To add Remote functions as AWS Lambda
// the handler method for AWS Lambda should be set as
// io.github.spycsh.hesse.aws.HesseAwsLambdaHandler::handleRequest
public class HesseAwsLambdaHandler
    implements RequestHandler<APIGatewayV2ProxyRequestEvent, APIGatewayV2ProxyResponseEvent> {

  private static final APIGatewayStateFunHandlerProxy HANDLER;

  static {
    final StatefulFunctions functions = new StatefulFunctions();
    functions.withStatefulFunction(RemoteUDFFn.SPEC);

    HANDLER = new APIGatewayStateFunHandlerProxy(functions.requestReplyHandler());
  }

  @Override
  public APIGatewayV2ProxyResponseEvent handleRequest(
      APIGatewayV2ProxyRequestEvent event, Context context) {
    return HANDLER.handle(event);
  }
}
