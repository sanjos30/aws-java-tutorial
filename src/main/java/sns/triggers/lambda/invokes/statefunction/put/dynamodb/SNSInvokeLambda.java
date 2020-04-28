package sns.triggers.lambda.invokes.statefunction.put.dynamodb;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsAsync;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsAsyncClientBuilder;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;

/**
 * Lambda function to read a notification from SNS and insert it into a DynamoDB table.
 * Lambda function is setup as a subscriber to an AWS Topic. 
 * The function inserts the event to a DynamoDB Table.
 */

public class SNSInvokeLambda implements RequestHandler<SNSEvent,String>{

	public String handleRequest(SNSEvent incomingEvent, Context context) {
		
		String response = new String("200 OK");
		
		//Init Lambda Logging
		String timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
		context.getLogger().log("Invocation started: " + timeStamp);
		context.getLogger().log(incomingEvent.getRecords().get(0).getSNS().getMessage());

		//The JSON expects the content in JSON format.
		//The input would be a user_id
		String stateMachineInputJson = "{\r\n" + "    \"user_id\": \"" + incomingEvent.getRecords().get(0).getSNS().getMessage() + "\"\r\n" + "}\r\n" + "";
		
		//Extract from Lambda Environment variable
		String stateMachineArn = System.getenv("USER_REGISTERATION_STATE_FUNCTION_ARN");
		String MY_AWS_REGION = System.getenv("AWS_REGION");

		StartExecutionRequest startExecutionRequest = new StartExecutionRequest();
		startExecutionRequest.setStateMachineArn(stateMachineArn);
		startExecutionRequest.setInput(stateMachineInputJson);

		AWSStepFunctionsAsync client = AWSStepFunctionsAsyncClientBuilder.standard()
				.withClientConfiguration(
						new ClientConfiguration()).withRegion(MY_AWS_REGION).build();
				
		try {
			// Asynch mode
			// client.startExecutionAsync(startExecutionRequest);

			// Synch mode
			client.startExecution(startExecutionRequest);
			context.getLogger().log("startExecutionAsync done");
			response = new String("200 StepFunctionTriggered");

		} catch (Exception e) {
			response = new String("400 Error occured while executing Step Function");
			context.getLogger().log("Exception while starting execution:" + e);
		}

		client.shutdown();
		return response;
	}

	
	
}
