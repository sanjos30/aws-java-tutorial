package com.amazonaws.samples;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsAsync;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsAsyncClientBuilder;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class UserRegoHandler implements RequestHandler<List<String>, String> {

	Gson gson = new GsonBuilder().setPrettyPrinting().create();
	
	private static String stateMachineArn = "arn:aws:states:ap-southeast-2:350823840181:stateMachine:MyStateMachine";


	public String handleRequest(SNSEvent request, Context context) {
		String timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
		context.getLogger().log("Invocation started: " + timeStamp);
		context.getLogger().log(request.getRecords().get(0).getSNS().getMessage());
		
		insertRecordInDynamoDB(request.getRecords().get(0).getSNS().getMessage(),context.getLogger());

		timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
		context.getLogger().log("Invocation completed: handleRequest SNS " + timeStamp);
		return null;
	}

	public String handleRequest(List<String> event, Context context) {

		LambdaLogger logger = context.getLogger();
		String response = new String("200 OK");

		logger.log("EVENT: " + gson.toJson(event));
		logger.log("EVENT TYPE: " + event.getClass().toString());

		logger.log("EVENT: Triggering the State");
		String stateMachineArn = "arn:aws:states:ap-southeast-2:350823840181:stateMachine:MyStateMachine";

		String stateMachineInputJson = "{\r\n" + "    \"user_id\": \"" + event.get(0) + "\"\r\n" + "}\r\n" + "";
		logger.log("The event user input is: " + event.get(0) + " ,and its size is: " + event.size());

		/*
		 * if(event.get(0)!=null) { stateMachineInputJson=event.get(0);
		 * logger.log("The event user input is: " + event.get(0) + " ,and its size is: "
		 * + event.size()); }
		 */
		StartExecutionRequest startExecutionRequest = new StartExecutionRequest();
		startExecutionRequest.setStateMachineArn(stateMachineArn);
		startExecutionRequest.setInput(stateMachineInputJson);

		System.out.println("stateMachineArn: " + stateMachineArn);
		System.out.println("stateMachineInputJson: " + stateMachineInputJson.toString());

		AWSStepFunctionsAsync client = AWSStepFunctionsAsyncClientBuilder.standard()
				.withClientConfiguration(new ClientConfiguration()).withRegion(Regions.AP_SOUTHEAST_2).build();
		System.out.println("startExecutionRequest: " + startExecutionRequest);

		try {
			System.out.println("startExecutionAsync now");

			// Asynch mode
			// client.startExecutionAsync(startExecutionRequest);

			// Synch mode
			client.startExecution(startExecutionRequest);
			logger.log("startExecutionAsync done");
			// return new Response(200,"","stepFunctionTriggered");

		} catch (Exception e) {
			logger.log("Exception while starting execution:" + e);
			// return new Response(400,"","Error occured while executing Step Function");
			response = new String("400 Error occured while executing Step Function");
		}

		client.shutdown();
		context.getLogger().log("Invocation completed: handleRequest List String ");
		return response;
	}
	
	private boolean insertRecordInDynamoDB( String content, LambdaLogger logger) {
		boolean result = true;
		
		logger.log("EVENT: Triggering the State");


		/*
		 * if(event.get(0)!=null) { stateMachineInputJson=event.get(0);
		 * logger.log("The event user input is: " + event.get(0) + " ,and its size is: "
		 * + event.size()); }
		 */
		
		String stateMachineInputJson = "{\r\n" + "    \"user_id\": \"" + content + "\"\r\n" + "}\r\n" + "";
		logger.log("The event user input is: " + content);

		
		StartExecutionRequest startExecutionRequest = new StartExecutionRequest();
		startExecutionRequest.setStateMachineArn(stateMachineArn);
		startExecutionRequest.setInput(stateMachineInputJson);

		System.out.println("stateMachineArn: " + stateMachineArn);
		System.out.println("stateMachineInputJson: " + stateMachineInputJson.toString());

		AWSStepFunctionsAsync client = AWSStepFunctionsAsyncClientBuilder.standard()
				.withClientConfiguration(new ClientConfiguration()).withRegion(Regions.AP_SOUTHEAST_2).build();
		System.out.println("startExecutionRequest: " + startExecutionRequest);

		try {
			System.out.println("startExecutionAsync now");

			// Asynch mode
			// client.startExecutionAsync(startExecutionRequest);

			// Synch mode
			client.startExecution(startExecutionRequest);
			logger.log("startExecutionAsync done");
			// return new Response(200,"","stepFunctionTriggered");

		} catch (Exception e) {
			logger.log("Exception while starting execution:" + e);
			// return new Response(400,"","Error occured while executing Step Function");
			result=false;
		}

		client.shutdown();
		
		return result;
	}

}
