package sqs.triggers.lambda;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;

public class SQSTriggerLambda implements RequestHandler<SQSEvent, Void>{

	public Void handleRequest(SQSEvent event, Context context)
    {
		
		String timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
		context.getLogger().log("Invocation started:SQSTriggerLambda:handleRequest " + timeStamp);
		
        for(SQSMessage msg : event.getRecords()){
        	context.getLogger().log(msg.getBody());            
        }
        return null;
    }
}
