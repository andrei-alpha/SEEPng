
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.net.URLConnection;

import org.json.simple.*;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;


public class Source implements SeepTask {

	private Schema schema = SchemaBuilder.getInstance().newField(Type.LONG, "ts")
            .newField(Type.STRING, "text").newField(Type.STRING, "user").build();
	
	private BufferedReader serverIn;
	private Socket clientSocket;
	private boolean working = true;
	private static long ts = 0;
	
	@Override
	public void setUp() {
	    try {
	        String hostName = "wombat07.doc.res.ic.ac.uk";
            clientSocket = new Socket(hostName, 7012);
            serverIn = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        } 
        catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	@Override
	public void processData(ITuple data, API api) {	    
	    while(working) {
            try{                
                Object obj=JSONValue.parse(serverIn.readLine());
                JSONArray messages = ((JSONArray)obj);
                for (Object message : messages) {
                    String text = ((JSONObject)(message)).get("text").toString();
                    String user = ((JSONObject)(message)).get("user").toString();
                    byte[] processedData = OTuple.create(schema, new String[]{"ts", "text", "user"}, new Object[]{ts++, text, user});
                    api.send(processedData);
                }
            } catch (MalformedURLException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
	}
	
	private void waitHere(int time){
		try {
			Thread.sleep(time);
		} 
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void processDataGroup(ITuple dataBatch, API api) {
		// TODO Auto-generated method stub
	}

	@Override
	public void close() {
		this.working = false;
	}
}
