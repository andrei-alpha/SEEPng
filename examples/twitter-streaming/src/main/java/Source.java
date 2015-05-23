
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
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
            .newField(Type.STRING, "text").build();
	
	private boolean working = true;
	private static long ts = 0;
	
	@Override
	public void setUp() {
	    // TODO: auto generated method
	}
	
	@Override
	public void processData(ITuple data, API api) {	    
	    while(working) {
            try{
                URL resourceReport = new URL("http://wombat07.doc.res.ic.ac.uk:7011/tweet/text100");
                URLConnection urlConn = resourceReport.openConnection();
                BufferedReader in = new BufferedReader(new InputStreamReader(urlConn.getInputStream()));                
                Object obj=JSONValue.parse(in.readLine());
                Object[] messages = ((JSONArray)obj).toArray();
                for (Object message : messages) {
                    String text = (String)message;
                    byte[] processedData = OTuple.create(schema, new String[]{"ts", "text"}, new Object[]{ts++, text});
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
