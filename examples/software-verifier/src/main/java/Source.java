
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
            .newField(Type.STRING, "program").newField(Type.STRING, "mode").build();
	
	private boolean working = true;
	private static long ts = 0;
	
	@Override
	public void setUp() {
	    // TODO
	}
	
	@Override
	public void processData(ITuple data, API api) {	    
	    while(working) {
            try{                
                URL url = new URL("http://wombat07.doc.res.ic.ac.uk:7231/program");
                URLConnection conn = url.openConnection();
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                
                Object obj=JSONValue.parse(in.readLine());
                JSONArray params = ((JSONArray)obj);
                String mode = params.get(0).toString();
                String program = params.get(1).toString();
               
                byte[] processedData = OTuple.create(schema, new String[]{"ts", "program", "mode"}, new Object[]{ts++, program, mode});
                api.send(processedData);
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
