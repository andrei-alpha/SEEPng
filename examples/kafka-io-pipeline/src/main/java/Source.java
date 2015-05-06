import java.math.BigInteger;
import java.util.Random;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;


public class Source implements SeepTask {

	private Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "ts").newField(Type.INT, "key").newField(Type.STRING, "text").build();
	
	private boolean working = true;
	
	@Override
	public void setUp() {
		// TODO Auto-generated method stub
	}

	@Override
	public void processData(ITuple data, API api) {
		int ts = 0;
		Random random = new Random();
		while(working){
		    int key = 0;
		    
		    // Create 100Kb text and send it over the network
		    String text = new BigInteger(256000, random).toString(32);
		    key = random.nextInt(128);

		    //System.out.println("[Source] ts: " + ts + " text size: " + text.length() + " hash: " + text.hashCode());
			byte[] d = OTuple.create(schema, new String[]{"ts", "key", "text"}, new Object[]{ts, key, text});
			api.send(d);
			
			ts++;
			waitHere(100);
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
