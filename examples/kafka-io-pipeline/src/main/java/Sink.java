import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;


public class Sink implements SeepTask {
	
	@Override
	public void setUp() {
		// TODO Auto-generated method stub
	}

	@Override
	public void processData(ITuple data, API api) {
	    int ts = data.getInt("ts");
	    int key = data.getInt("key");
        String text = data.getString("text");
		
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < text.length(); i++)
            sb.append((char)(text.charAt(i) ^ key));
        String original = sb.toString();
        
        int hash = original.hashCode();
		//System.out.println("[Sink] ts: " + ts + " text size: " + original.length() + " hash: " + hash);
	}

	@Override
	public void processDataGroup(ITuple dataBatch, API api) {
		// TODO Auto-generated method stub
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}

}
