import java.util.HashMap;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;


public class Sink implements SeepTask {
	private HashMap<String, Integer> map;
    
	@Override
	public void setUp() {
	    map = new HashMap<String, Integer>();
	}

	@Override
	public void processData(ITuple data, API api) {
        String text = data.getString("text");

        if (map.containsKey(text)) {
            Integer count = map.get(text);
            map.put(text, count + 1);
        } else {
            map.put(text, 1);
        }
        
        if (map.size() > 50000) {
            map.clear();
        }
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
