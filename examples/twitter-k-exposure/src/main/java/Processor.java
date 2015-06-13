import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;

public class Processor implements SeepTask {

    private Schema schema = SchemaBuilder.getInstance().newField(Type.LONG, "ts").newField(Type.STRING, "text").newField(Type.STRING, "user").build();
    private HashMap<String, HashSet<String> > neighbours, posters;
    private long timestamp;
    
	@Override
	public void setUp() {
		timestamp = System.currentTimeMillis();
		neighbours = new HashMap<String, HashSet<String>>();
		posters = new HashMap<String, HashSet<String>>();
	}
	
	public static long getIntersection(Set<String> set1, Set<String> set2) {
	    boolean set1IsLarger = set1.size() > set2.size();
	    Set<String> cloneSet = new HashSet<String>(set1IsLarger ? set2 : set1);
	    cloneSet.retainAll(set1IsLarger ? set1 : set2);
	    return cloneSet.size();
	}
	
	@Override
	public void processData(ITuple data, API api) {
		String text = data.getString("text");
		String user = data.getString("user");
		
		for (String word : text.split(" ")) {
		    if (word.endsWith(":"))
		        word = word.substring(0, word.length() - 1);
		    if (word.startsWith("@")) {
		        word = word.substring(1, word.length());
		        HashSet<String> nodes;
		        if (!neighbours.containsKey(user))
		            nodes = new HashSet<String>();
		        else
		            nodes = neighbours.get(user);
		        nodes.add(word);
		        neighbours.put(user, nodes);
		        
		        HashSet<String> users;
		        if (!posters.containsKey(word))
		            users = new HashSet<String>();
		        else
		            users = posters.get(word);
		        users.add(user);
		        posters.put(word, users);

		        long count = getIntersection(neighbours.get(user), posters.get(word));
		        if (count > 4) {
		            byte[] processedData = OTuple.create(schema, new String[]{"ts", "text", "user"}, new Object[]{count, word, ""});
		            api.send(processedData);
		        }
		    }
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
