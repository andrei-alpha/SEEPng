import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;

public class Processor implements SeepTask {

    private Schema schema = SchemaBuilder.getInstance().newField(Type.LONG, "ts").newField(Type.STRING, "text").build();
    private HashMap<String, Integer> wordCount;
    private HashSet<String> ignoredWords = new HashSet<String>(Arrays.asList("RT", "is", "ok", "the", "and", "for"));
    private long timestamp;
    
	@Override
	public void setUp() {
		timestamp = System.currentTimeMillis();
		wordCount = new HashMap<>();
	}
	
	@Override
	public void processData(ITuple data, API api) {
		String text = data.getString("text");
		
		for (String word : text.split(" ")) {
		    if (word.length() > 2 && !ignoredWords.contains(word)) {
		        Integer prevCnt = wordCount.get(word);
		        wordCount.put(word, (prevCnt == null ? 0 : prevCnt) + 1);
		    }
		    //byte[] processedData = OTuple.create(schema, new String[]{"ts", "text"}, new Object[]{timestamp, word});
            //api.send(processedData);
		}
		
		if (System.currentTimeMillis() - timestamp > 10000) {
		    String mostFrequentWord = null;
		    long count = -1;
		    for (Entry<String, Integer> entry : wordCount.entrySet()) {
		        if (mostFrequentWord == null || entry.getValue() > count) {
		            mostFrequentWord = entry.getKey();
		            count = entry.getValue();
		        }
		    }
		    ignoredWords.add(mostFrequentWord);
		    wordCount.clear();
		
		    byte[] processedData = OTuple.create(schema, new String[]{"ts", "text"}, new Object[]{count, mostFrequentWord});
		    api.send(processedData);
		    timestamp = System.currentTimeMillis();
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
