import java.math.BigInteger;
import java.util.Random;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;


public class Sink implements SeepTask {
    private String pattern;
    private int wordCount = 0;
    
	@Override
	public void setUp() {
	    Random random = new Random();
	    pattern = new BigInteger(500, random).toString(32);
	}

	@Override
	public void processData(ITuple data, API api) {
        String text = data.getString("text");
        
        if (text.equals(pattern))
            ++wordCount;
	}

	@Override
	public void processDataGroup(ITuple dataBatch, API api) {
		// TODO Auto-generated method stub
	}

	@Override
	public void close() {
		System.out.println("WordCount: " + wordCount);
	}

}
