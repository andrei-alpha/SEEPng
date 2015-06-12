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

    private Schema schema = SchemaBuilder.getInstance().newField(Type.LONG, "ts").newField(Type.STRING, "program").newField(Type.STRING, "mode").build();
    
	@Override
	public void setUp() {
	    // TODO
	}
	
	@Override
	public void processData(ITuple data, API api) {
		String program = data.getString("program");
		String mode = data.getString("mode");
		
		try {
		    String result = srt.tool.API.verify(program, mode, 5, 50, false);
		    byte[] processedData = OTuple.create(schema, new String[]{"ts", "program", "mode"}, new Object[]{program, result});
	        api.send(processedData);
		} catch (Exception e) {
		    // pass
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
