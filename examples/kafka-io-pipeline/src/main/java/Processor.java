import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;

public class Processor implements SeepTask {

    private Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "ts").newField(Type.INT, "key").newField(Type.STRING, "text").build();
	
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
		
		byte[] processedData = OTuple.create(schema, new String[]{"ts", "key", "text"},  new Object[]{ts, key, sb.toString()});
		api.send(processedData);
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