import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Random;
import java.util.Scanner;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;


public class Source implements SeepTask {

	private Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "ts").newField(Type.STRING, "text").build();
	
	private boolean working = true;
	
	@Override
	public void setUp() {
		// TODO Auto-generated method stub
	}

	@Override
	public void processData(ITuple data, API api) {
		int ts = 0;
		
		while(working) {
		    try {
		        FileInputStream inputStream = new FileInputStream("/home/aba111/iotest/data");
		        Scanner sc = new Scanner(inputStream);
		        while (sc.hasNextLine()) {
		            byte[] d = OTuple.create(schema, new String[]{"ts", "text"}, new Object[]{++ts, sc.nextLine()});
		            api.send(d);
		            
		        }
		        inputStream.close();
		        sc.close();
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
