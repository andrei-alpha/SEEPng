import java.math.BigInteger;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;

public class Processor implements SeepTask {

    private Schema schema = SchemaBuilder.getInstance().newField(Type.LONG, "ts").newField(Type.LONG, "pubE")
            .newField(Type.LONG, "pubModulus").newField(Type.LONG, "secret").build();
	
	@Override
	public void setUp() {
		// TODO Auto-generated method stub
	}

	private static long inverse(long x, long m) {
        BigInteger b1 = new BigInteger(String.valueOf(x));
        BigInteger b2 = new BigInteger(String.valueOf(m));
        BigInteger b3 = b1.modInverse(b2);

        return b3.longValue();
    }
	
	private static long modPow(long x, long p, long mod) {
	    BigInteger b1 = new BigInteger(String.valueOf(x));
	    BigInteger b2 = new BigInteger(String.valueOf(p));
	    BigInteger b3 = new BigInteger(String.valueOf(mod));
	    
	    return b1.modPow(b2, b3).longValue();
    }
	
	@Override
	public void processData(ITuple data, API api) {
		long ts = data.getLong("ts");
		long e = data.getLong("pubE");
		long N = data.getLong("pubModulus");
		long ex = data.getLong("secret");
		
		System.out.println("[Processor] ts: " + ts + " crypted text: " + ex);
		
		long p = 2, q = 1;
        while(N % p != 0)
            ++p;
        q = N / p;

        //System.out.printf("RSA: p=%d q=%d e=%d d=?\n x=%d", (int)p, (int)q, (int)e, -1);    
        
        long d = inverse(e, (p - 1) * (q - 1));
        long dx = modPow(ex, d, N);
		
		byte[] processedData = OTuple.create(schema, new String[]{"ts", "pubE", "pubModulus", "secret"}, new Object[]{ts, e, N, dx});
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
