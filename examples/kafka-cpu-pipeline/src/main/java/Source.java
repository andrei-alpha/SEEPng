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

	private Schema schema = SchemaBuilder.getInstance().newField(Type.LONG, "ts").newField(Type.LONG, "pubE")
            .newField(Type.LONG, "pubModulus").newField(Type.LONG, "secret").build();
	
	private boolean working = true;
	
	@Override
	public void setUp() {
		// TODO Auto-generated method stub
	}

	private static boolean isPrime(int x) {
        if (x == 1)
            return true;
        for (int i = 2; i * i <= x; ++i)
            if (x % i == 0)
                return false;
        return true;
    }
    
    private static int getRandomPrime(int min, int max) {
        Random rand = new Random();
        
        int x = rand.nextInt(max - min) + min;
        while (!isPrime(x))
            x = rand.nextInt(max - min) + min;
        return x;
    }
    
    private static long modPow(long x, long p, long mod) {
        long res = 1;
        long a = x;
        while(p > 0){
            if(p % 2 == 1){
                res = (res * a) % mod;
            }
            a = (a * a) % mod;
            p /= 2;
        }
        if (res < 0)
            res += mod;
        return res;
    }
	
	@Override
	public void processData(ITuple data, API api) {
		long ts = 0;
		while(working){
		    // RSA
		    long p = getRandomPrime(20000, 35000);
	        long q = getRandomPrime(35000, 50000);
	        long N = p * q;
	        long e;
	        
	        e = 2;
	        long phi = (p - 1) * (q - 1);
	        while (BigInteger.valueOf(e).gcd(BigInteger.valueOf(phi)).intValue() != 1)
	            ++e;

	        Random rand = new Random();
	        long x = rand.nextInt((int)N);
	        long ex = modPow(x, e, N);
		    
	        System.out.printf("RSA: p=%d q=%d e=%d d=?\n x=%d", (int)p, (int)q, (int)e, (int)x);
	        System.out.println("[Source] ts: " + ts + " original text: " + x);
	        
			byte[] output = OTuple.create(schema, new String[]{"ts", "pubE", "pubModulus", "secret"}, new Object[]{ts, e, N, ex});
			api.send(output);
			
			ts++;
			waitHere(1000);
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
