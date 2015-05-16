package uk.ac.imperial.lsds.seepmaster;

import java.lang.reflect.Method;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterShutdownHookWorker implements Runnable {

	final private static Logger LOG = LoggerFactory.getLogger(MasterShutdownHookWorker.class);
	private ArrayList<Object> tasks;
	
	public MasterShutdownHookWorker() {
        tasks = new ArrayList<>();
    }
	
	@Override
	public void run() {
		LOG.info("JVM is shutting down...");
		LOG.info("Closing Master...");
		
		// Close all the tasks that are running
		for (Object task : tasks) {
		    Method method;
		    try {
                method = task.getClass().getMethod("stop");
                method.invoke(task);
            } catch (Exception e) {
                e.printStackTrace();
            }
		}
		
		LOG.info("Closing Master... OK");
		Runtime.getRuntime().halt(0);
	}

	public void addTask(Object task) {
	    tasks.add(task);
	}
}
