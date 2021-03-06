package uk.ac.imperial.lsds.seepmaster;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;

import joptsimple.OptionParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.IOComm;
import uk.ac.imperial.lsds.seep.comm.serialization.JavaSerializer;
import uk.ac.imperial.lsds.seep.config.CommandLineArgs;
import uk.ac.imperial.lsds.seep.config.ConfigKey;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.InfrastructureManager;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepcontrib.yarn.config.YarnConfig;
import uk.ac.imperial.lsds.seepmaster.comm.MasterSchedulerCommManager;
import uk.ac.imperial.lsds.seepmaster.comm.MasterWorkerAPIImplementation;
import uk.ac.imperial.lsds.seepmaster.comm.MasterWorkerCommManager;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManagerFactory;
import uk.ac.imperial.lsds.seepmaster.query.InvalidLifecycleStatusException;
import uk.ac.imperial.lsds.seepmaster.query.QueryManager;
import uk.ac.imperial.lsds.seepmaster.ui.UI;
import uk.ac.imperial.lsds.seepmaster.ui.UIFactory;


public class Main {
	
	final private static Logger LOG = LoggerFactory.getLogger(Main.class);

	private void executeMaster(String[] args, MasterConfig mc, YarnConfig yc, String[] queryArgs, MasterShutdownHookWorker hook) {
		int infType = mc.getInt(MasterConfig.DEPLOYMENT_TARGET_TYPE);
		LOG.info("Deploy target of type: {}", InfrastructureManagerFactory.nameInfrastructureManagerWithType(infType));
		InfrastructureManager inf = InfrastructureManagerFactory.createInfrastructureManager(infType);
		inf.init(yc);
		
		LifecycleManager lifeManager = LifecycleManager.getInstance();
		// TODO: get file from config if exists and parse it to get a map from operator to endPoint
		Map<Integer, EndPoint> mapOperatorToEndPoint = null;
		// TODO: from properties get serializer and type of thread pool and resources assigned to it
		Comm comm = new IOComm(new JavaSerializer(), Executors.newCachedThreadPool());
		QueryManager qm = QueryManager.getInstance(inf, mapOperatorToEndPoint, comm, lifeManager);
		// TODO: put this in the config manager
		int port = mc.getInt(MasterConfig.LISTENING_PORT);
		int schedulerPort = mc.getInt(MasterConfig.SCHEDULER_LISTENING_PORT);
		MasterWorkerAPIImplementation api = new MasterWorkerAPIImplementation(qm, inf);
		MasterWorkerCommManager mwcm = new MasterWorkerCommManager(port, api);
		MasterSchedulerCommManager mscm = new MasterSchedulerCommManager(schedulerPort, api);
		mwcm.start();
		mscm.start();
		
		int uiType = mc.getInt(MasterConfig.UI_TYPE);
		UI ui = UIFactory.createUI(uiType, qm, inf);
		
		// Add bookkeping tasks that need to be closed before exiting
		hook.addTask(mscm);
        hook.addTask(mscm);
        hook.addTask(ui);
		
		String queryPathFile = null;
		String baseClass = null;
		// TODO: find a more appropriate way of checking whether a property is defined in config
		if(! mc.getString(MasterConfig.QUERY_FILE).equals("") && (! mc.getString(MasterConfig.BASECLASS_NAME).equals(""))){
			queryPathFile = mc.getString(MasterConfig.QUERY_FILE);
			baseClass = mc.getString(MasterConfig.BASECLASS_NAME);
			LOG.info("Loading query {} with baseClass: {} from file...", queryPathFile, baseClass);
			boolean success = qm.loadQueryFromFile(queryPathFile, baseClass, queryArgs);
			if(! success){
				throw new InvalidLifecycleStatusException("Could not load query due to attempt to violate app lifecycle");
			}
			LOG.info("Loading query...OK");
		
			if (mc.getBoolean(MasterConfig.AUTO_DEPLOYMENT) == true) {
				// Wait for worker nodes
				while (!qm.canStartExecution()) {
					LOG.info("Waiting for workers...");
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
				success = qm.deployQueryToNodes();
				LOG.info("Deploying query to nodes...");
				if (!success) {
					LOG.warn("Could not deploy query");
				} else {
					LOG.info("Deploying query to nodes...OK");
					
					LOG.info("Starting query...");
					success = qm.startQuery();
					if(!success){
						LOG.warn("Could not start query");
					} else {
						LOG.info("Starting query...OK");
					}
				}
			}
		}
		
		if (mc.getInt(MasterConfig.DEPLOYMENT_TARGET_TYPE) != 1) {
		    LOG.info("Created UI of type: {}", UIFactory.nameUIOfType(uiType));
		    ui.start();
		} else {
		    try {
		        while (true)
		            Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
		}
	}
	
	public static void main(String args[]){
		// Register JVM shutdown hook
		MasterShutdownHookWorker hook = registerShutdownHook();
		// Get Properties with command line configuration 
		List<ConfigKey> configKeys = MasterConfig.getAllConfigKey();
		configKeys.addAll(YarnConfig.getAllConfigKey());
		OptionParser parser = new OptionParser();
		// Unrecognized options are passed through to the query
		parser.allowsUnrecognizedOptions();
		CommandLineArgs cla = new CommandLineArgs(args, parser, configKeys);
		Properties commandLineProperties = cla.getProperties();
		
		// Get Properties with file configuration
		Properties fileProperties = Utils.readPropertiesFromFile(MasterConfig.PROPERTIES_FILE, MasterConfig.PROPERTIES_RESOURCE_FILE);
		
		// Merge both properties, command line has preference
		Properties validatedProperties = Utils.overwriteSecondPropertiesWithFirst(commandLineProperties, fileProperties);
		boolean validates = validateProperties(validatedProperties);		
		if(!validates){
			printHelp(parser);
			System.exit(0);
		}
		
		MasterConfig mc = new MasterConfig(validatedProperties);
		YarnConfig yc = new YarnConfig(validatedProperties);
		
		// Any other infrastructure calls executeMaster
		Main instance = new Main();
		instance.executeMaster(args, mc, yc, cla.getQueryArgs(), hook);
	}
	
	private static boolean validateProperties(Properties validatedProperties){	
		return true;
	}
	
	private static void printHelp(OptionParser parser){
		try {
			parser.printHelpOn(System.out);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static MasterShutdownHookWorker registerShutdownHook(){
	    MasterShutdownHookWorker shutdownHook = new MasterShutdownHookWorker();
		Thread hook = new Thread(shutdownHook);
		Runtime.getRuntime().addShutdownHook(hook);
		return shutdownHook;
	}
}
