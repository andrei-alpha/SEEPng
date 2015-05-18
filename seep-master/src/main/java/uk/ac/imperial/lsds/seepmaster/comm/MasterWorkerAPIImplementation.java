package uk.ac.imperial.lsds.seepmaster.comm;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.comm.protocol.DeadWorkerCommand;
import uk.ac.imperial.lsds.seep.infrastructure.ExecutionUnit;
import uk.ac.imperial.lsds.seep.infrastructure.InfrastructureManager;
import uk.ac.imperial.lsds.seepcontrib.yarn.infrastructure.YarnClusterManager;
import uk.ac.imperial.lsds.seepmaster.query.QueryManager;


public class MasterWorkerAPIImplementation {

	final private Logger LOG = LoggerFactory.getLogger(MasterWorkerAPIImplementation.class.getName());
	
	private QueryManager qm;
	private InfrastructureManager inf;
	
	public MasterWorkerAPIImplementation(QueryManager qm, InfrastructureManager inf) {
		this.qm = qm;
		this.inf = inf;
	}
	
	public void bootstrapCommand(uk.ac.imperial.lsds.seep.comm.protocol.BootstrapCommand bc){
		InetAddress bootIp = null;
		try {
			String ipStr = bc.getIp();
			bootIp = InetAddress.getByName(ipStr);
		} 
		catch (UnknownHostException e) {
			e.printStackTrace();
		}
		int port = bc.getPort();
		int dataPort = bc.getDataPort();
		LOG.info("New worker node in {}:{}, dataPort: {}", bootIp.toString(), port, dataPort);
		ExecutionUnit eu = inf.buildExecutionUnit(bootIp, port, dataPort);
		inf.addExecutionUnit(eu);
		
		qm.newNodeDiscovered();
	}
	
	public void handleDeadWorker(DeadWorkerCommand dwc){
		int workerId = dwc.getWorkerId();
		String reason = dwc.reason();
		LOG.warn("Worker {} has died, reason: {}", workerId, reason);
		inf.removeExecutionUnit(workerId);
		
		qm.newDeadWorker(workerId);
	}

	public void handleMigration(Integer dataPort, String newHost) {
	    if (inf instanceof YarnClusterManager) {
	        handleStop(dataPort);
	        handleExit(dataPort);
	        
	        ((YarnClusterManager) inf).markPortAsAvailable(dataPort);
	        if (newHost != null) {
	            ((YarnClusterManager) inf).addHostForNextAllocation(newHost);
	        }
	        
	        ((YarnClusterManager) inf).requestContainer();
	        
	    } else {
	        LOG.warn("Migration is currently suporrted only for YARN deployments.");
	    }
	}
	
	public void handleStop(Integer dataPort) {
	    if (dataPort == null) {
	        LOG.info("Stopping query...");
	        boolean allowed = qm.stopQuery();
	        if(!allowed){
	            LOG.warn("Could not stop query");
	        }
	        else{
	            LOG.info("Stopping query...OK");
	        }
	    } else if (inf instanceof YarnClusterManager) {
	        Integer euId = ((YarnClusterManager) inf).getExecutionUnitIdFromDataPort(dataPort);
            if (euId == null) {
                LOG.error("No worker found running on dataPort {}", dataPort);
                return;
            }
            
            LOG.info("Stopping worker running on dataPort {} ...", dataPort);
            if (!qm.stopNode(euId)) {
                LOG.error("Failed to request worker to stop execution.");
                return;
            }
            LOG.info("Stopping worker OK", dataPort);
	    } else {
	        LOG.warn("Operator scheduling is currently suporrted only for YARN deployments.");
	    }
	}
	
	public void handleStart(Integer dataPort) {
	    if (dataPort == null) {
            LOG.info("Starting query...");
            boolean allowed = qm.startQuery();
            if(!allowed){
                LOG.warn("Could not start query");
            }
            else{
                LOG.info("Starting query...OK");
            }
        } else if (inf instanceof YarnClusterManager) {
            Integer euId = ((YarnClusterManager) inf).getExecutionUnitIdFromDataPort(dataPort);
            if (euId == null) {
                LOG.error("No worker found running on dataPort {}", dataPort);
                return;
            }
            
            LOG.info("Starting worker running on dataPort {} ...", dataPort);
            if (!qm.startNode(euId)) {
                LOG.error("Failed to request worker to start execution.");
                return;
            }
            LOG.info("Starting worker OK", dataPort);
        } else {
            LOG.warn("Operator scheduling is currently suporrted only for YARN deployments.");
        }
	}
	
	public void handleExit(Integer dataPort) {
	    if (dataPort == null) {
	        LOG.info("Exit query...");
	        boolean allowed = qm.exitQuery();
	        if(!allowed){
	            LOG.warn("Could not exit query");
	        }
	        else{
	            LOG.info("Exit query OK");
	            System.exit(0);
	        }
	    } else if (inf instanceof YarnClusterManager) {
	        Integer euId = ((YarnClusterManager) inf).getExecutionUnitIdFromDataPort(dataPort);
            if (euId == null) {
                LOG.error("No worker found running on dataPort {}", dataPort);
                return;
            }
            
            LOG.info("Exiting worker running on dataPort {} ...", dataPort);
            if (!qm.exitNode(euId)) {
                LOG.error("Failed to request worker to exit.");
                return;
            }
            LOG.info("Exiting worker OK", dataPort);
        } else {
            LOG.warn("Operator scheduling is currently suporrted only for YARN deployments.");
        }
	}
}
