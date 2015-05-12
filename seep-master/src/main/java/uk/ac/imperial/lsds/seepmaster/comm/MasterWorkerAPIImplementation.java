package uk.ac.imperial.lsds.seepmaster.comm;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.comm.protocol.DeadWorkerCommand;
import uk.ac.imperial.lsds.seep.infrastructure.ExecutionUnit;
import uk.ac.imperial.lsds.seep.infrastructure.InfrastructureManager;
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
	
}
