package uk.ac.imperial.lsds.seep.infrastructure;

import java.net.InetAddress;
import java.util.Set;

import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.config.Config;

public interface InfrastructureManager {
	
    public void init(Config config);
    
	public ExecutionUnit buildExecutionUnit(InetAddress ip, int port, int dataPort);
	
	public void addExecutionUnit(ExecutionUnit eu);
	public ExecutionUnit getExecutionUnit();
	public boolean removeExecutionUnit(int id);
	public int executionUnitsAvailable();
	
	public void claimExecutionUnits(int numExecutionUnits);
	public void decommisionExecutionUnits(int numExecutionUnits);
	public void decommisionExecutionUnit(ExecutionUnit node);
	
	public Set<Connection> getConnectionsTo(Set<Integer> executionUnitIds);
	public Connection getConnectionTo(int executionUnitId);
	
}
