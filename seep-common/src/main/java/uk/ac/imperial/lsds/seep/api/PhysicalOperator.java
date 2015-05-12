package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;

public interface PhysicalOperator extends Operator{
	
	public int getIdOfWrappingExecutionUnit();
	public EndPoint getWrappingEndPoint();
	public void replaceWrappingEndPoint(EndPoint ep);
	public String toString();
	
}