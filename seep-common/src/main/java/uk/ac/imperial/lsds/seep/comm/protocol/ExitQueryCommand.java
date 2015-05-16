package uk.ac.imperial.lsds.seep.comm.protocol;

public class ExitQueryCommand implements CommandType {

    public ExitQueryCommand(){}
    
    @Override
    public short type() {
        return MasterWorkerProtocolAPI.EXITQUERY.type();
    }

}


