package uk.ac.imperial.lsds.seepmaster.comm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.comm.protocol.BootstrapCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.DeadWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerProtocolAPI;
import uk.ac.imperial.lsds.seep.comm.serialization.KryoFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

public class MasterSchedulerCommManager {

    final private Logger LOG = LoggerFactory.getLogger(MasterWorkerCommManager.class.getName());
    
    private ServerSocket serverSocket;
    private Kryo k;
    private Thread listener;
    private boolean working = false;
    private MasterWorkerAPIImplementation api;
    
    public MasterSchedulerCommManager(int port, MasterWorkerAPIImplementation api){
        this.api = api;
        this.k = KryoFactory.buildKryoForMasterWorkerProtocol();
        try {
            serverSocket = new ServerSocket(port);
            LOG.info(" Listening for scheduler commands on {}:{}", InetAddress.getLocalHost(), port);
        } 
        catch (IOException e) {
            e.printStackTrace();
        }
        listener = new Thread(new CommMasterWorker());
        listener.setName(CommMasterWorker.class.getSimpleName());
        // TODO: set uncaughtexceptionhandler
    }
    
    public void start(){
        this.working = true;
        LOG.info("Start MasterWorkerCommManager");
        this.listener.start();
    }
    
    public void stop(){
        //TODO: do some other cleaning work here
        this.working = false;
    }
    
    class CommMasterWorker implements Runnable{

        @Override
        public void run() {
            while(working){
                Socket incomingSocket = null;
                try{
                    // Blocking call
                    incomingSocket = serverSocket.accept();
                    
                    InputStream is = incomingSocket.getInputStream();
                    
                    StringWriter writer = new StringWriter();
                    IOUtils.copy(is,  writer);
                    String input = writer.toString();
                    
                    String[] args = input.split(",");
                    
                    if (args.length == 1 && args[0].equals("stop")) {
                        api.handleStop();
                    }
                    else if (args.length == 1 && args[0].equals("exit")) {
                        api.handleExit();
                    }
                    else if (args.length > 1 && args[0].equals("migrate")) {
                        String workerDataPort = args[1];
                        String newHost = (args.length > 2 ? args[2] : null);
                        api.handleMigration(workerDataPort, newHost);
                    }
                }
                catch(IOException io){
                    io.printStackTrace();
                }
                finally {
                    if (incomingSocket != null){
                        try {
                            incomingSocket.close();
                        }
                        catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }   
        }
    }
}
