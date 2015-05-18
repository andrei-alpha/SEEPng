package uk.ac.imperial.lsds.seepmaster.comm;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterSchedulerCommManager {

    final private Logger LOG = LoggerFactory.getLogger(MasterWorkerCommManager.class.getName());
    
    private ServerSocket serverSocket;
    private Thread listener;
    private boolean working = false;
    private MasterWorkerAPIImplementation api;
    
    public MasterSchedulerCommManager(int port, MasterWorkerAPIImplementation api){
        this.api = api;
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
                    
                    Integer dataPort = null;
                    if (args.length > 1) {
                        try {
                            dataPort = Integer.parseInt(args[1]);
                        } catch (NumberFormatException e) {
                            LOG.error(e.getMessage());
                        }
                    }
                    
                    if (args.length >= 1 && args[0].equals("stop")) {
                        api.handleStop(dataPort);
                    }
                    else if (args.length >= 1 && args[0].equals("start")) {
                        api.handleStart(dataPort);
                    }
                    else if (args.length >= 1 && args[0].equals("exit")) {
                        api.handleExit(dataPort);
                    }
                    else if (args.length > 1 && args[0].equals("migrate")) {
                        if (dataPort == null)
                            continue;
                        
                        String newHost = (args.length > 2 ? args[2] : null);
                        api.handleMigration(dataPort, newHost);
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
