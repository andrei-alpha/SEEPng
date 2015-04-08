package uk.ac.imperial.lsds.seepcontrib.yarn;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seepcontrib.yarn.config.YarnConfig;

public class SeepYarnAppMasterContainerManager {

    final private static Logger LOG = LoggerFactory.getLogger(SeepYarnAppMasterContainerManager.class);
    private NMClient containerManager;
    private Configuration config;
    private int numTotalContainers;
    private int numCompletedContainers;
    
    public void init(AMRMClientAsync<ContainerRequest> amClient, Configuration config) {
        this.config = config;
        numTotalContainers = Integer.parseInt(config.get(YarnConfig.YARN_TASK_COUNT));
        containerManager = NMClient.createNMClient();
        containerManager.init(config);
        containerManager.start();
        
        int containerMemory = Integer.parseInt(config.get(YarnConfig.YARN_CONTAINER_MEMORY_MB));
        int containerCores = Integer.parseInt(config.get(YarnConfig.YARN_CONTAINER_CPU_CORES));
        
        int numTotalContainersToRequest = numTotalContainers - numCompletedContainers;
        // Setup ask for containers from RM, Send request for containers to RM
        // Until we get our fully allocated quota, we keep on polling RM for containers
        // Keep looping until all the containers are launched and shell script
        // executed on them ( regardless of success/failure).
        for (int i = 0; i < numTotalContainersToRequest; ++i) {
            Resource capability = Records.newRecord(Resource.class);
            Priority priority = Records.newRecord(Priority.class);
            priority.setPriority(0);
            capability.setMemory(containerMemory);
            capability.setVirtualCores(containerCores);
            
            AMRMClient.ContainerRequest request =
                    new AMRMClient.ContainerRequest(capability, null, null, priority);
            amClient.addContainerRequest(request);
        }
    }
    
    public void onContainerAllocated(Container container) throws IOException, YarnException {
        // TODO: get system environment
        Map<String, String> env = System.getenv();
        
        Resource resource = Records.newRecord(Resource.class);
        LocalResource packageResource = Records.newRecord(LocalResource.class);
        
        // set the local package so that the containers and app master are provisioned with it
        Path packagePath = new Path(config.get(YarnConfig.YARN_WORKER_PACKAGE_PATH));
        URL packageUrl = ConverterUtils.getYarnUrlFromPath(packagePath);
        FileStatus fileStatus = packagePath.getFileSystem(config).getFileStatus(packagePath);
        
        // TODO: check if this is all we need to configure the package
        packageResource.setResource(packageUrl);
        packageResource.setSize(fileStatus.getLen());
        packageResource.setTimestamp(fileStatus.getModificationTime());
        packageResource.setType(LocalResourceType.ARCHIVE);
        packageResource.setVisibility(LocalResourceVisibility.APPLICATION);
        
        HashMap<String, LocalResource> packageResourceMap = new HashMap<String, LocalResource>();
        packageResourceMap.put("__package", packageResource);
        
        ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
        context.setEnvironment(env);
        context.setTokens(null);
        
        //TODO: set commands to run on application
        context.setCommands(null);
        context.setLocalResources(packageResourceMap);
        
        StartContainerRequest containerRequest = Records.newRecord(StartContainerRequest.class);
        containerRequest.setContainerLaunchContext(context);
        containerManager.startContainer(container, context);
    }
    
    public void onContainerCompleted(ContainerStatus status) {
        String containerId = status.getContainerId().toString();
        
        switch (status.getExitStatus()) {
            case 0: { 
                LOG.info("Container " + containerId + " completed successfully");
                numCompletedContainers += 1;
                
                if (numCompletedContainers == numTotalContainers) {
                    LOG.info("Job status is now SUCCEEDED, since all tasks have been marked as completed.");
                }
            }
            case -100: {
                LOG.info("Got an exit code of -100. This means that container " + containerId + " was killed"
                        + "by YARN, either due to being released by the application master or being 'lost'"
                        + "due to node failures etc.");
            }
            default: {
                LOG.info("Container " + containerId + " failed with exit code " + status.getExitStatus()
                        + " " + status.getDiagnostics());
                
                // TODO: implement retry logic
            }
        }
    }
    
    public float getProgress() {
        // set progress to deliver to RM on next heartbeat
        float progress = (float) numCompletedContainers / numTotalContainers;
        return progress;
    }
}
