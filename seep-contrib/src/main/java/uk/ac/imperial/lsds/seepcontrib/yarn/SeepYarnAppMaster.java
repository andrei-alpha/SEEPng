package uk.ac.imperial.lsds.seepcontrib.yarn;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.config.Config;
import uk.ac.imperial.lsds.seepcontrib.yarn.client.SeepYarnAppSubmissionClient;
import uk.ac.imperial.lsds.seepcontrib.yarn.config.YarnConfig;

import com.sun.jersey.api.model.AbstractResourceModelContext;
import com.sun.jersey.spi.container.ContainerRequest;

public class SeepYarnAppMaster implements AMRMClientAsync.CallbackHandler  {
	
    final private static Logger LOG = LoggerFactory.getLogger(SeepYarnAppMaster.class);
    private SeepYarnAppMasterContainerManager containerManager;
    
	public void init(Configuration config) {
	    AMRMClientAsync<AMRMClient.ContainerRequest> amClient;
	    //NMClientAsyncImpl nmClient;
	    
		Map<String, String> envs = System.getenv();
		String containerIdString =
		  envs.get(ApplicationConstants.Environment.CONTAINER_ID);
		if (containerIdString == null) {
		// container id should always be set in the env by the framework
		throw new IllegalArgumentException(
		    "ContainerId not set in the environment");
		}
		ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
		ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();

		// TODO: get heartbeet interval from config instead of 100 default value
		amClient = AMRMClientAsync.createAMRMClientAsync(1000, this);
		amClient.init(config);
	    amClient.start();
	    
	    // Get port, ulr information. TODO: get tracking url
	    String appMasterHostname = NetUtils.getHostname();
	    int appMasterPort = Integer.parseInt(envs.get(ApplicationConstants.Environment.NM_PORT));
	    String appMasterTrackingUrl = "";
	    // Register self with ResourceManager. This will start heartbeating to the RM
	    try {
	        RegisterApplicationMasterResponse response = amClient
	                .registerApplicationMaster(appMasterHostname, appMasterPort, appMasterTrackingUrl);
	    
            // Dump out information about cluster capability as seen by the
            // resource manager
            int maxMem = response.getMaximumResourceCapability().getMemory();
            LOG.info("Max mem capabililty of resources in this cluster " + maxMem);
        
            int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
            LOG.info("Max vcores capabililty of resources in this cluster " + maxVCores);
        
            int containerMemory = Integer.parseInt(config.get(YarnConfig.YARN_CONTAINER_MEMORY_MB));
            int containerCores = Integer.parseInt(config.get(YarnConfig.YARN_CONTAINER_CPU_CORES));
            
            // A resource ask cannot exceed the max.
            if (containerMemory > maxMem) {
              LOG.info("Container memory specified above max threshold of cluster."
                  + " Using max value." + ", specified=" + containerMemory + ", max="
                  + maxMem);
              containerMemory = maxMem;
            }
        
            if (containerCores > maxVCores) {
              LOG.info("Container virtual cores specified above max threshold of  cluster."
                + " Using max value." + ", specified=" + containerCores + ", max=" + maxVCores);
              containerCores = maxVCores;
            }
            List<Container> previousAMRunningContainers = response.getContainersFromPreviousAttempts();
            LOG.info("Received " + previousAMRunningContainers.size()
                    + " previous AM's running containers on AM registration.");
            
            // Start ContainerManager to manage Container Allocation Requests
            containerManager = new SeepYarnAppMasterContainerManager();
            containerManager.init(amClient, config);
            
	    } catch (IOException | YarnException e) {
	        e.printStackTrace();
	    }
	}

	@Override
	public void onContainersCompleted(List<ContainerStatus> statuses) {
	    LOG.info("Got response from RM for container completed, completedCnt=" + statuses.size());
        //numAllocatedContainers.addAndGet(containers.size());
        for (ContainerStatus status : statuses) {
            containerManager.onContainerCompleted(status);
        }
	}

	@Override
	public void onContainersAllocated(List<Container> containers) {
	    LOG.info("Got response from RM for container ask, allocatedCnt=" + containers.size());
        //numAllocatedContainers.addAndGet(containers.size());
        for (Container allocatedContainer : containers) {
            try {
                containerManager.onContainerAllocated(allocatedContainer);
            } catch (IOException | YarnException e) {
                e.printStackTrace();
            }
        }
	}

	@Override
	public void onShutdownRequest() {
		// TODO Auto-generated method stub
	}

	@Override
	public void onNodesUpdated(List<NodeReport> updatedNodes) {
		// TODO Auto-generated method stub
	}

	@Override
	public float getProgress() {
	    return containerManager.getProgress();
	}

	@Override
	public void onError(Throwable e) {
		LOG.error(e.getMessage());
	}
	
}
