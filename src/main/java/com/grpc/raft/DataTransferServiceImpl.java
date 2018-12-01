package com.grpc.raft;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.grpc.proxy.HeartbeatService;
import com.util.ConfigUtil;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import grpc.FileTransfer.ProxyInfo;
import io.grpc.stub.StreamObserver;

/**
 * Raft Server Implmentation 
 *
 */
public class DataTransferServiceImpl extends DataTransferServiceGrpc.DataTransferServiceImplBase{


	private final String KEY_DELIMINATOR= "#";
	private RaftServer server;
	private HeartbeatService heartbeat;
	final static Logger logger = Logger.getLogger(DataTransferServiceImpl.class);
	RaftClient client = null;

	public DataTransferServiceImpl(RaftServer serv){
		super();
		new ConfigUtil();
		server = serv;
		client = new RaftClient();
		heartbeat = new HeartbeatService();
	}
	/**
	 * This methods gets list of active proxies based on hearbeat between RAFT and proxy node
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void requestFileUpload(FileTransfer.FileUploadInfo request, StreamObserver<FileTransfer.ProxyList> responseObserver) {

		logger.debug("RequestFileUpload arrived...");

		List<FileTransfer.ProxyInfo> activeProxies= getLiveProxies();

		FileTransfer.ProxyList response = FileTransfer.ProxyList.newBuilder()
				.addAllLstProxy(activeProxies)
				.build();
		responseObserver.onNext(response);

		logger.debug("Finished RequestFileUpload ...");
		responseObserver.onCompleted();

	}

	/**
	 * This methods returns list of whatever files you have in your Raft hashmap
	 * @param request
	 * @param responseObserver
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	@Override
	public void listFiles(FileTransfer.RequestFileList request, StreamObserver<FileTransfer.FileList> responseObserver) {

		logger.debug("ListFiles started...");
		List<String> fileList = new ArrayList<String>();
		FileTransfer.FileList.Builder responseBuilder = grpc.FileTransfer.FileList.newBuilder();
		FileTransfer.FileList response = null;
		//If the call came from another cluster.
		if(request.getIsClient()) {
			
			logger.debug("ListFiles rpc going to all clusters...");
			client.listFilesFromOtherTeams(ConfigUtil.globalNodes, fileList, request);
		}
		//send your own files.

		Iterator<Map.Entry<String, String>> it = server.data.entrySet().iterator();
		while(it.hasNext()){

			Map.Entry<String, String> entry  = it.next();
			String[] value = entry.getKey().split(KEY_DELIMINATOR);
			fileList.add(value[0]);

		}
		response = responseBuilder.addAllLstFileNames(fileList).build();
		if(responseBuilder.getLstFileNamesCount() == 0) {
			logger.debug("No Files Founds in own  cluster!!! ");
		}

		responseObserver.onNext(response);
		logger.debug("ListFiles ended...");
		responseObserver.onCompleted();

	}

	/**
	 * This methods gets called from other team to our cluster to get files in our cluster.
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void getFileLocation(FileTransfer.FileInfo request, StreamObserver<FileTransfer.FileLocationInfo> responseObserver){

		logger.debug("GetFileLocation started...");

		List<FileTransfer.ProxyInfo> activeProxies= getLiveProxies();
		boolean isFileFound = false;
		String  maxChunks="0";
		if(server.data.get(request.getFileName()+KEY_DELIMINATOR+"0")!= null) {
			isFileFound = true;
			String value =  server.data.get(request.getFileName()+KEY_DELIMINATOR+"0");
			maxChunks = value.split("\\$")[0];
		}

		FileTransfer.FileLocationInfo response = FileTransfer.FileLocationInfo.newBuilder()
				.addAllLstProxy(activeProxies)
				.setIsFileFound(isFileFound)
				.setFileName(request.getFileName())
				.setMaxChunks(Long.parseLong(maxChunks))
				.build();

		responseObserver.onNext(response);
		logger.debug("GetFileLocation ended...");
		responseObserver.onCompleted();

	}

	/**
	 * Utility method to get live proxies
	 * @return
	 */
	private List<ProxyInfo> getLiveProxies() {

		logger.debug("Getting live proxies ...");
		List<FileTransfer.ProxyInfo> activeProxies = new ArrayList<FileTransfer.ProxyInfo>();
		boolean[] proxyStatus = heartbeat.getProxyStatus();
		for(int i =0 ; i <proxyStatus.length; i++) {
			if(proxyStatus[i]) {
				FileTransfer.ProxyInfo proxy = FileTransfer.ProxyInfo.newBuilder()
						.setIp(ConfigUtil.proxyNodes.get(i).getIP())
						.setPort(""+ConfigUtil.proxyNodes.get(i).getPort())
						.build();
				activeProxies.add(proxy);
			}		  					
		}
		logger.debug("Got "+ activeProxies.size() + "live proxies..");
		return activeProxies;
	}
	/**
	 * This methods gets list of files from within the team. If not found then calls getFileLocation from other team
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void requestFileInfo(FileTransfer.FileInfo request, StreamObserver<FileTransfer.FileLocationInfo> responseObserver){

		logger.debug("RequestFileInfo started...");
		FileTransfer.FileLocationInfo response = null;
		List<FileTransfer.ProxyInfo> activeProxies = null;
		String value = server.data.get(request.getFileName() + KEY_DELIMINATOR+"0");  
		String maxChunks = "0";
		//If not found in own team
		if(value == null) {
			logger.log(Level.INFO, "Fetching Files from other teams..");
			response = client.getFileLocationFromOtherTeam(ConfigUtil.globalNodes, request);
		}else {
			maxChunks = value.split("\\$")[0];
			activeProxies= getLiveProxies();
			logger.log(Level.INFO, "File found in own cluster");
			response = FileTransfer.FileLocationInfo.newBuilder()
					.setIsFileFound(true)
					.setFileName(request.getFileName())
					.setMaxChunks(Long.parseLong(maxChunks))
					.addAllLstProxy(activeProxies)
					.build();
		}

		// This means that file is found nowhere neither in our cluster nor in other clusters.
		if(response == null) {
			logger.log(Level.WARN, "File is not found anywhere - neither in our cluster nor in other clusters");
			response = FileTransfer.FileLocationInfo.newBuilder()
					.setIsFileFound(false)
					.setFileName(request.getFileName())
					.setMaxChunks(0)
					.build();
		}


		responseObserver.onNext(response);
		logger.debug("RequestFileInfo ended...");
		responseObserver.onCompleted();
	}

}




