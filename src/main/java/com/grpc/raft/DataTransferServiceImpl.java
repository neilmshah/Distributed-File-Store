package com.grpc.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;

import com.grpc.proxy.HeartbeatService;
import com.util.ConfigUtil;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import io.grpc.stub.StreamObserver;

/**
 * Raft Server Implmentation 
 *
 */
public class DataTransferServiceImpl extends DataTransferServiceGrpc.DataTransferServiceImplBase{


	private RaftServer server;
	private HeartbeatService heartbeat;
	final static Logger logger = Logger.getLogger(DataTransferServiceImpl.class);

	public DataTransferServiceImpl(RaftServer serv){
		super();
		new ConfigUtil();
		server = serv;

	}
	/**
	 * This methods gets list of active proxies based on hearbeat between RAFT and proxy node
	 * @param request
	 * @param responseObserver
	 */
	public void  RequestFileUpload(FileTransfer.FileUploadInfo request, StreamObserver<FileTransfer.ProxyList> responseObserver) {
		
		logger.debug("Inside RequestFileUpload ...");
		List<FileTransfer.ProxyInfo> activeProxies = new ArrayList<FileTransfer.ProxyInfo>();
		logger.debug("Getting live proxies ...");
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
		FileTransfer.ProxyList response = FileTransfer.ProxyList.newBuilder()
				.addAllLstProxy(activeProxies)
				.build();
		responseObserver.onNext(response);
		logger.debug("Finished RequestFileUpload ...");
		responseObserver.onCompleted();

	}

	/**
	 * This methods gets list of files from the other team cluster
	 * @param request
	 * @param responseObserver
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public void ListFiles(FileTransfer.RequestFileList request, StreamObserver<FileTransfer.FileList> responseObserver) {

		//TODO waiting for Vishnu to know the structure of the HashMap.
		
		/*
		Iterator<Map.Entry<String, Object>> it = data.entrySet().iterator();
		List<String> list = new ArrayList<String>();

		FileTransfer.FileList.Builder responseBuilder = grpc.FileTransfer.FileList.newBuilder();
		int count =0;
		while(it.hasNext()){

			Map.Entry<String, Object> entry  = it.next();
			String[] value = ((String)entry.getValue()).split(",");
			responseBuilder.setLstFileNames(count, value[1]);
			count++;
			list.add(value[1]);
		}


		FileTransfer.FileList response = responseBuilder.build();
		responseObserver.onNext(response);
		responseObserver.onCompleted(); */

	}
	
	/**
	 * This methods gets called from other team to our cluster to get files in our cluster.
	 * @param request
	 * @param responseObserver
	 */
	public void GetFileLocation(FileTransfer.FileInfo request, StreamObserver<FileTransfer.FileLocationInfo> responseObserver){
		
		//TODO waiting for Vishnu to return the list of live proxies
		
		
		
	}
	
	/**
	 * This methods gets list of files from within the team. If not found then calls getFileLocation from other team
	 * @param request
	 * @param responseObserver
	 */
	public void  RequestFileInfo(FileTransfer.FileInfo request, StreamObserver<FileTransfer.FileLocationInfo> responseObserver){
		
	     FileTransfer.FileLocationInfo response = null;
		 ArrayList<FileTransfer.ProxyInfo> proxyList = new ArrayList<FileTransfer.ProxyInfo>();
		 //TODO
		// String value = (String) data.get(request.getFileName() + "_0");  change this. get this from the hashmap.
		 String value = "";
		 RaftClient client =new RaftClient();
		 
		 //If not found in own team
		 if(value == null) {
			 response = client.getFileFromOtherTeam(ConfigUtil.globalNodes, request);
		 }else {
			 
			 // TODO
			 // waiting for Vishnu to return the list of live proxies
			 
		 }
		 
		 if(response == null) {
			 response = FileTransfer.FileLocationInfo.newBuilder()
				      .setIsFileFound(false)
				      .setFileName(request.getFileName())
				      .setMaxChunks(0)
				      .build();
		 }
	}
	
}




