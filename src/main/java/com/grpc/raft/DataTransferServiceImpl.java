package com.grpc.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

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


	private ConcurrentHashMap data;
	private HeartbeatService heartbeat;

	public DataTransferServiceImpl(ConcurrentHashMap map){
		data = map;
	}

	/**
	 * This methods gets list of active proxies based on hearbeat between RAFT and proxy node
	 * @param request
	 * @param responseObserver
	 */
	public void  RequestFileUpload(FileTransfer.FileUploadInfo request, StreamObserver<FileTransfer.ProxyList> responseObserver) {
		
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
		
		FileTransfer.ProxyList response = FileTransfer.ProxyList.newBuilder()
				.addAllLstProxy(activeProxies)
				.build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
		
	}
	
	
	
}
