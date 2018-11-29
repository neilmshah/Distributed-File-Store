package com.grpc.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import io.grpc.stub.StreamObserver;

public class DataTransferServiceImpl extends DataTransferServiceGrpc.DataTransferServiceImplBase{

	private ConcurrentHashMap data;

	public DataTransferServiceImpl(ConcurrentHashMap map){
		data = map;
	}

	public void  RequestFileUpload(FileTransfer.FileUploadInfo request, StreamObserver<FileTransfer.ProxyList> responseObserver) {
		
		List<FileTransfer.ProxyInfo> activeProxies = new ArrayList<FileTransfer.ProxyInfo>();
		//TODO Heartbeat between 
		FileTransfer.ProxyList response = FileTransfer.ProxyList.newBuilder()
				.addAllLstProxy(activeProxies)
				.build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
		
	}
	
	
	
}
