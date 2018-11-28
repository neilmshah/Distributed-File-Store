package com.grpc.raft;

import java.util.ArrayList;
import java.util.List;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import io.grpc.stub.StreamObserver;

public class DataTransferServiceImpl extends DataTransferServiceGrpc.DataTransferServiceImplBase{
	
	public void  RequestFileUpload(FileTransfer.FileUploadInfo request, StreamObserver<FileTransfer.ProxyList> responseObserver) {
		
		List<FileTransfer.ProxyInfo> activeProxies = new ArrayList<FileTransfer.ProxyInfo>();
		FileTransfer.ProxyList response = FileTransfer.ProxyList.newBuilder()
				.addAllLstProxy(activeProxies)
				.build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
		
	}
}
