package com.grpc.proxy;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

public class ProxyDBNodeService {
	
	
	public void uploadDataToDB() {
		
		//This will have Database Port
		final ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:8080")
				.usePlaintext(true)
				.build();

		DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(channel);
		StreamObserver<FileTransfer.FileUploadData> uploadFile = stub.uploadFile(new StreamObserver<FileTransfer.FileInfo>() {
			@Override
			public void onNext(FileTransfer.FileInfo value) {
				
			}

			@Override
			public void onError(Throwable t) {

			}

			@Override
			public void onCompleted() {

			}
		});
	}

}
