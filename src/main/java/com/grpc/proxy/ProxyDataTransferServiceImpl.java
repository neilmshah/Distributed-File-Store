package com.grpc.proxy;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import grpc.FileTransfer.FileUploadData;
import io.grpc.stub.StreamObserver;

public class ProxyDataTransferServiceImpl extends DataTransferServiceGrpc.DataTransferServiceImplBase {
	
	ProxyDBNodeService dbservice = new ProxyDBNodeService();
			
	@Override
	public StreamObserver<FileTransfer.FileUploadData> uploadFile(StreamObserver<FileTransfer.FileInfo> responseObserver) {
		
		return new StreamObserver<FileTransfer.FileUploadData>() {

			@Override
			public void onNext(FileUploadData value) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void onError(Throwable t) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void onCompleted() {
				// TODO Auto-generated method stub
				
				dbservice.uploadDataToDB();
				
			}
			
			
			
		};
	}
}
