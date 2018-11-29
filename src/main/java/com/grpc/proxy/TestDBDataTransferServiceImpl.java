package com.grpc.proxy;

import org.apache.log4j.Logger;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import grpc.FileTransfer.FileInfo;
import grpc.FileTransfer.FileUploadData;
import io.grpc.stub.StreamObserver;

/**
 * Test Proxy DB class
 * @author Sricheta's computer
 *
 */
public class TestDBDataTransferServiceImpl extends DataTransferServiceGrpc.DataTransferServiceImplBase{

	final static Logger logger = Logger.getLogger(TestDBDataTransferServiceImpl.class);
	@Override
	public StreamObserver<FileTransfer.FileUploadData> uploadFile(StreamObserver<FileTransfer.FileInfo> responseObserver) {

		return new StreamObserver<FileTransfer.FileUploadData>() {

			String fileName;

			@Override
			public void onNext(FileUploadData value) {
				fileName = value.getFileName();
				System.out.println(value+"fffd");
			}

			@Override
			public void onError(Throwable t) {
				t.printStackTrace();
				
				logger.error("Error occured in uploadFile" + t.getMessage());
				responseObserver.onError(t);
			}

			@Override
			public void onCompleted() {
				responseObserver.onNext(FileInfo.newBuilder()
						.setFileName(fileName)
						.build());
				responseObserver.onCompleted();
			}



		};
	}

}
