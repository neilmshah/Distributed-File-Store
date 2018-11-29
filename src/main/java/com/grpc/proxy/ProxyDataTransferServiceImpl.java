package com.grpc.proxy;

import org.apache.log4j.Logger;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import grpc.FileTransfer.FileInfo;
import grpc.FileTransfer.FileUploadData;
import io.grpc.stub.StreamObserver;

/**
 * Proxy Server Implmentation
 * @author Sricheta's computer
 *
 */
public class ProxyDataTransferServiceImpl extends DataTransferServiceGrpc.DataTransferServiceImplBase {

	DBClient dbservice = new DBClient();
	final static Logger logger = Logger.getLogger(ProxyDataTransferServiceImpl.class);
	
	/**
	 * This method calls the db node to upload File chunk
	 */
	@Override
	public StreamObserver<FileTransfer.FileUploadData> uploadFile(StreamObserver<FileTransfer.FileInfo> responseObserver) {

		return new StreamObserver<FileTransfer.FileUploadData>() {

			String fileName;

			@Override
			public void onNext(FileUploadData value) {
				fileName = value.getFileName();
				dbservice.uploadDataToDB(value);
			}

			@Override
			public void onError(Throwable t) {
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
