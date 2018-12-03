package com.grpc.proxy;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.util.ConfigUtil;
import com.util.Connection;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import grpc.FileTransfer.FileInfo;
import grpc.FileTransfer.FileMetaData;
import grpc.FileTransfer.FileUploadData;
import grpc.Team.ChunkLocations;
import io.grpc.stub.StreamObserver;

/**
 * Proxy Server Implmentation
 * @author Sricheta's computer
 *
 */
public class ProxyDataTransferServiceImpl extends DataTransferServiceGrpc.DataTransferServiceImplBase {

	ProxyClient proxyClient = new ProxyClient();
	Connection ownDB = ConfigUtil.databaseNodes.get(1);
	final static Logger logger = Logger.getLogger(ProxyDataTransferServiceImpl.class);
	   
	/**
	 * Upload chunks from Client to Proxy
	 * This method calls the db node to upload File chunk
	 * 
	 * rpc UploadFile (stream FileUploadData) returns (FileInfo); 
	 */
	@Override
	public StreamObserver<FileTransfer.FileUploadData> uploadFile(StreamObserver<FileTransfer.FileInfo> responseObserver) {

		return new StreamObserver<FileTransfer.FileUploadData>() {
			
			List<Connection> successFullDbNnodes= null;
			String fileName;

			@Override
			public void onNext(FileUploadData value) {
				successFullDbNnodes = new ArrayList<Connection>();
				fileName = value.getFileName();
				//for(Connection dbNode : ConfigUtil.databaseNodes ) {
				proxyClient.uploadDataToDB(value, ownDB, successFullDbNnodes);
				logger.debug("Successfull DB Nodes -> " + successFullDbNnodes.size());
				successFullDbNnodes.add(ConfigUtil.databaseNodes.get(1));
				proxyClient.updateChunkLocations(successFullDbNnodes, ConfigUtil.raftNodes.get(0), value);
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
	
	

	/**
	 *  Download chunks from Client to Proxy
	 *  
	 * 	rpc DownloadChunk (ChunkInfo) returns (stream FileMetaData);
	 * 
	 */
	@Override
	public void downloadChunk(grpc.FileTransfer.ChunkInfo request,
	        io.grpc.stub.StreamObserver<grpc.FileTransfer.FileMetaData> responseObserver) {
		Timestamp ts1  =  new Timestamp(System.currentTimeMillis());
		ChunkLocations ch = proxyClient.GetChunkLocations(request, ConfigUtil.raftNodes.get(0));
			
		Iterator <FileMetaData> fileMetaDataList = proxyClient.downloadChunk(request, ch.getDbAddressesList());

		
		while(fileMetaDataList.hasNext()) {
			responseObserver.onNext(fileMetaDataList.next());
		}
		 responseObserver.onCompleted();
			Timestamp ts2  =  new Timestamp(System.currentTimeMillis());
			logger.debug("Method requestFileInfo ended at "+ ts2);
			logger.debug("Method downloadChunk ended at "+ ts2);
			logger.debug("Method downloadChunk execution time : "+ (ts2.getTime() - ts1.getTime()) + "ms");
	    }
}
