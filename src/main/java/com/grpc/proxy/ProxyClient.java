package com.grpc.proxy;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.lang.Math;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.util.Connection;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import grpc.FileTransfer.ChunkInfo;
import grpc.FileTransfer.FileMetaData;
import grpc.FileTransfer.FileUploadData;
import grpc.Team.ChunkLocations;
import grpc.Team.FileData;
import grpc.Team;
import grpc.TeamClusterServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * This class acts a Client to DB Server and Raft Server
 * @author Sricheta's computer
 *
 */
public class ProxyClient {

	final static Logger logger = Logger.getLogger(ProxyClient.class);
	
	/**
	 * This method calls the db node to upload File chunk
	 * 
	 * rpc UploadFile (stream FileUploadData) returns (FileInfo); 
	 * 
	 * TODO Update target address with Db Server IP
	 * 
	 * @param dbNode 
	 * @param successFullDbNnodes 
	 */
	public void uploadDataToDB(FileUploadData fileUploadData, Connection dbNode, List<Connection> successFullDbNnodes) {
		
		String addressString = dbNode.getIP() +":"+ dbNode.getPort();
		final ManagedChannel channel = ManagedChannelBuilder.forTarget(addressString)
				.usePlaintext(true)
				.build();

		DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(channel);	

		StreamObserver<FileTransfer.FileInfo> responseObserver = new StreamObserver<FileTransfer.FileInfo>() {
			public void onNext(FileTransfer.FileInfo fileInfo) {
				//send to DB
				//successFullDbNnodes.add(dbNode);
				logger.debug("Successfully written chunk: "+fileInfo.getFileName());
			}

			public void onError(Throwable t) {
				t.printStackTrace();
				logger.debug("Upload File Chunk failed:" + t.getMessage() + "at " + dbNode.getIP());
			}

			public void onCompleted() {
				logger.debug("Upload File Chunk completed.");
			}
		};

		StreamObserver<FileTransfer.FileUploadData> requestObserver = stub.uploadFile(responseObserver);
		try{
			requestObserver.onNext(fileUploadData);
			Thread.sleep(5000);

		} catch(RuntimeException e){
			requestObserver.onError(e);
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		requestObserver.onCompleted();
		//channel.shutdown();
	}
	
	/**
	 * Initiate downloadChunk request from proxy to db
	 * 
	 * rpc DownloadChunk (ChunkInfo) returns (stream FileMetaData);
	 * 
	 * TODO  Update target address with Db Server IP
	 */
	public Iterator<FileMetaData> downloadChunk(ChunkInfo request, List<String> li) {
		Iterator<FileMetaData> fileMetaDataList = null;
		for (String addr : li) {
			final ManagedChannel channel = ManagedChannelBuilder.forTarget(addr)
					.usePlaintext(true)
					.build();
			DataTransferServiceGrpc.DataTransferServiceBlockingStub blockingStub = DataTransferServiceGrpc.newBlockingStub(channel);	
			try {
				fileMetaDataList = blockingStub.downloadChunk(request);
				} catch (StatusRuntimeException ex) {
				  logger.log(Level.WARN, "RPC failed: {0} from "+ li);
				}
			channel.shutdown();
			if(fileMetaDataList != null) {
				break;
			}
		}
		return fileMetaDataList;
	}
	
	
	/**
	 * Proxy to get File Locations from RAFT
	 * 
	 * rpc getChunkLocations (FileData) returns (ChunkLocations)
	 * 
	 * TODO Update target address with RAFT Server IP
	 * @param connection 
	 * 
	 */
	public ChunkLocations GetChunkLocations(ChunkInfo ch, Connection connection) {
		

		String addressString = connection.getIP() +":"+ connection.getPort();
		final ManagedChannel channel = ManagedChannelBuilder.forTarget(addressString)
				.usePlaintext(true)
				.build();
		
		TeamClusterServiceGrpc.TeamClusterServiceBlockingStub blockingStub = TeamClusterServiceGrpc.newBlockingStub(channel);
		
		FileData fd = FileData.newBuilder().setChunkId(ch.getChunkId()).setFileName(ch.getFileName()).setMessageId(ch.getStartSeqNum()).build();
		
		return blockingStub.getChunkLocations(fd);
	}

	/**
	 * This method will update the RAFT about chunk locations and whether upload to Db has been successful
	 * @param successFullDbNnodes
	 * @param raftNode 
	 * @param value 
	 */
	public void updateChunkLocations(List<Connection> successFullDbNnodes, Connection raftNode, FileUploadData value) {
		
		logger.debug("updateChunkLocations rpc called ..");
		String addressString = raftNode.getIP() +":"+ raftNode.getPort();
		final ManagedChannel channel = ManagedChannelBuilder.forTarget(addressString)
				.usePlaintext(true)
				.build();
		
		List<String> dbNodesAsString = new ArrayList<String>();
		for(Connection con : successFullDbNnodes) {
			dbNodesAsString.add(con.getIP()+":"+ con.getPort());
		}
		//TODO I have not set message ID as I dont knwo how to set  a unique random message ID
		TeamClusterServiceGrpc.TeamClusterServiceBlockingStub blockingStub = TeamClusterServiceGrpc.newBlockingStub(channel);
		Team.ChunkLocations request =  Team.ChunkLocations.newBuilder().setChunkId(value.getChunkId())
										.addAllDbAddresses(dbNodesAsString)
										.setFileName(value.getFileName())
										.setMaxChunks(value.getMaxChunks())
										.setMessageId(value.getSeqMax())
										.build();

	    Team.Ack response = blockingStub.updateChunkLocations(request);
	    logger.debug(response.getIsAck() +"  " + addressString);
		logger.debug("updateChunkLocations rpc finished ..");
	    //channel.shutdownNow();

	}
}
