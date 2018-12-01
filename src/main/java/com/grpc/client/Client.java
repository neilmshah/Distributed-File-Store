package com.grpc.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Client {
	
	private static ManagedChannel getChannel(String address) {
		return ManagedChannelBuilder.forTarget(address)
		        .usePlaintext(true)
		        .build();
	}

	public static void main(String[] args) throws IOException {
		Scanner scan = new Scanner(System.in);
		showMenu();
		String readFile = "";
		List<ProxyInfo> proxyList = new ArrayList<ProxyInfo>();
		int maxChunks;
		String fileUploadInfo = null;
		
		while(scan.hasNext()) {
			String s1 = scan.nextLine();
			if(s1.equals("Over")) {
				break;
			}
			switch(s1) {
			case "0":
				showMenu();
				break;
			case "1":
				listFiles();
				break;
			case "2":
				System.out.println("Enter the File Name: \n");
				readFile = scan.nextLine();
				requestFileInfo(readFile);
				System.out.println("Press 3 to download file. \n");
				break;
			case "3":
				if(readFile != "") {
					downloadFile(readFile);
				} else {
					System.out.println("Press 1 to select a file. \n");
				}
				readFile = "";
				break;
			case "4":
				System.out.println("Enter the fileName, fileSize and maxChunks separated by comma to upload: \n");
				fileUploadInfo = scan.nextLine();
				while(!checkFileUploadInfo(fileUploadInfo)) {
					System.out.println("Press check the format and try again. \n");
				}
				proxyList = requestFileUpload(fileUploadInfo);
				System.out.println("Press 5 to start uploading. \n");
				break;
			case "5":
				System.out.println("Enter File location \n");
				File f = new File(scan.nextLine());
				splitFile(f, Integer.parseInt(fileUploadInfo.split(",")[1]));
				if(proxyList.isEmpty()) {
					System.out.println("Please Press 4 to request file upload. \n");
				} else {
					uploadFile(proxyList, fileUploadInfo);
					fileUploadInfo = null;
				}
				break;
			default:
				System.out.println("Invalid option. Press 0 to see Menu\n");
				break;
			}
			
		}
		scan.close();
	}

	private static void uploadFile(List <ProxyInfo> proxyList, String fileUploadInfo) {
		
		// TODO Split file into Chunks
		
		
		
		int proxyNum = proxyList.size();
		String [] words = fileUploadInfo.split(",");
		
		ArrayList<ManagedChannel> chList = new ArrayList<ManagedChannel>();
		for(ProxyInfo pr: proxyList) {
			chList.add(getChannel(pr.getIp() + pr.getPort()));
		}
		
		//TODO Create stub per each channel
		DataTransferServiceGrpc.DataTransferServiceStub ayncStub = DataTransferServiceGrpc.newStub(chList.get(1));
	}

	private static boolean checkFileUploadInfo(String line) {
		// TODO Check each element of words
		String [] words = line.split(",");
		if(words.length != 3)
			return false;
		return true;
		
	}

	private static List <ProxyInfo> requestFileUpload(String fileUploadInfo) {
		// TODO Auto-generated method stub
		System.out.println("requestFileUpload called with" + fileUploadInfo);
		
		ManagedChannel ch = getChannel("localhost: 8000");
		DataTransferServiceGrpc.DataTransferServiceBlockingStub blockingStub = DataTransferServiceGrpc.newBlockingStub(ch);
		
		String [] fileDetails = fileUploadInfo.split(",");
		FileUploadInfo request = FileUploadInfo.newBuilder()
				.setFileName(fileDetails[0])
				.setFileSize(Float.parseFloat(fileDetails[1]))
				.setMaxChunks(Long.parseLong(fileDetails[2]))
				.build();
		
		return blockingStub.requestFileUpload(request).getLstProxyList();
	}

	private static void showMenu() {
		System.out.println("Select an option and press Enter: \n"
				+ "1) List Files\n" 
				+ "2) Read a particular file\n"
				+ "3) Download File Chunk\n"
				+ "4) Request File Upload\n"
				+ "5) Upload File\n"
				);
		
	}

	private static void downloadFile(String readFile) {
		// TODO Auto-generated method stub
		System.out.println("downloadFile called with" + readFile);
		
	}

	private static void requestFileInfo(String fileName) {
		// TODO Auto-generated method stub
		System.out.println("requestFileInfo called with " + fileName);
		
		//TODO Update RAFT Node address
		ManagedChannel channel = getChannel("localhost:8000");
		DataTransferServiceGrpc.DataTransferServiceBlockingStub blockingStub = DataTransferServiceGrpc.newBlockingStub(channel);
		
		FileInfo request = FileInfo.newBuilder().setFileName(fileName).build();
		
		FileLocationInfo fileLocations = blockingStub.requestFileInfo(request);
		
		System.out.println("Locations: \n" + fileLocations.getLstProxyList());
	}

	private static void listFiles() {
		System.out.println("listFiles called");
		
		//TODO Update RAFT Node address
		ManagedChannel channel = getChannel("localhost:8000");
		
		DataTransferServiceGrpc.DataTransferServiceBlockingStub blockingStub = DataTransferServiceGrpc.newBlockingStub(channel);
		
		RequestFileList request = RequestFileList.newBuilder().setIsClient(true).build();
		FileList li = blockingStub.listFiles(request);
		
		System.out.println("File List: \n" + li);
	}
	
	public static void splitFile(File f, int fileSize) throws IOException {

        int chunkSize = 1024 * 1024;// 1MB
        byte[] buffer = new byte[chunkSize];

        String fileName = f.getName();
        
        FileInputStream fis = new FileInputStream(f);
        BufferedInputStream bis = new BufferedInputStream(fis, chunkSize);
        
        OutputStream out = null;
        int totalBytes = 0;
        
        while(totalBytes <= fileSize) {
        		totalBytes = bis.read(buffer);
        }
        
        BufferedOutputStream bos = new BufferedOutputStream(out, chunkSize);
        
        bos.write(buffer, 0, buffer.length);
        System.out.println("git");
        
       


    }
}
