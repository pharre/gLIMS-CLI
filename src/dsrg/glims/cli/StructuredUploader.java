package dsrg.glims.cli;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.java6.auth.oauth2.FileCredentialStore;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.Drive.Files.Insert;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.ParentReference;
import com.google.appengine.api.files.LockException;
import com.google.common.base.Preconditions;


public class StructuredUploader {

	/** Global instance of the HTTP transport. */
	private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

	/** Global instance of the JSON factory. */
	private static final JsonFactory JSON_FACTORY = new JacksonFactory();

	/** Global Drive API client. */
	private static Drive drive;
	
	private final static String FOLDER_MIME_TYPE = "application/vnd.google-apps.folder";
	
	/** Authorizes the installed application to access user's protected data. */
	private static Credential authorize() throws Exception {
		// load client secrets
		GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(
				JSON_FACTORY, new FileInputStream("client_secrets.json"));
				//StructuredUploader.class.getResourceAsStream("client_secrets.json"));
		if (clientSecrets.getDetails().getClientId().startsWith("Enter")
				|| clientSecrets.getDetails().getClientSecret()
						.startsWith("Enter ")) {
			System.out
					.println("Enter Client ID and Secret from https://code.google.com/apis/console/?api=drive "
							+ "into resources/client_secrets.json");
			System.exit(1);
		}
		// set up file credential store
		FileCredentialStore credentialStore = new FileCredentialStore(
				new java.io.File(System.getProperty("user.home"),
						".credentials/drive.json"), JSON_FACTORY);
		// set up authorization code flow
		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
				HTTP_TRANSPORT, JSON_FACTORY, clientSecrets,
				Collections.singleton(DriveScopes.DRIVE_FILE))
				.setCredentialStore(credentialStore).build();
		// authorize
		return new AuthorizationCodeInstalledApp(flow,
				new LocalServerReceiver()).authorize("user");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Preconditions.checkArgument(args.length >= 3,
				"Usage: java -jar StructuredUploader.jar <path to file> <path to google drive folder> <root name> <transpose?>");
		String fileName = args[0];
		String gDirStr = args[1];
		String rootName = args[2];
		boolean transpose = false;
		if (args.length > 3) {
			String firstChar = args[3].substring(0, 1);
			if (firstChar.equalsIgnoreCase("y") || firstChar.equalsIgnoreCase("t")) {
				transpose = true;
			}
		}
		
		//TODO remove these hardcoded arguments
		// hardcode here to avoid awkward eclipse cmdln args
		// remove for deploy
		fileName = "collection_OA_transposed_less_short.txt";
		rootName = "OA_Sample_Data_In_Local";
		transpose = false;
		gDirStr = "C:\\Users\\pharre\\Google Drive\\";
		
		// validate the path to the folder in google drive
		if (!isValidFileName(gDirStr) || !isValidFileName(rootName)) {
			throw new IllegalArgumentException("must provide a valid path and folder names to google drive directiry");
		}
		java.io.File gDir = new java.io.File(gDirStr);
		if (!gDir.canWrite())
			throw new IllegalArgumentException("google drive folder must be writeable");
		
		if (transpose) { // looks for .txt extension
			System.out.println("transposing file ...");
			RowColumnTransposer rct = new RowColumnTransposer();
			java.io.File infile = new java.io.File(fileName);
			java.io.File outfile = new java.io.File(rct.makeTransposeName(infile));
			rct.transpose(infile, outfile, "\t", "\t");
			fileName = outfile.getAbsolutePath();
			System.out.println("transpose done. transpose written to " + fileName);
		}
		
		try {
			try {
				// authorization
				Credential credential = authorize();
				// set up the global Drive instance
				drive = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY,
						credential).setApplicationName("Midgard Uploader/1.0")
						.build();

				// run commands
				System.out.println("Starting Resumable Media Upload");
				writeDocsLabelsUsingLocalGDriveClient(fileName, rootName, drive, gDir);
				
				System.out.println("Success!");
				return;
			} catch (IOException e) {
				System.err.println(e.getMessage());
			}
		} catch (Throwable t) {
			t.printStackTrace();
			System.err.println("failed to authroize to google drive");
		}
		System.exit(1);
	}
		
	private static boolean isValidFileName(String fileName) {
	    return fileName.matches("^[^.\\\\/:*?\"<>|]?[^\\\\/:*?\"<>|]*") 
	    && getValidFileName(fileName).length()>0;
	}

	private static String getValidFileName(String fileName) {
	    String newFileName = fileName.replaceAll("^[.\\\\/:*?\"<>|]?[\\\\/:*?\"<>|]*", "");
	    if(newFileName.length()==0)
	        throw new IllegalStateException(
	                "File Name " + fileName + " results in a empty fileName!");
	    return newFileName;
	}
	
	private static void writeDocsLabelsUsingLocalGDriveClient(String fileName, String rootName, Drive service, java.io.File gDir)
			throws FileNotFoundException, LockException, IOException {
		
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		
		System.out.println("reading file: " + fileName);
		
		String metadataline = reader.readLine();
		
		String[] metadataArray = metadataline.split("\t");
		for (int i = 0; i < metadataArray.length; i++) {
			metadataArray[i] = metadataArray[i].trim().replace("\\", "");
		}
		
		System.out.println("gdir and filename "+ gDir + rootName);
		
		java.io.File localRootFolder = new java.io.File(gDir + "test\\test");
		System.out.println("can write " + localRootFolder.canWrite());
		System.exit(0);
		
		if (localRootFolder.mkdir()) {
			System.out.println("local root file created");
		} else {
			System.out.println("a file with that name alredy exists");
			reader.close();
			return;
		}
		
		int dataStart = 0;
		for (int i = 0; i < metadataArray.length; i++) {
			if (metadataArray[i].equals("X")) {
				dataStart = i;
				break;
			}
		}
		
		// Now to the data
		System.out.println("uploading the data...");
		reader.close();
		
		File driveRootFolder = new File();
		Insert rootFolderInsert = service.files().insert(new File().setTitle(rootName).setMimeType(FOLDER_MIME_TYPE));
		driveRootFolder = rootFolderInsert.execute();
		
		reader = new BufferedReader(new FileReader(fileName));
		// Now open the file and upload the data
		reader.readLine(); // Read header which we don't need
		Map<String, String> title2id = new HashMap<String, String>();
		String line;
		while ((line = reader.readLine()) != null) {
			String [] data = line.split("\t");
			
			StringBuffer buffer = new StringBuffer();
			buffer.append("X\tY\n");			
			for (int i = dataStart+1; i < data.length; i++) {
				buffer.append(metadataArray[i]).append("\t").append(data[i]).append("\n");				
			}
			
			File dataFile = new File();
			dataFile.setTitle(data[dataStart]);
			String mimeType = "text/plain";
			dataFile.setMimeType(mimeType);
			List<ParentReference> driveRootFolderParentRef = new ArrayList<ParentReference>();
			driveRootFolderParentRef.add(new ParentReference().setId(driveRootFolder.getId()));
			dataFile.setParents(driveRootFolderParentRef);
			ByteArrayContent byteArrayContent = new ByteArrayContent("text/plain", buffer.toString().getBytes());
			System.out.println("adding file " + data[dataStart]);
			while (true) {
				try {
					Drive.Files.Insert insert = drive.files().insert(dataFile, byteArrayContent);
					MediaHttpUploader uploader2 = insert.getMediaHttpUploader();
					uploader2.setDirectUploadEnabled(false);
					uploader2.setProgressListener(new FileUploadProgressListener());
					dataFile = insert.execute();
					title2id.put(dataFile.getTitle(), dataFile.getId());
					break;
				} catch (GoogleJsonResponseException e) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				}
			}
		}
		System.out.println("all files added");
		System.out.println(title2id);

		// Creates the new metadata key folders
		HashMap<String, java.io.File> metadataKeyFolders = new HashMap<String, java.io.File>();
		for (int i = 0; i < dataStart; i++) {
			java.io.File metadataKeyFolder = metadataKeyFolders.get(metadataArray[i]);
			if (metadataKeyFolder == null) {
				metadataKeyFolder = new java.io.File(gDir + rootName + "\\" + metadataArray[i]);
				metadataKeyFolder.mkdir();
			}
			metadataKeyFolders.put(metadataKeyFolder.getName(), metadataKeyFolder);
		}
				
		System.out.println("metadata key folders added");
		System.out.println(metadataKeyFolders);
		
		HashMap<String, HashMap<String, java.io.File>> metadataValueFolders = new HashMap<String, HashMap<String, java.io.File>>();
		for (String metadataKeyTitle : metadataKeyFolders.keySet()) {
			metadataValueFolders.put(metadataKeyTitle, new HashMap<String, java.io.File>());
		}
		
		System.out.println("creating metadata values that don't already exist");
		
		// Now create the metadata values that don't already exist
		String[] firstData = null;
		reader.close();
		reader = new BufferedReader(new FileReader(fileName));
		while ((line = reader.readLine()) != null) {
			String[] data = line.split("\t");
			if (firstData == null) {
				firstData = line.split("\t");
				for (int i = 0; i < firstData.length; i++)
					firstData[i] = firstData[i].trim().replace("\\", "");
				continue;
			}
			for (int i = 0; i < dataStart; i++) {
				data[i] = data[i].trim().replace("\\", "")	;
				if (firstData[i].equals(""))
					continue;
				if (data[i].equals(""))
					data[i] = firstData[i];
				String metadataKey = metadataArray[i]; 
				// Only create if new
				if (!metadataValueFolders.get(metadataKey).containsKey(data[i])) {
					// add metadata value folder
					String path = metadataKeyFolders.get(metadataKey).getAbsolutePath();
					System.out.println("absolute path of metadatakey folder: " + path);
					
					String metdataValFolderParentKey = firstData[i];
					String metadataValFolderTitle = data[i];
					String metadataValFolderPath = metadataKeyFolders.get(metdataValFolderParentKey).toString();
					java.io.File metadataValFolder = new java.io.File(metadataValFolderPath + "\\" + metadataValFolderTitle);
					if (!metadataValFolder.exists()) {
						try {
							metadataValFolder.mkdir(); // this is ok, some metadata files will have been created	
						} catch (Exception exception) {
							System.out.println(exception);
						}						
					}
					
					String dataFileIdTitle = title2id.get(data[dataStart]);
					String dataFilePath = metadataValFolder.getAbsolutePath();
					java.io.File dataIdFile = new java.io.File(dataFilePath + "\\" + dataFileIdTitle);
					try {
						dataIdFile.createNewFile(); // this is not ok, all data file ids must be written
					} catch (Exception e) {
						System.out.println(e);
					}
				}
			}
		}
	}
}
