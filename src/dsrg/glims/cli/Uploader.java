/*
 * Copyright (c) 2012 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package dsrg.glims.cli;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.java6.auth.oauth2.FileCredentialStore;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.media.MediaHttpDownloader;
import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.ChildList;
import com.google.api.services.drive.model.ChildReference;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.ParentReference;
import com.google.api.services.drive.model.Permission;
import com.google.common.base.Preconditions;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

/**
 * A sample application that runs multiple requests against the Drive API. The
 * requests this sample makes are:
 * <ul>
 * <li>Does a resumable media upload</li>
 * <li>Updates the uploaded file by renaming it</li>
 * <li>Does a resumable media download</li>
 * <li>Does a direct media upload</li>
 * <li>Does a direct media download</li>
 * </ul>
 * 
 * @author rmistry@google.com (Ravi Mistry)
 */
public class Uploader {
	 private static Permission perm = new Permission().setRole("writer").setType("user");

	 /** Global instance of the HTTP transport. */
	private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

	/** Global instance of the JSON factory. */
	private static final JsonFactory JSON_FACTORY = new JacksonFactory();

	/** Global Drive API client. */
	private static Drive drive;

	/** Authorizes the installed application to access user's protected data. */
	private static Credential authorize() throws Exception {
		// load client secrets
		GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(
				JSON_FACTORY,
				Uploader.class.getResourceAsStream("client_secrets.json"));
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

	public static void main(String[] args) {
		Preconditions
				.checkArgument(args.length >= 5,
						"Usage: java -jar Uploader.jar <path to file> <title> <mime type> <email> <workflow>");

		String fileName = args[0];
		String title = args[1];
		String mimeType = args[2];
		String raw_email = args[3];
		String email = null;
		if (raw_email.contains("__at__")) {
			String[] email_parts = raw_email.split("\\__at__");
			email = email_parts[0] + "@" + email_parts[1];
		} else {
			email = raw_email;
		}
		perm.setValue(email);
		String workflow = args[4];

		try {
			try {
				// authorization
				Credential credential = authorize();
				// set up the global Drive instance
				drive = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY,
						credential)
						.setApplicationName("Midgard Uploader/1.0").build();

				// run commands

				View.header1("Starting Resumable Media Upload");

				String parent = determineParent(email,workflow);
				File uploadedFile = uploadFile(fileName, title, mimeType, parent, false);
				// Now update the permissions
				drive.permissions().insert(uploadedFile.getId(), perm).execute();

				/*
				 * View.header1("Starting Resumable Media Download");
				 * downloadFile(false, updatedFile);
				 * 
				 * View.header1("Starting Simple Media Upload"); uploadedFile =
				 * uploadFile(true);
				 * 
				 * View.header1("Starting Simple Media Download");
				 * downloadFile(true, uploadedFile);
				 */

				View.header1("Success!");
				return;
			} catch (IOException e) {
				System.err.println(e.getMessage());
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}
		System.exit(1);
	}

	private static String determineParent(String email, String workflow) {
		String root = "0B7Jfx3RRVE5YenZEY1N5cE5pRms";
		String parentFolder = null;

		String level = root;
		String parent = root;
		boolean lowest = false;

		// Checks for user
		level = folderContains(email, level);
		if (level != null) {

			DateFormat today = new SimpleDateFormat("E, MM/dd/yyyy");
			Date now = new Date();
			String nowStr = today.format(now);

			// Checks for today's date
			parent = level;
			level = folderContains(nowStr, level);
			if (level != null) {

				// Checks for a workflowID folder
				parent = level;
				level = folderContains(workflow, level);
				// System.out.println("level: "+level);
				if (level != null) {

					// Finds the highest folder number; add 1 and creates it.
					try {
						File child = null;
						int lastRun = 0;
						ChildList children = drive.children().list(level)
								.execute();
						for (ChildReference element : children.getItems()) {
							child = drive.files().get(element.getId())
									.execute();
							lastRun = Integer.parseInt(child.getTitle());
						}
						lastRun += 1;
						String next = new Integer(lastRun).toString();
						// System.out.println("level: "+level);
						// System.out.println("next: "+next);
						parentFolder = createFolderWithParentAndTitle(level,
								next);

					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();

					} finally {
						lowest = true;
					}

				} else {
					parentFolder = createFolderWithParentAndTitle(parent,
							workflow);
				}
			} else {
				parentFolder = createFolderWithParentAndTitle(parent, nowStr);
			}
		} else {
			parentFolder = createFolderWithParentAndTitle(parent, email);
		}

		try {
			File file = drive.files().get(parentFolder).execute();
			drive.permissions().insert(file.getId(), perm).execute();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (!lowest)
			parentFolder = determineParent(email, workflow);

		return parentFolder;
	}

	private static String folderContains(String filename, String folderID) {

		// Returns the ID of a specified filename in a folder or null if it does
		// not exist

		File child = null;
		ChildList children = null;
		try {
			children = drive.children().list(folderID).execute();
			for (ChildReference element : children.getItems()) {
				child = drive.files().get(element.getId()).execute();
				if (child.getTitle().equals(filename)) {
					break;
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			// System.out.println("children: "+children.toPrettyString());
			// System.out.println("child: "+child.toPrettyString());
		}

		if (child != null && child.getTitle().equals(filename)) {
			return child.getId();
		}

		return null;
	}

	private static String createFolderWithParentAndTitle(String parentID,
			String title) {
		String folderMIME = "application/vnd.google-apps.folder";

		File resultsDes = new File()
				.setTitle(title)
				.setParents(
						Arrays.asList(new ParentReference().setId(parentID)))
				.setMimeType(folderMIME);
		File returned = null;
		try {
			// System.out.println(resultsDes);
			returned = drive.files().insert(resultsDes).execute();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (returned != null)
			return returned.getId();

		return null;
	}

	/** Uploads a file using either resumable or direct media upload. */
	private static File uploadFile(String filePath, String title,
			String mimeType, String parentId, boolean useDirectUpload) throws IOException {
		File fileMetadata = new File();
		fileMetadata.setTitle(title);
	    fileMetadata.setParents(Arrays.asList(new ParentReference().setId(parentId)));
		java.io.File uploadFile = new java.io.File(filePath);
		InputStreamContent mediaContent = new InputStreamContent(mimeType,
				new BufferedInputStream(new FileInputStream(uploadFile)));
		mediaContent.setLength(uploadFile.length());

		Drive.Files.Insert insert = drive.files().insert(fileMetadata,
				mediaContent);
		
		MediaHttpUploader uploader = insert.getMediaHttpUploader();
		uploader.setDirectUploadEnabled(useDirectUpload);
		uploader.setProgressListener(new FileUploadProgressListener());
		return insert.execute();
	}

	/** Downloads a file using either resumable or direct media download. */
	/*
	 * private static void downloadFile(boolean useDirectDownload, File
	 * uploadedFile) throws IOException { // create parent directory (if
	 * necessary) java.io.File parentDir = new java.io.File(DIR_FOR_DOWNLOADS);
	 * if (!parentDir.exists() && !parentDir.mkdirs()) { throw new
	 * IOException("Unable to create parent directory"); } OutputStream out =
	 * new FileOutputStream(new java.io.File(parentDir,
	 * uploadedFile.getTitle()));
	 * 
	 * Drive.Files.Get get = drive.files().get(uploadedFile.getId());
	 * MediaHttpDownloader downloader = get.getMediaHttpDownloader();
	 * downloader.setDirectDownloadEnabled(useDirectDownload);
	 * downloader.setProgressListener(new FileDownloadProgressListener());
	 * downloader.download(new GenericUrl(uploadedFile.getDownloadUrl()), out);
	 * }
	 */
}
