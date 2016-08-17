package org.jeet.awsS3.demo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;

public class BucketUtils {

	private static final boolean IS_WINDOWS = System.getProperty("os.name").contains("indow");
	
	public static void deleteAllBuckets(AmazonS3Client s3Client) throws Exception {
		
		for(Bucket bucket:s3Client.listBuckets()) {
			System.out.println("Deleting bucket: "+ bucket.getName());
			BucketUtils.deleteBucket(bucket.getName(), s3Client);
		}
	}

	public static void deleteBucket(String bucketName, AmazonS3Client s3Client) throws IOException {
		
		try{
			System.out.println("Deleting S3 Bucket: "+ bucketName);
			ObjectListing objectListing = s3Client.listObjects(bucketName);
			
			while(true) {
				for( Iterator<?> iterator = 
						objectListing.getObjectSummaries().iterator();
						iterator.hasNext();) {
					S3ObjectSummary objectSummary = (S3ObjectSummary) iterator.next();
					s3Client.deleteObject(bucketName, objectSummary.getKey());
				}
				
				 if (objectListing.isTruncated()) {
                     objectListing = s3Client.listNextBatchOfObjects(objectListing);
                 } else {
                     break;
                 }
             };
             VersionListing list = s3Client.listVersions(new ListVersionsRequest().withBucketName(bucketName));
             for ( Iterator<?> iterator = list.getVersionSummaries().iterator(); iterator.hasNext(); ) {
                 S3VersionSummary s = (S3VersionSummary)iterator.next();
                 s3Client.deleteVersion(bucketName, s.getKey(), s.getVersionId());
             }
             s3Client.deleteBucket(bucketName);


         } catch (AmazonServiceException ase) {
             System.out.println("Caught an AmazonServiceException, which " +
                     "means your request made it " +
                     "to Amazon S3, but was rejected with an error response" +
                     " for some reason.");
             System.out.println("Error Message:    " + ase.getMessage());
             System.out.println("HTTP Status Code: " + ase.getStatusCode());
             System.out.println("AWS Error Code:   " + ase.getErrorCode());
             System.out.println("Error Type:       " + ase.getErrorType());
             System.out.println("Request ID:       " + ase.getRequestId());
         } catch (AmazonClientException ace) {
             System.out.println("Caught an AmazonClientException, which " +
                     "means the client encountered " +
                     "an internal error while trying to " +
                     "communicate with S3, " +
                     "such as not being able to access the network.");
             System.out.println("Error Message: " + ace.getMessage());
         }
     }

 public static String readFileFromResources(String fileName)
         throws Exception
 {

     String path = S3JavaSDKExample.class.getResource(fileName).toURI().getPath();

     String osAppropriatePath = IS_WINDOWS ? path.substring(1) : path;


     byte[] encoded = Files.readAllBytes(Paths.get(osAppropriatePath));


     return new String(encoded);
 }
}
