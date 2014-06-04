package org.wiztools.s3uploadcli;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

/**
 *
 * @author schandran
 */
public class S3UploadMain {
    
    private static final int EXIT_CLI_ERROR = 1;
    private static final int EXIT_IO_ERROR = 2;
    private static final int EXIT_S3_ERROR = 3;
    private static final int EXIT_SYS_ERROR = 4;
    
    private static void printCommandLineHelp(PrintStream out){
        String cmdLine = "java -jar s3upload-cli-NN-jar-with-dependencies.jar [opts] file(s)";
        String descriptor = "Where [opts] are:";
        
        String opts =
                "  -a  AWS access key (not needed when -k option is used).\n" +
                "  -s  AWS secret key (not needed when -k option is used).\n" +
                "  -k  Java properties file with AWS credentials.\n" +
                "  -b  Destination S3 bucket name.\n" +
                "  -h  Prints this help.\n";
        
        String moreHelp = "Format of `aws-creds-file': \n"
                + "\tAWSAccessKeyId=XXX\n"
                + "\tAWSSecretKey=XXX";
        
        out.printf("Usage: %s\n%s\n%s\n%s\n", cmdLine, descriptor, opts, moreHelp);
    }
    
    public static void main(String[] arg) {
        
        try{
            
            OptionParser parser = new OptionParser( "a:s:k:b:h" );
            OptionSet options = parser.parse(arg);
            
            if(options.has("h")) {
                printCommandLineHelp(System.out);
                System.exit(0);
            }

            String awsCredsFile = (String) options.valueOf("k");
            String accessKey = (String) options.valueOf("a");
            String secretKey = (String) options.valueOf("s");
            String bucketName = (String) options.valueOf("b");
            
            if(awsCredsFile == null && (accessKey == null || secretKey == null)) {
                System.err.println("Either -k or (-a and -s) options are mandatory.");
                printCommandLineHelp(System.err);
                System.exit(EXIT_CLI_ERROR);
            }
            
            if(awsCredsFile != null && (accessKey != null && secretKey != null)) {
                System.err.println("Options -k and (-a and -s) cannot coexist.");
                printCommandLineHelp(System.err);
                System.exit(EXIT_CLI_ERROR);
            }
            
            if(awsCredsFile != null) {
                Properties p = new Properties();
                try {
                    p.load(new FileInputStream(new File(awsCredsFile)));
                    
                    accessKey = p.getProperty("AWSAccessKeyId");
                    secretKey = p.getProperty("AWSSecretKey");
                }
                catch(IOException ex) {
                    System.err.println("Cannot read AWS property file.");
                    ex.printStackTrace(System.err);
                    System.exit(EXIT_IO_ERROR);
                }
            }
            
            AWSCredentials cred = new AWSCredentials(accessKey, secretKey);
            try{
                S3Service service = new RestS3Service(cred);
                S3Bucket bucket = new S3Bucket(bucketName);
                
                for(Object o: options.nonOptionArguments()) {
                    final String fileName = (String) o;
                    final File file = new File(fileName);
                    if(file.isFile() && file.canRead()) {
                        S3Object object = new S3Object(bucket, file);
                        service.putObject(bucket, object);
                    }
                    else {
                        System.err.println("Failed: " + fileName);
                    }
                }
                
                service.shutdown();
                
            }
            catch(S3ServiceException ex){
                ex.printStackTrace(System.err);
                System.exit(EXIT_S3_ERROR);
            }
            catch(ServiceException ex) {
                ex.printStackTrace(System.err);
                System.exit(EXIT_S3_ERROR);
            }
            catch(NoSuchAlgorithmException ex){
                ex.printStackTrace(System.err);
                System.exit(EXIT_SYS_ERROR);
            }
            catch(FileNotFoundException ex){
                ex.printStackTrace(System.err);
                System.exit(EXIT_IO_ERROR);
            }
            catch(IOException ex){
                ex.printStackTrace(System.err);
                System.exit(EXIT_IO_ERROR);
            }
        }
        catch(OptionException ex){
            System.err.println(ex.getMessage());
            printCommandLineHelp(System.err);
            System.exit(EXIT_CLI_ERROR);
        }
    }
}
