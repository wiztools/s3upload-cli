package org.wiztools.s3uploadcli;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
    
    private static Options generateOptions(){
        Options options = new Options();
        
        // AWS credentials
        Option option = OptionBuilder
                .withLongOpt("aws-creds-file")
                .hasArg()
                .withDescription("Java properties file with AWS credentials")
                .create('k');
        options.addOption(option);
        
        // access key
        option = OptionBuilder
                .withLongOpt("accesskey")
                .hasArg()
                .withDescription("AWS access key (not needed when -k option is used)")
                .create('a');
        options.addOption(option);
        
        // secret key
        option = OptionBuilder
                .withLongOpt("secretkey")
                .hasArg()
                .withDescription("AWS secret key (not needed when -k option is used)")
                .create('s');
        options.addOption(option);
        
        // bucket
        option = OptionBuilder
                .withLongOpt("bucket")
                .hasArg()
                .isRequired()
                .withDescription("Destination S3 bucket name")
                .create('b');
        options.addOption(option);
        
        // help
        option = OptionBuilder
                .withLongOpt("help")
                .hasArg(false)
                .isRequired(false)
                .withDescription("Print this help")
                .create('h');
        options.addOption(option);
        
        return options;
    }
    
    private static void printCommandLineHelp(Options options){
        HelpFormatter hf = new HelpFormatter();
        String cmdLine = "java -jar s3upload-cli-NN-jar-with-dependencies.jar [options] file1 file2 ...";
        String descriptor = "AWS S3 upload tool";
        String moreHelp = "Format of `aws-creds-file': \n"
                + "\tAWSAccessKeyId=XXX\n"
                + "\tAWSSecretKey=XXX";
        hf.printHelp(cmdLine, descriptor, options, moreHelp);
    }
    
    public static void main(String[] arg) {
        Options options = generateOptions();
        try{
            CommandLineParser parser = new GnuParser();
            CommandLine cmd = parser.parse(options, arg);
            
            if(cmd.hasOption('h')){
                printCommandLineHelp(options);
                return;
            }
            
            if(cmd.getArgs().length == 0) {
                System.err.println("No files specified for transfer.");
                printCommandLineHelp(options);
                System.exit(EXIT_CLI_ERROR);
            }

            String awsCredsFile = options.getOption("aws-creds-file").getValue();
            String accessKey = options.getOption("accesskey").getValue();
            String secretKey = options.getOption("secretkey").getValue();
            String bucketName = options.getOption("bucket").getValue();
            
            if(awsCredsFile == null && (accessKey == null || secretKey == null)) {
                System.err.println("Either -k or (-a and -s) options are mandatory.");
                printCommandLineHelp(options);
                System.exit(EXIT_CLI_ERROR);
            }
            
            if(awsCredsFile != null && (accessKey != null && secretKey != null)) {
                System.err.println("Options -k and (-a and -s) cannot coexist.");
                printCommandLineHelp(options);
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
                
                for(String fileName: cmd.getArgs()) {
                    File file = new File(fileName);
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
        catch(ParseException ex){
            System.err.println(ex.getMessage());
            printCommandLineHelp(options);
            System.exit(EXIT_CLI_ERROR);
        }
    }
}
