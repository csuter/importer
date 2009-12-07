package importer;

import java.io.File;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import importer.database.DataAccessManager;
import importer.database.MysqlDataAccessManager;
import importer.configuration.InputFile;

public class Main
{
    static Logger log = Logger.getLogger(Main.class.getName());
    private static String dataFilename;
    private static String descriptorFile;

    public static void main(final String[] args)
    {
    	processCommandLine(args);
    	initLog();
    	try
    	{
            InputFile inputFile = new InputFile(descriptorFile);
            DataAccessManager dam = new MysqlDataAccessManager(inputFile,new File("/tmp/importer_tables"));
            Importer importer = new Importer(inputFile, dam);

            importer.setDataFile(new File(dataFilename));
            importer.start();
    	}
    	catch (Exception e)
    	{
            log.error("Error: ", e);
    	}
    }


    private static void processCommandLine(final String[] args)
    {
        dataFilename = args[0];
        descriptorFile = args[1];
    }

    private static void initLog()
    {
    	/* Fetch location of log4j.properties file from Configuration */
    	String log4jConfigFilename = "conf/log4j.properties";
    	/* Initialize logger properties */
    	PropertyConfigurator.configure(log4jConfigFilename);
    }
}
