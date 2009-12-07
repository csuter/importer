package importer;

import importer.strategy.MainImportStrategy;
import importer.strategy.ImportStrategy;
import importer.strategy.ImportStrategy;
import importer.database.DataAccessManager;
import importer.configuration.InputFile;
import com.grooveshark.readfast.LineSource;
import org.apache.log4j.Logger;
import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

public class Importer
{
    static Logger log = Logger.getLogger(Importer.class.getName());

    private ImportStrategy strategy;
    private File dataFile;

    public Importer(InputFile inputFile, DataAccessManager dataAccessManager) throws Exception
    {
        this.strategy = new MainImportStrategy(dataAccessManager, inputFile);
    }

    public void setDataFile(File dataFile) { this.dataFile = dataFile; }

    public void start()
    {
        long bigstart, bigend;
        long start, end;
        try
        {
            LineSource lineSource = new LineSource(dataFile);
            String line;
            int count = 0;

            start = System.currentTimeMillis();
            while ((line = lineSource.readLine()) != null) {
                strategy.importEntry(line);
                count++;
            }
            end = System.currentTimeMillis();
            lineSource.close();

            log.info("Processed " + count + " log entries in " + nsecs(start,end) + " seconds");

            log.info("Beginning database import");
            start = System.currentTimeMillis();
            strategy.finalizeImport();
            end = System.currentTimeMillis();

            log.info("DONE! in " + nsecs(start,end) + " seconds");
        }
        catch (Exception e)
        {
            log.error("Could not access file (probably)", e);
        }
    }

    public static float nsecs(long start, long end) { return ((float)(end - start)/(1000f)); }
}
