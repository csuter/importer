package importer.configuration;

import java.io.File;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileInputStream;

public class InputFile
{

    DatabaseInfo databaseInfo;
    InputFormat inputFormat;

    public InputFile(String filename) throws Exception
    {
        this(new File(filename));
    }

    public InputFile(File file) throws Exception
    {
        String rawInput = readData(file);
        KVList kvList = new KVList(rawInput);

        for (Entry entry : kvList.getList().getEntries())
        {
            if (entry instanceof PairEntry)
            {
                PairEntry pair = (PairEntry) entry;

                String keyText = pair.getLhs().getText();
                if (keyText.equals("Database"))
                {
                    this.databaseInfo = new DatabaseInfo((ListEntry)pair.getRhs());
                }
                else if (keyText.equals("InputFormat"))
                {
                    this.inputFormat = new InputFormat( ((ListEntry)pair.getRhs()).getEntry(0) );
                }
            }
        }
    }
    
    private String readData(File file) throws Exception
    {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));

        StringBuilder dataBuilder = new StringBuilder();
        String line;

        while ( (line=reader.readLine()) != null)
        {
            if (! line.matches("^\\s*#.*"))
            {
                dataBuilder.append(line+"\n");
            }
        }

        return dataBuilder.toString().replaceAll("\\s+", "").trim();
    }
    public DatabaseInfo getDatabaseInfo() { return this.databaseInfo; }
    public InputFormat getInputFormat() { return this.inputFormat; }
}
