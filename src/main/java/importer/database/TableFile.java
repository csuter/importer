package importer.database;

import org.apache.log4j.Logger;
import java.io.File;
import java.io.FileWriter;
import java.nio.channels.FileChannel;
//import java.io.FileOutputStream;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.sql.ResultSet;

public class TableFile
{
    static Logger log = Logger.getLogger(TableFile.class.getName());
    private String tableName;
    private File tmpFolder;
    private DataAccessManager dataAccessManager;

    private int rowCount;
    private File file;
    private List<String> columnListing;
    private String autoIncId;
    private String foreignKeyField = null;
    private String uniqueField = null;
    private Map<String,String> currentRow = null;

    private FileChannel fileChannel;

    private FileWriter writer;


    public TableFile(DataAccessManager dataAccessManager, String tableName, File tmpFolder) throws Exception
    {
        this.dataAccessManager = dataAccessManager;
        this.tableName = tableName;
        this.tmpFolder = tmpFolder;
        this.rowCount = 0;
        this.file = new File(tmpFolder, tableName);
        if (file.exists()) file.delete();
        this.columnListing = getTableColumnListing(tableName);

        this.writer = new FileWriter(this.file, true);

        //this.fileChannel = new FileInputStream(file, true).getChannel();

        // create the first empty row
        this.createRow();
    }
    
    public List<String> getTableColumnListing(String tableName) throws Exception
    {
        List<String> result = new LinkedList<String>();

        ResultSet resultSet = dataAccessManager.getDescribeTableResustSet(this.tableName);

        while (resultSet.next())
        {
            String fieldName = resultSet.getString("Field");
            if (resultSet.getString("Extra").equals("auto_increment"))
            {
                this.autoIncId = fieldName;
                fieldName = "*" + fieldName;
            }
            if (resultSet.getString("Key").equals("UNI"))
            {
                this.uniqueField = fieldName;
            }

            result.add(fieldName);
        }

        return result;
    }

    public void createRow() throws Exception
    {
        this.currentRow = new HashMap<String,String>(5);
        this.rowCount++;

        for (String columnName : this.columnListing)
        {
            // set the auto-inc field to a tmp value
            // (current row count), if there is one
            if (columnName.startsWith("*"))
            {
                currentRow.put(columnName.substring(1), ""+this.rowCount);
                break;
            }
        }
    }

    public void populateCurrentRow(String fieldName, String data)
    {
        this.currentRow.put(fieldName, data);
    }

    public void commitCurrentRow()
    {
        StringBuilder builder = new StringBuilder();

        for (String columnName : this.columnListing)
        {
            if (columnName.startsWith("*")) columnName = columnName.substring(1);

            builder.append(this.currentRow.get(columnName) + "\t");
        }
        builder.deleteCharAt( builder.lastIndexOf("\t") );

        try
        {
            writer.write(builder.toString() + "\n");
            // create next row
            this.createRow();
        }
        catch (Exception e)
        {
            log.error("Error writing to cache file: " + file.toString(), e);
        }
    }

    public void closeForWriting() throws Exception
    {
        writer.flush();
        writer.close();
    }

    public int getRowCount() { return this.rowCount; }
    public Map<String,String> getCurrentRow() { return this.currentRow; }
    public String getAutoIncId() { return this.autoIncId; }
    public List<String> getColumnListing() { return this.columnListing; }
    public String getForeignKeyField() { return this.foreignKeyField; }
    public String getUniqueField() { return this.uniqueField; }
    public File getFile() { return this.file; }
}
