package importer.database;

import importer.Importer;
import importer.database.DataAccessManager;
import importer.configuration.Entry;
import importer.configuration.TextEntry;
import importer.configuration.PairEntry;
import importer.configuration.ListEntry;
import importer.configuration.InputFile;
import importer.configuration.DatabaseInfo;
import org.apache.log4j.Logger;
import java.util.Collections;
import java.util.List;
import java.util.LinkedList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.io.File;
import java.io.PrintWriter;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.FileWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

public class MysqlDataAccessManager implements DataAccessManager
{
    static Logger log = Logger.getLogger(MysqlDataAccessManager.class.getName());
    private final InputFile inputFile;

    private List<String> tableNames;
    private Map<String,TableFile> tableNameToTableFileObject;

    private File tmpFolder;

    private Connection connection;
    private Statement statement;
    private String className = "com.mysql.jdbc.Driver";

    private Map<String,String> tablesToForeignKeyField;

    public MysqlDataAccessManager(InputFile inputFile, File tmpFolder) throws Exception
    {
        this.inputFile = inputFile;
        this.tmpFolder = tmpFolder;

        if (!tmpFolder.exists()) tmpFolder.mkdirs();


        initDBConnection();

        this.tableNames = new LinkedList<String>();
        this.tableNameToTableFileObject = new HashMap<String,TableFile>(10);
        this.tablesToForeignKeyField = new HashMap<String,String>(10);
    }

    public void commitCurrentRow(String tableName)
    {
        this.tableNameToTableFileObject.get(tableName).commitCurrentRow();
    }

    private void initDBConnection() throws Exception
    {
        DatabaseInfo dbInfo = inputFile.getDatabaseInfo();

        /* load the driver */

        Class driverClass = Class.forName(className);
        driverClass.newInstance();

        /* Create Connection obj... */

        this.connection = DriverManager.getConnection(
                "jdbc:mysql://" + dbInfo.getHost() + "/" + dbInfo.getName(),
                dbInfo.getUsername(),
                dbInfo.getPassword());
        this.statement = connection.createStatement();
    }

    public int insertIgnore(String tableName, String fieldName, String data) throws Exception
    {
        TableFile tableFile = getTableFileByName(tableName);

        tableFile.populateCurrentRow(fieldName, data);
        int rowId = tableFile.getRowCount();
        tableFile.commitCurrentRow();

        return rowId;
    }

    public void populateCurrentRow(String tableName, String fieldName, String data) throws Exception
    {
        this.tableNameToTableFileObject.get(tableName).populateCurrentRow(fieldName, data);
    }

    public void flush() throws Exception
    {
    }

    private Set<String> parseTableNames(PairEntry pair)
    {
        Set<String> result = new HashSet<String>();
        // main table name is lhs of main pair
        result.add(pair.getLhs().getText());

        ListEntry rhs = (ListEntry) pair.getRhs();

        result.addAll( parseListEntryForTableNames(rhs) );

        return result;
    }

    private Set<String> parseListEntryForTableNames(ListEntry listEntry)
    {
        Set<String> result = new HashSet<String>();

        for (Entry entry : listEntry.getEntries())
        {
            if (entry instanceof PairEntry)
            {
                PairEntry pairEntry = (PairEntry) entry;
                if (pairEntry.getRhs() instanceof TextEntry)
                {
                    String text = ((TextEntry)pairEntry.getRhs()).getText();
                    if (text.matches("\\w+\\.\\w+"))
                    {
                        result.add(text.substring(0, text.indexOf(".")));
                    }
                }
                else if (pairEntry.getRhs() instanceof ListEntry)
                {
                    result.add(pairEntry.getLhs().getText());
                    result.addAll(parseListEntryForTableNames((ListEntry)pairEntry.getRhs()));
                }
            }
            else if (entry instanceof ListEntry)
            {
                result.addAll(parseListEntryForTableNames((ListEntry)entry));
            }
        }
        return result;
    }

    public TableFile getTableFileByName(String tableName) throws Exception
    {
        TableFile tableFile;
        if (!tableNames.contains(tableName))
        {
            tableFile = createTableFile(tableName);
        }
        else
        {
            tableFile = tableNameToTableFileObject.get(tableName);
        }
        return tableFile;
    }

    private TableFile createTableFile(String tableName) throws Exception
    {
        tableNames.add(tableName);
        TableFile tableFile = new TableFile(this, tableName, this.tmpFolder);
        tableNameToTableFileObject.put(tableName, tableFile);
        return tableFile;
    }

    public ResultSet getDescribeTableResustSet(String tableName) throws Exception
    {
        return statement.executeQuery("describe " + tableName);
    }

    public void finalizeImport(Map<String,Set<String>> tableDependencies) throws Exception
    {
        connection.setAutoCommit(false);

        for (TableFile tf: tableNameToTableFileObject.values())
        {
            tf.closeForWriting();
        }

        Map<String,Set<String>> reverseDependencies = new HashMap<String,Set<String>>();

        for (String depender : tableDependencies.keySet())
        {
            for ( String dependee : tableDependencies.get(depender) )
            {
                Set<String> dependers;

                String foreignKeyField = dependee.split("->")[0];
                String foreignTable = dependee.split("->")[1];

                if ( (dependers = reverseDependencies.get(foreignTable)) == null)
                {
                    reverseDependencies.put( foreignTable,(dependers = new HashSet<String>()) );
                }

                dependers.add(depender);

                tablesToForeignKeyField.put( depender + foreignTable , foreignKeyField );
            }
        }

        List<String> tablesToImport = new LinkedList<String>();

        tablesToImport.addAll( this.tableNames );

        long start,end;
        while (!tablesToImport.isEmpty())
        {
            if ( tableDependencies.get(tablesToImport.get(0)) != null )
            {
                Collections.rotate(tablesToImport, -1);
                continue;
            }

            String tableName = tablesToImport.get(0);

            log.info("Importing table: " + tableName);
            start = System.currentTimeMillis();
            if (reverseDependencies.get(tableName) != null)
            {
                int[] ids = insertAndGetIDs(tableName);
                for (String depender : reverseDependencies.get(tableName))
                {
                    String fieldName = tablesToForeignKeyField.get( depender + tableName );

                    adjustIDs(depender, tableName, fieldName, ids);
                    Set<String> dependencies = tableDependencies.get(depender);

                    dependencies.remove(fieldName+"->"+tableName);

                    if (dependencies.size() == 0)
                    {
                        tableDependencies.remove(depender);
                    }
                }
            }
            else
            {
                justInsert(tableName);
            }
            end = System.currentTimeMillis();
            log.info("Imported table in " + Importer.nsecs(start,end) + " seconds");

            tablesToImport.remove(tableName);
        }

        this.statement.close();
        connection.commit();
        connection.setAutoCommit(true);
    }

    private void justInsert(String tableName) throws Exception
    {
        statement.execute( "load data local infile '" + tableNameToTableFileObject.get(tableName).getFile().getPath() + "' ignore into table " + tableName + " character set utf8 ");
    }

    private int[] insertAndGetIDs(String tableName) throws Exception
    {
        TableFile tableFile = tableNameToTableFileObject.get(tableName);
        int[] ids = new int[tableFile.getRowCount()];

        String tmpTableName = tableName + "Tmp";
        String uniqueField = tableFile.getUniqueField();

        statement.execute( "create temporary table " + tmpTableName + " like " + tableName );
        statement.execute( "alter table " + tmpTableName + " add column (TmpId int)" );

        if (uniqueField != null)
        {
            statement.execute( "alter table " + tmpTableName + " drop key " + uniqueField );
        }

        List<String> columns = tableFile.getColumnListing();

        StringBuilder builder = new StringBuilder();

        for (String column : columns)
        {
            if (!column.startsWith("*"))
            {
                builder.append(column+",");
            }
        }
        builder.deleteCharAt( builder.length()-1 );

        String tableColumnList    = "(" + ((tableFile.getAutoIncId()!=null) ? "@dummy," : "") + builder.toString() + ")";
        String tmpTableColumnList = "(" + "TmpId," + builder.toString() + ")";


        statement.execute( "load data local infile '" + tableFile.getFile().getPath() + "' ignore into table " + tableName + " character set utf8 " + " " + tableColumnList);
        statement.execute( "load data local infile '" + tableFile.getFile().getPath() + "' ignore into table " + tmpTableName + " character set utf8 " + " " + tmpTableColumnList);
        

        ResultSet resultSet;

        String idField = tableFile.getAutoIncId();
        if (uniqueField != null)
        {
            String query =
                "SELECT " +
                    "T." + idField + "," +
                    "TT.TmpId " +
                "FROM " +
                    tableName + " T " +
                    "JOIN " + tmpTableName + " TT ON " + "T." + uniqueField + "=" + "TT." + uniqueField +
                " ORDER BY TT.TmpId";

            resultSet = statement.executeQuery(query);
        }
        else
        {
            String createAwkwardJoinTmpTableStatement = "CREATE TEMPORARY TABLE awkward ("+idField+" int, TmpId int, unique key("+idField+"), unique key(TmpId))";

            builder = new StringBuilder();

            for (String fieldName : tableFile.getColumnListing())
            {
                if (!fieldName.startsWith("*"))
                    builder.append(  tableName + "." + fieldName + "=" + tmpTableName + "." + fieldName + " AND ");
            }

            String joinConditions = builder.toString().substring(0, builder.lastIndexOf("AND"));

            String populateAwkwardJoinTmpTableStatement =
                "INSERT IGNORE INTO awkward " +
                " SELECT " +
                    tableName + "." + tableFile.getAutoIncId() + "," +
                    tmpTableName + ".TmpId " +
                " FROM " +
                    tableName + " JOIN " + tmpTableName +
                    " ON " + joinConditions;

            statement.execute( createAwkwardJoinTmpTableStatement );
            statement.execute( populateAwkwardJoinTmpTableStatement );

            resultSet = statement.executeQuery( "SELECT "+idField+",tmpId FROM awkward" );
        }

        while (resultSet.next())
        {
            int tmpId = resultSet.getInt("TmpId");
            ids[ tmpId-1 ] = resultSet.getInt(idField);
        }

        statement.execute( "DROP TABLE IF EXISTS awkward" );
        statement.execute( "DROP TABLE " + tmpTableName );

        return ids;
    }

    private void adjustIDs(String depender, String tableName, String fieldName, int[] ids) throws Exception
    {
        String beforeFilename = depender + "_before";
        File beforeFile = new File(tmpFolder, beforeFilename);
        TableFile tableFile = tableNameToTableFileObject.get(tableName);
        TableFile dependerTableFile = tableNameToTableFileObject.get(depender);
        if (dependerTableFile == null) return;
        File dependerFile = dependerTableFile.getFile();

        dependerFile.renameTo( beforeFile );

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream( beforeFile )));
        FileWriter writer = new FileWriter(dependerFile, true);


        String line;
        int position = dependerTableFile.getColumnListing().indexOf(fieldName);
        String[] fields;
        StringBuilder replacementLineBuilder;
        int id;

        int lineCount = 0;

        while ( (line = reader.readLine()) != null )
        {
            lineCount++;
            fields = line.split("\\t");
            replacementLineBuilder = new StringBuilder();

            for (int i = 0; i < fields.length; i++)
            {

                if (i != position)
                {
                    replacementLineBuilder.append(fields[i] + "\t");
                }
                else
                {
                    try
                    {
                        id = ids[ Integer.parseInt(fields[i])-1 ];
                        replacementLineBuilder.append( id + "\t" );
                    }
                    catch (Exception e)
                    {
                        log.info("file: " + dependerFile);
                        log.info("lineCount: " + lineCount);
                        log.info("line: " + line);
                        log.info("fields: " + fields);
                        log.info("builder: " + replacementLineBuilder.toString());
                        throw e;
                    }
                }
            }
            replacementLineBuilder.deleteCharAt( replacementLineBuilder.lastIndexOf("\t") );

            writer.write( replacementLineBuilder.toString() + "\n" );
        }

        reader.close();
        writer.close();

        beforeFile.delete();
    }
    
    public void killCurrentRow(String tableName)
    {
    }

    public int getCurrentRowId(String tableName) throws Exception
    {
        TableFile tableFile = getTableFileByName(tableName);
        return tableFile.getRowCount();
    }
}
