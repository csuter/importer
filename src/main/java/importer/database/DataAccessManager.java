package importer.database;

import importer.configuration.Entry;
import java.util.Map;
import java.util.Set;
import java.sql.ResultSet;

public interface DataAccessManager
{
    public void populateCurrentRow(String tableName, String fieldName, String data) throws Exception;
    public int insertIgnore(String tableName, String fieldName, String data) throws Exception;
    public void commitCurrentRow(String tableName);
    public void killCurrentRow(String tableName);
    public void flush() throws Exception;
    public void finalizeImport(Map<String,Set<String>> tableDependencies) throws Exception;
    public ResultSet getDescribeTableResustSet(String tableName) throws Exception;
    public int getCurrentRowId(String tableName) throws Exception;
}
