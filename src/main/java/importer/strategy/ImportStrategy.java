package importer.strategy;

import importer.database.DataAccessManager;
import java.util.Map;
import java.util.Set;

public interface ImportStrategy
{
    public ImportStrategy getParentStrategy();
    public void importEntry(String wholeRow) throws Exception;
    public DataAccessManager getDataAccessManager();
    public void populateCurrentRow(String fieldName, String data) throws Exception;
    public int getCurrentRowId();
    public void finalizeImport() throws Exception;
    public void resolveDependenciesRecursively(String currentTable, Map<String,Set<String>> tableDependencies);
    public String getAssociatedTableName();
}
