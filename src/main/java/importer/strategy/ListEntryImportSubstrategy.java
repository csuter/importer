package importer.strategy;

import importer.database.DataAccessManager;
import importer.configuration.PairEntry;
import importer.configuration.ListEntry;
import importer.configuration.Entry;
import org.apache.log4j.Logger;
import java.util.List;
import java.util.LinkedList;
import java.util.Set;
import java.util.Map;

public class ListEntryImportSubstrategy implements ImportSubstrategy
{
    static Logger log = Logger.getLogger(ListEntryImportSubstrategy.class.getName());
    private final ImportStrategy parentStrategy;

    private final String relationalTableName;
    private final List<ImportSubstrategy> subStrategies;

    private DataAccessManager dataAccessManager;

    public String getName() { return relationalTableName + " (relational)"; }

    public ListEntryImportSubstrategy(ImportStrategy parentStrategy, PairEntry manyToManyReferencePair) throws Exception
    {
        this.parentStrategy = parentStrategy;
        this.dataAccessManager = parentStrategy.getDataAccessManager();

        this.relationalTableName = manyToManyReferencePair.getLhs().getText();
        this.subStrategies = new LinkedList<ImportSubstrategy>();

        ListEntry listEntry = (ListEntry) manyToManyReferencePair.getRhs();
        ImportSubstrategyBuilder builder = new ImportSubstrategyBuilder(this);

        for (Entry entry : listEntry.getEntries())
        {
            subStrategies.add( builder.build(entry) ); 
        }
    }

    public void importEntry(String data) throws Exception
    {
        log.debug("Importing " + relationalTableName);
        data = data.substring(  data.indexOf("{")+1 , data.lastIndexOf("}") ).trim();

        if (data.length() == 0) return;

        int cursor = 0;
        int seek = 0;
        while (seek < data.length())
        {
            this.currentRowId = dataAccessManager.getCurrentRowId(this.relationalTableName);

            for (ImportSubstrategy subStrategy : subStrategies)
            {
                seek = subStrategy.parseEntry(data, cursor);

                String entryText = data.substring(cursor, seek).trim();

                subStrategy.importEntry(entryText);
                cursor = seek;
                if (cursor < data.length() && data.charAt(cursor) == ',') cursor++;
            }

            dataAccessManager.commitCurrentRow(relationalTableName);
        }
    }

    public int parseEntry(String wholeRow, int cursor) throws Exception
    {
        log.debug("Parsing " + relationalTableName);
        int seek = cursor;
        while ( wholeRow.charAt(seek) == ' ' ) seek++;

        seek = findMatchingCurly(wholeRow, seek);
        seek++;

        if (seek<wholeRow.length() && wholeRow.charAt(seek) == ',') seek++;
        return seek;
    }

    private int findMatchingCurly(String string, int cursor) throws Exception
    {
        int pCount = 1;
        int c = cursor;
        while (pCount > 0)
        {
            c++;
            if (c > string.length()) throw new Exception("Error: unclosed parenthesis");
            if (string.charAt(c) == '"')
            {
                c++;
                while (string.charAt(c) != '"') c++;
                c++;
            }
            if (string.charAt(c) == '{' && string.charAt(c-1) != '\\')
            {
                pCount++;
            }
            else if (string.charAt(c) == '}' && string.charAt(c-1) != '\\')
            {
                pCount--;
            }
        }
        return c;
    }
    
    private int currentRowId = 0;
    public int getCurrentRowId() { return this.currentRowId; }

    public void populateCurrentRow(String fieldName, String data) throws Exception
    {
        dataAccessManager.populateCurrentRow(relationalTableName, fieldName, data);
    }


    public ImportStrategy getParentStrategy() { return this.parentStrategy; }
    public DataAccessManager getDataAccessManager() { return this.dataAccessManager; }
    public void finalizeImport() {}

    public void resolveDependenciesRecursively(String currentTable, Map<String,Set<String>> tableDependencies)
    {
        for (ImportSubstrategy subStrategy : subStrategies)
        {
            subStrategy.resolveDependenciesRecursively( this.relationalTableName,tableDependencies );
        }
    }

    public String getAssociatedTableName() { return this.relationalTableName; }
}
