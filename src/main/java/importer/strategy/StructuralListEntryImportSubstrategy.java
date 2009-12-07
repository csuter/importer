package importer.strategy;

import importer.database.DataAccessManager;
import importer.configuration.ListEntry;
import importer.configuration.Entry;
import org.apache.log4j.Logger;
import java.util.List;
import java.util.LinkedList;
import java.util.Set;
import java.util.Map;

public class StructuralListEntryImportSubstrategy implements ImportSubstrategy
{
    static Logger log = Logger.getLogger(StructuralListEntryImportSubstrategy.class.getName());
    private final ImportStrategy parentStrategy;


    private DataAccessManager dataAccessManager;
    private final List<ImportSubstrategy> subStrategies;

    public String getName() {return "(structural)";}

    public StructuralListEntryImportSubstrategy(ImportStrategy parentStrategy, ListEntry structuralList) throws Exception
    {
        this.parentStrategy = parentStrategy;
        this.dataAccessManager = parentStrategy.getDataAccessManager();

        this.subStrategies = new LinkedList<ImportSubstrategy>();

        // set my parent as the parent for subs...i'm just a place holder.
        ImportSubstrategyBuilder builder = new ImportSubstrategyBuilder(parentStrategy);

        for (Entry entry : structuralList.getEntries())
        {
            subStrategies.add( builder.build(entry) ); 
        }
    }

    public void importEntry(String data)
    {
        data = data.substring(  data.indexOf("{")+1 , data.lastIndexOf("}") ).trim();
        if (data.length() == 0) return;

        int cursor = 0;
        int seek = 0;
        for (ImportSubstrategy subStrategy : subStrategies)
        {
            try
            {
                seek = subStrategy.parseEntry(data, cursor);


                String entryText = data.substring(cursor, seek).trim();


                subStrategy.importEntry(entryText);
                cursor = seek;
                if (cursor < data.length() && data.charAt(cursor) == ',') cursor++;
            }
            catch (Exception e)
            {
                log.error("Error parsing input: " + data, e);
            }
        }

    }

    public int parseEntry(String wholeRow, int cursor) throws Exception
    {
        int seek = cursor;

        while ( wholeRow.charAt(seek) == ' ' ) seek++;

        seek = findMatchingCurly(wholeRow, seek);

        return seek+1;
    }

    private int findMatchingCurly(String string, int cursor) throws Exception
    {
        int pCount = 1;
        int c = cursor;
        while (pCount > 0)
        {
            c++;
            if (c > string.length()) throw new Exception("Error: unclosed curly brace:");
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

    public ImportStrategy getParentStrategy() { return this.parentStrategy; }
    public DataAccessManager getDataAccessManager() { return this.dataAccessManager; }
    public void populateCurrentRow(String fieldName, String data) throws Exception
    {
        parentStrategy.populateCurrentRow(fieldName, data);
    }
    public int getCurrentRowId() { return 0; }
    public void finalizeImport() {}

    public void resolveDependenciesRecursively(String currentTable, Map<String,Set<String>> tableDependencies)
    {
        for (ImportSubstrategy subStrategy : subStrategies)
        {
            subStrategy.resolveDependenciesRecursively( currentTable,tableDependencies );
        }
    }

    public String getAssociatedTableName() { return parentStrategy.getAssociatedTableName(); }
}
