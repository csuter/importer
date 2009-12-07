package importer.strategy;

import importer.database.DataAccessManager;
import importer.configuration.PairEntry;
import importer.configuration.IndirectEntry;
import importer.configuration.Entry;
import org.apache.log4j.Logger;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;

public class IndirectEntryImportSubstrategy implements ImportSubstrategy
{
    static Logger log = Logger.getLogger(IndirectEntryImportSubstrategy.class.getName());
    private final ImportStrategy parentStrategy;
    private final IndirectEntry indirectEntry;

    private DataAccessManager dataAccessManager;

    public String getName() {return indirectEntry.getForeignTableName() + "." + indirectEntry.getForeignTableFieldName();}

    public IndirectEntryImportSubstrategy(ImportStrategy parentStrategy, PairEntry referencePair) throws Exception
    {
        this.parentStrategy = parentStrategy;
        this.dataAccessManager = parentStrategy.getDataAccessManager();

        // ditch the object; just keep the string
        this.indirectEntry = new IndirectEntry(referencePair);
    }

    public void importEntry(String data) throws Exception
    {
        log.debug("Importing " + indirectEntry.getForeignTableName() + "." + indirectEntry.getForeignTableFieldName());
        int position = data.indexOf("\"");
        if (position != -1)
        {
            int c = position+1;
            while ( data.charAt(c) != '\"' || ( data.charAt(c) == '\"' && data.charAt(c-1) == '\\' ))
            {
                c++;
                if (c > data.length()) throw new Exception("Error: unclosed quote");
            }
            data = data.substring ( data.indexOf("\"") + 1, c );
            data = data.replaceAll("\\\\([\\{\\}\"])", "$1");
        }

        int indirectElementId;
        if ((indirectEntry.getForeignTableName().length() == 1) && indirectEntry.getForeignTableName().startsWith("."))
        {
            indirectElementId = parentStrategy.getParentStrategy().getCurrentRowId();
        }
        else
        {
            indirectElementId = dataAccessManager.insertIgnore(indirectEntry.getForeignTableName(), indirectEntry.getForeignTableFieldName(), data);
        }

        parentStrategy.populateCurrentRow(indirectEntry.getLocalTableFieldName(), ""+indirectElementId);
    }

    public int parseEntry(String wholeRow, int cursor) throws Exception
    {
        log.debug("Parsing " + indirectEntry.getForeignTableName() + "." + indirectEntry.getForeignTableFieldName());
        if ( indirectEntry.isGenerated() ) return cursor;
        int seek = cursor;
        while ( wholeRow.charAt(seek) == ' ' ) seek++;

        cursor = seek;
        if (wholeRow.charAt(seek) == '"')
        {
            do
            {
                seek++;
                if (seek == wholeRow.length()) throw new Exception("Unmatched \" at " + cursor + ":" + wholeRow);
            }
            while (wholeRow.charAt(seek) != '\"' || (wholeRow.charAt(seek) == '\"' && wholeRow.charAt(seek-1) == '\\'));
            if (seek<wholeRow.length() && wholeRow.charAt(seek) == ',') seek++;
            return seek;
        }

        while (true)
        {
            if (seek == wholeRow.length()) break;
            if (wholeRow.charAt(seek) == ' ' || ( wholeRow.charAt(seek) == '{'  && wholeRow.charAt(seek-1) != '\\'))
            {
                break;
            }
            seek++;
        }
        if (seek<wholeRow.length() && wholeRow.charAt(seek) == ',') seek++;
        return seek;
    }

    public ImportStrategy getParentStrategy() { return this.parentStrategy; }
    public DataAccessManager getDataAccessManager() { return this.dataAccessManager; }
    public int getCurrentRowId() { return -1; }
    public void populateCurrentRow(String fieldName, String data) {}
    public void finalizeImport() {}
    public void resolveDependenciesRecursively(String currentTable, Map<String,Set<String>> tableDependencies)
    {
        Set<String> dependencies;
        if ( (dependencies = tableDependencies.get(currentTable)) == null )
            tableDependencies.put( currentTable,(dependencies=new HashSet<String>()) );

        if ((indirectEntry.getForeignTableName().length() == 1) && indirectEntry.getForeignTableName().startsWith("."))
        {
            dependencies.add( indirectEntry.getLocalTableFieldName() + "->" + parentStrategy.getParentStrategy().getAssociatedTableName());
        }
        else if (!indirectEntry.getForeignTableName().isEmpty())
        {
            dependencies.add( indirectEntry.getLocalTableFieldName() + "->" + indirectEntry.getForeignTableName());// + "." + indirectEntry.getForeignTableFieldName());
        }
    }

    public String getAssociatedTableName() { return indirectEntry.getForeignTableName(); }
}
