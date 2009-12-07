package importer.strategy;

import importer.database.DataAccessManager;
import importer.configuration.TextEntry;
import importer.configuration.Entry;
import org.apache.log4j.Logger;
import java.util.Set;
import java.util.Map;

public class DirectEntryImportSubstrategy implements ImportSubstrategy
{
    static Logger log = Logger.getLogger(DirectEntryImportSubstrategy.class.getName());
    private final ImportStrategy parentStrategy;
    private final String directEntryField;

    public String getName() {return directEntryField;}

    public DirectEntryImportSubstrategy(ImportStrategy parentStrategy, TextEntry directEntryField)
    {
        this.parentStrategy = parentStrategy;

        // ditch the object; just keep the string
        this.directEntryField = directEntryField.getText();
    }

    public void importEntry(String data) throws Exception
    {
        log.debug("Importing " + directEntryField);
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

        parentStrategy.populateCurrentRow(this.directEntryField, data);
    }

    public int parseEntry(String wholeRow, int cursor) throws Exception
    {
        log.debug("Parsing " + directEntryField);
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
            seek++;
            if (seek<wholeRow.length() && wholeRow.charAt(seek) == ',') seek++;
            return seek;
        }

        while (true)
        {
            if (seek == wholeRow.length()) break;
            if (
                    wholeRow.charAt(seek) == ' '
                    || ( wholeRow.charAt(seek) == '{'  && wholeRow.charAt(seek-1) != '\\')
                    || ( wholeRow.charAt(seek) == ',' && wholeRow.charAt(seek-1) != '\\')
                )
            {
                break;
            }
            seek++;
        }
        if (seek<wholeRow.length() && wholeRow.charAt(seek) == ',') seek++;
        return seek;
    }

    public ImportStrategy getParentStrategy() { return this.parentStrategy; }
    public DataAccessManager getDataAccessManager() { return this.parentStrategy.getDataAccessManager(); }
    public int getCurrentRowId() { return 0; }
    public void populateCurrentRow(String fieldName, String data) { }
    public void finalizeImport() {}
    public void resolveDependenciesRecursively(String currentTable, Map<String,Set<String>> tableDependencies) {}
    public String getAssociatedTableName() { return null; }
}
