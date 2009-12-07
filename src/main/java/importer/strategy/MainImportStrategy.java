package importer.strategy;

import importer.strategy.MainImportStrategy;
import importer.database.DataAccessManager;
import importer.configuration.Entry;
import importer.configuration.PairEntry;
import importer.configuration.ListEntry;
import importer.configuration.InputFormat;
import importer.configuration.InputFile;
import org.apache.log4j.Logger;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

public class MainImportStrategy implements ImportStrategy
{
    static Logger log = Logger.getLogger(MainImportStrategy.class.getName());
    private InputFile inputFile;

    private List<ImportSubstrategy> subStrategies;
    private DataAccessManager dataAccessManager;
    private String mainTableName;

    public MainImportStrategy(DataAccessManager dataAccessManager, InputFile inputFile) throws Exception
    {
        this.dataAccessManager = dataAccessManager;
        this.inputFile = inputFile;

        init();
    }

    private void init() throws Exception
    {
        InputFormat inputFormat = inputFile.getInputFormat();
        Entry mainEntry = inputFormat.getMainEntry();

        if (mainEntry instanceof PairEntry)
        {
            PairEntry mainTableInfo = (PairEntry) mainEntry;

            this.mainTableName = mainTableInfo.getLhs().getText();

            ListEntry tableEntries = (ListEntry) mainTableInfo.getRhs();
            this.subStrategies = new LinkedList<ImportSubstrategy>();
            ImportSubstrategyBuilder builder = new ImportSubstrategyBuilder(this);

            for (Entry entry : tableEntries.getEntries())
            {
                subStrategies.add( builder.build(entry) );
            }
        }
        else
        {
            throw new Exception("Error in file syntax...");
        }
    }

    public void importEntry(String wholeRow)
    {
        int cursor = 0;
        int seek = 0;

        try
        {
            this.currentRowId = dataAccessManager.getCurrentRowId(mainTableName);

            for (ImportSubstrategy subStrategy : subStrategies)
            {
                seek = subStrategy.parseEntry(wholeRow, cursor);

                String entryText = wholeRow.substring(cursor, seek).trim();

                subStrategy.importEntry(entryText);
                cursor = seek;
            }

            dataAccessManager.commitCurrentRow(mainTableName);
        }
        catch (Exception e)
        {
            log.error("Error parsing input: " + wholeRow, e);
            dataAccessManager.killCurrentRow(mainTableName);
        }
    }

    private int currentRowId = -1;
    public int getCurrentRowId() { return this.currentRowId; }

    public void populateCurrentRow(String fieldName, String data) throws Exception
    {
        dataAccessManager.populateCurrentRow(mainTableName, fieldName, data);
    }

    public DataAccessManager getDataAccessManager() { return this.dataAccessManager; }
    public ImportStrategy getParentStrategy() { return null; }

    public void finalizeImport() throws Exception
    {
        Map<String,Set<String>> tableDependencies = new HashMap<String,Set<String>>();

        for (ImportSubstrategy subStrategy : subStrategies)
        {
            subStrategy.resolveDependenciesRecursively( this.mainTableName,tableDependencies );
        }

        dataAccessManager.finalizeImport(tableDependencies);
    }

    public void resolveDependenciesRecursively(String currentTable, Map<String,Set<String>> tableDependencies)
    {
    }

    public String getAssociatedTableName() { return this.mainTableName; }
}
