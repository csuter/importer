package importer.strategy;

import importer.configuration.Entry;
import importer.configuration.TextEntry;
import importer.configuration.PairEntry;
import importer.configuration.ListEntry;
import importer.configuration.IndirectEntry;
import org.apache.log4j.Logger;

public class ImportSubstrategyBuilder
{
    static Logger log = Logger.getLogger(ImportSubstrategyBuilder.class.getName());
    private final ImportStrategy importStrategy;

    public ImportSubstrategyBuilder(ImportStrategy strategy)
    {
        this.importStrategy = strategy;
    }

    public ImportSubstrategy build(Entry entry) throws Exception
    {
        if (entry instanceof TextEntry)
        {
            return new DirectEntryImportSubstrategy(importStrategy,(TextEntry)entry);
        }
        else if (entry instanceof PairEntry)
        {
            PairEntry pair = (PairEntry) entry;
            Entry rhs = pair.getRhs();
            if (rhs instanceof TextEntry)
            {
                return new IndirectEntryImportSubstrategy(importStrategy, pair);
            }
            else if (rhs instanceof ListEntry)
            {
                return new ListEntryImportSubstrategy(importStrategy,pair);
            }
        }
        else if (entry instanceof ListEntry)
        {
            return new StructuralListEntryImportSubstrategy(importStrategy,(ListEntry)entry);
        }
        throw new Exception("Couldn't understand entry: " + entry);
    }
}
