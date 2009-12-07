package importer.strategy;

public interface ImportSubstrategy extends ImportStrategy
{
    public ImportStrategy getParentStrategy();
    public int parseEntry(String wholeRow, int cursor) throws Exception;
    public String getName();
}
