package importer.configuration;


public class InputFormat
{
    private Entry mainEntry;

    public InputFormat(Entry entry)
    {
        this.mainEntry = entry;
    }
    public Entry getMainEntry() { return this.mainEntry; }
}
