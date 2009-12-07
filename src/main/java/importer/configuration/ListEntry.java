package importer.configuration;

import java.util.List;
import java.util.LinkedList;

public class ListEntry implements Entry
{
    private final List<Entry> entries;

    public ListEntry()
    {
        this.entries = new LinkedList<Entry>();
    }

    public void addEntry(Entry entry) { this.entries.add(entry); }
    public Entry getEntry(int index) { return this.entries.get(index); }

    public List<Entry> getEntries() { return this.entries; }
    public String toString()
    {
        StringBuilder builder = new StringBuilder("(");

        for (Entry entry : entries)
        {
            builder.append(entry.toString() + ",");
        }

        builder.deleteCharAt(builder.length() - 1);
        builder.append(")");

        return builder.toString();
    }
}

