package importer.configuration;


public class TextEntry implements Entry
{
    String text;

    public TextEntry(String text)
    {
        this.text = text;
    }

    public String getText() { return this.text; }

    public String toString() { return this.text; }
}

