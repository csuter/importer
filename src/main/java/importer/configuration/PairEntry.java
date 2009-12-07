package importer.configuration;


public class PairEntry implements Entry
{
    private final TextEntry lhs;
    private final Entry rhs;

    public PairEntry(TextEntry lhs, Entry rhs)
    {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    public TextEntry getLhs() { return this.lhs; }
    public Entry getRhs() { return this.rhs; }

    public String toString() { return lhs.toString() + "->" + rhs.toString(); }
}

