package importer.configuration;

public class IndirectEntry
{
    private final String localTableFieldName;
    private final String foreignTableName;
    private final String foreignTableFieldName;

    private final boolean generated;

    public IndirectEntry(PairEntry referencePair) throws Exception
    {
        this.localTableFieldName = referencePair.getLhs().getText();

        String foreignTableFieldText = ((TextEntry)referencePair.getRhs()).getText();

        if (foreignTableFieldText.equals("."))
        {
            this.foreignTableName = ".";
            this.foreignTableFieldName = "PRIMARY_KEY_FIELD";
            this.generated = true;
        }
        else
        {
            int dot = foreignTableFieldText.indexOf(".");
            if (    dot == -1 ||
                    dot == 0 ||
                    dot == (foreignTableFieldText.length() - 1) )
            {
                throw new Exception("Error: " + foreignTableFieldText + " is not a valid foreign table field reference. Please enter something like 'Table.fieldname' in your .rel file");
            }
            this.foreignTableName = foreignTableFieldText.substring(0,dot);
            this.foreignTableFieldName = foreignTableFieldText.substring(dot+1);
            this.generated = false;
        }
    }

    public boolean isGenerated() { return this.generated; }
    public String getLocalTableFieldName() { return this.localTableFieldName; }
    public String getForeignTableName() { return this.foreignTableName; }
    public String getForeignTableFieldName() { return this.foreignTableFieldName; }
}
