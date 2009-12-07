package importer.configuration;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
//import java.util.HashSet;

public class RelationshipList
{
    String rawRelationshipList;

    HashMap<String,String> relationships;

    public RelationshipList(String rawRelationshipList)
    {
        this.rawRelationshipList = rawRelationshipList;

        // trim parens, split on the commas
        String[] kvps = rawRelationshipList.substring(
                rawRelationshipList.indexOf("(") + 1,
                rawRelationshipList.lastIndexOf(")")).split( "\\s*,\\s*");

        this.relationships = new HashMap<String,String>(kvps.length);

        String[] kvpAry;
        for (String kvp : kvps)
        {
            kvpAry = kvp.split("\\s*->\\s*");
            relationships.put(
                    kvpAry[0].trim(),
                    kvpAry[1].trim());
        }
    }

    public Set<String> getSchemaInfo()
    {
        Set<String> result = new HashSet<String>();

        result.addAll( relationships.keySet() );
        result.addAll( relationships.values() );

        return result;
    }

    public String getRawRelationshipList() { return this.rawRelationshipList; }
    public String toString() { return "RelationshipList { " + this.relationships + " }"; }

    public HashMap<String,String> getRelationships() { return this.relationships; }
}
