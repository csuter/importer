package importer.configuration;

public class DatabaseInfo
{
    private String name;
    private String host;
    private String port;
    private String username;
    private String password;

    public DatabaseInfo(ListEntry listEntry) throws Exception
    {

        for (Entry entry : listEntry.getEntries())
        {
            if (entry instanceof PairEntry)
            {
                PairEntry pair = (PairEntry) entry;
                String lhsText = pair.getLhs().getText();

                if (lhsText.equals("Name"))
                {
                    this.name = ((TextEntry)pair.getRhs()).getText();
                }
                else if (lhsText.equals("Host"))
                {
                    this.host = ((TextEntry)pair.getRhs()).getText();
                }
                else if (lhsText.equals("Port"))
                {
                    this.port = ((TextEntry)pair.getRhs()).getText();
                }
                else if (lhsText.equals("Username"))
                {
                    this.username = ((TextEntry)pair.getRhs()).getText();
                }
                else if (lhsText.equals("Password"))
                {
                    this.password = ((TextEntry)pair.getRhs()).getText();
                }
            }
            else throw new Exception("Error reading input: " + entry);
        }
    }
    public String getName() { return this.name; }
    public String getHost() { return this.host; }
    public String getPort() { return this.port; }
    public String getUsername() { return this.username; }
    public String getPassword() { return this.password; }

    public String toString()
    {
        StringBuilder builder = new StringBuilder("DatabaseInfo:\n");
        builder.append("\tHost: " + this.host + "\n");
        builder.append("\tName: " + this.name + "\n");
        builder.append("\tPort: " + this.port + "\n");
        builder.append("\tUsername: " + this.username + "\n");
        builder.append("\tPassword: " + this.password + "\n");

        return builder.toString();
    }
}
