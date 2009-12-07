package importer.configuration;

import java.util.List;

public class KVList
{
    private ListEntry list;
    private String data;

    private static final String comma = ",";
    private static final String arrow = "->";

    public KVList(String rawData) throws Exception
    {
        this.list = new ListEntry();
        this.data = rawData;
        parseData();
        System.out.println("LIST: " + list + "\n" + "DATA: " + data);
    }

    private void parseData() throws Exception
    {
        this.list = parseList(0, data.length());
    }

    private ListEntry parseList(int startpos, int endpos) throws Exception
    {
        ListEntry result = new ListEntry();

        int cursor = startpos;
        int seek = startpos;
        int length = endpos - startpos;

        int nextcomma = 0;
        int nextarrow = 0;

        // each pass of the loop assumes that the
        // cursor is on the first character of an
        // Entry which may be either a single text
        // element, a list element, or a key value
        // pair. 
        //
        // if the Entry is simple text, there will
        // be some text followed by a comma or an
        // eof before anything else (arrows, parens)
        //
        // if the Entry is a standalone list (not in
        // a kvp rhs) there will be a parenthetical
        // expression, possibly followed by more entries
        //
        // if the Entry is a kvp, there will be a
        // text Entry followed by an arrow before
        // anything else
        String text;
        while (cursor < endpos)
        {
            // if there is a paren, handle it
            // otherwise, it's text
            // first find the next non-text char (comma,
            // arrow, or eof) and grab the text preceding it.
            
            if (data.charAt(cursor) == '(')
            {
                int rightParen = findMatchingParen(cursor);
                ListEntry entry = parseList(++cursor, rightParen);
                cursor = rightParen + 1;
                result.addEntry(entry);
                if (data.charAt(cursor) == ',')
                {
                    cursor++;
                    continue;
                }
            }
            else
            {
                // parse text element and advance cursor
                text = parseText(cursor, endpos);
                cursor += text.length();

                if (cursor == endpos || cursor >= data.length())
                {
                    result.addEntry(new TextEntry(text));
                    break;
                }

                if (data.substring(cursor,cursor+2).equals(arrow))
                {
                    // it's a kvp
                    TextEntry lhs = new TextEntry(text);
                    Entry rhs;

                    // skip past the arrow...
                    cursor += 2;

                    if (data.charAt(cursor) == '(')
                    {
                        // find the matching paren
                        int rightParen = findMatchingParen(cursor);
                        // advance cursor to first char after left
                        // paren and recurse
                        rhs = parseList(++cursor, rightParen);
                        cursor = rightParen + 1;
                    }
                    else
                    {
                        String rhstext = parseText(cursor, endpos);
                        rhs = new TextEntry(rhstext);
                        cursor += rhstext.length();
                    }

                    PairEntry pair = new PairEntry( lhs, rhs );
                    result.addEntry(pair);

                    if (cursor >= data.length()) break;

                    if (data.charAt(cursor) == ',')
                    {
                        cursor++;
                        continue;
                    }
                }
                else if (data.charAt(cursor) == ',')
                {
                    result.addEntry(new TextEntry(text));
                    cursor++;
                    continue;
                }
            }

        }
        return result;
    }

    private int findMatchingParen(int cursor) throws Exception
    {
        int pCount = 1;
        int c = cursor;
        while (pCount > 0)
        {
            c++;
            if (c > data.length()) throw new Exception("Error: unclosed parenthesis");
            if (data.charAt(c) == '(')
            {
                pCount++;
            }
            else if (data.charAt(c) == ')')
            {
                pCount--;
            }
        }
        return c;
    }

    private String parseText(int cursor, int endpos)
    {
        int seek = 0;

        int nextcomma = data.indexOf(comma, cursor);
        int nextarrow = data.indexOf(arrow, cursor);

        if (nextarrow == -1 && nextcomma == -1)
        {
            return data.substring(cursor, endpos).trim();
        }

        // one and only one of these must be true:
        if (nextcomma < nextarrow) seek = nextcomma;
        if (nextarrow < nextcomma) seek = nextarrow;
        if (nextcomma == -1) seek = nextarrow;
        if (nextarrow == -1) seek = nextcomma;

        if (seek > endpos) seek = endpos;
        
        String result = data.substring(cursor, seek).trim();
        return result;
    }
    public ListEntry getList() { return this.list; }
}
