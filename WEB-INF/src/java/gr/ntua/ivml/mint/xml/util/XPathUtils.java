package gr.ntua.ivml.mint.xml.util;

import java.util.Map;

import gr.ntua.ivml.mint.util.StringUtils;

public class XPathUtils {
	public static String difference(String xpath1, String xpath2) {
        String[] paths1 = split(xpath1);
        String[] paths2 = split(xpath2);
        int length = Math.min(paths1.length, paths2.length);
        int index = 0;
        while (index < length && paths1[index].equals(paths2[index])) {
            index++;
        }
        StringBuffer b = new StringBuffer();
        for (int i = 0; i < index; i++) {
            b.append(paths1[i]);
        }
        while (index < paths1.length) {
            b.append(paths1[index++]);
            b.append('/');
        }
        return b.toString();
    }
    
    public static String[] split(String xpath) {
        String[] paths = xpath.split("/");
        if (paths.length == 0) {
            paths = new String[1];
            paths[0] = "";
        }
        return paths;
    }
    
    public static int levelChange(String xpath1, String xpath2) {
        String[] paths1 = split(xpath1);
        String[] paths2 = split(xpath2);
        int length = Math.min(paths1.length, paths2.length);
        int index = 0;
        while (index < length && paths1[index].equals(paths2[index])) {
            index++;
        }
        return index;
    }
    
    public static String getLevel(String xpath, int level) {
        String[] paths = split(xpath);
        StringBuffer b = new StringBuffer();
        for (int i = 0; i < level; i++) {
            b.append(paths[i]);
            b.append(' '); // compensates the slash
        }
        b.append(paths[level]);
        b.append('/');
        return b.toString();
    }
    
    public static int getDepth(String value) {
        int len = value.length();
        int levels = 0;
        char[] chars = new char[len];
        value.getChars(0,len,chars,0);
        for (int i = 0; i < len; i++) {
            if (chars[i] == '/' && (i+1 < len) && chars[i+1] != '/') {
                levels++;
            }
        }
        return levels;
    }
    
    public static String getNamespacePrefix(String qname) {
        int i = qname.indexOf(':');
        return (i == -1) ? "" : qname.substring(0,i);
    }
    
    // replace prefixes with {url}
    // map is prefix -> url
    public static String expandNamespaces( String xpath, Map<String, String> prefixes ) {
    	StringBuffer sb = new StringBuffer();
    	String[] parts = xpath.split( "/",-1);
    	for( String part:parts ) {
    		String[] locals = part.split( ":" );
    		if( locals.length ==2 ) {
    			if( locals[0].startsWith("@"))
    				sb.append( "@{" + prefixes.get( locals[0].substring(1) ) + "}:" + locals[1] + "/" );
    			else
    				sb.append( "{" + prefixes.get( locals[0] ) + "}:" + locals[1] + "/" );
    		} else 
    			sb.append(part+"/");
    	}
    	sb.delete( sb.length()-1, sb.length());
    	return sb.toString();
    }
    
    public static String changePrefixes( String oldPath, Map<String, String> translatePrefixes ) {
    	String[] split = oldPath.split( "/", -1 );
    	StringBuilder res=  new StringBuilder();
    	
    	for( String elem: split ) {
    		String[] locals = elem.split( ":" );
    		if( locals.length ==2 ) {
    			if( locals[0].startsWith("@")) {
    				String oldPrefix = locals[0].substring(1);
    				if( translatePrefixes.containsKey(oldPrefix))
    					res.append( "@" + translatePrefixes.get( oldPrefix )  + ":" + locals[1] + "/" );
    				else
    					res.append( elem + "/" );
    			} else {
    				if( translatePrefixes.containsKey(locals[0]))
    					res.append( translatePrefixes.get( locals[0] )  + ":" + locals[1] + "/" );
    				else
    					res.append( elem + "/" );
    			}
    		} else 
    			res.append(elem + "/");
    	}
    	// remove last "/"
    	res.delete( res.length()-1, res.length());
    	return res.toString();
    }
}
