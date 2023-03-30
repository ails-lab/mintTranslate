package gr.ntua.ivml.mint.persistent;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.jena.util.FileUtils;
import org.apache.log4j.Logger;
import org.xml.sax.XMLReader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import gr.ntua.ivml.mint.f.ThrowingConsumer;
import gr.ntua.ivml.mint.util.Config;
import gr.ntua.ivml.mint.util.Trie;
import gr.ntua.ivml.mint.util.XMLUtils;
import nu.xom.Builder;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Elements;
import nu.xom.Node;
import nu.xom.Nodes;
import nu.xom.ParentNode;
import nu.xom.Serializer;
import nu.xom.Text;

public class Enrichment {
    private static final Logger log = Logger.getLogger(Enrichment.class);
    
    // insert elements in the enrich xsl ... local namespace
    private static final String ENRICH_CSV = "csv";
    // match the input data into a variable
    private static final String ENRICH_MATCH_VALUE = "matchValue";
    // define here a variable parentPathString for the position of the insert fragments
    private static final String ENRICH_PARENT_PATH = "parentPathString";
    
    // create the inserts variable here with all relevant rows from the csv eg $csv/csv/row[col[1]=$matchValue]
    // or $csv/csv/row[matches( col[1],  concat( '_',$matchValue,'$'))] if the csv has some bs in front of the id
    private static final String ENRICH_INSERT_ROWS = "insertRows";
    
    // put the insert xml fragment here, use $row variable to create values
    private static final String ENRICH_INSERT_XML = "insertXml";
    
    // a list of strings into the variable names that list element QNames before which the insert has to happen
    // if the list is empty, insert at beginning. If we dont faind any of the names, insert at the end.
    private static final String ENRICH_BEFORE_ELEMENTS = "beforeElements";
    
    private Long dbID;
    private String name;
    private String headers;
    private User creator;
    private Organization organization;
    private Long bytesize;
    private Long lineCount;
    public List<Integer> projectIds = new ArrayList<>();
    private byte[] gzippedCsv;
    private Date creationDate;
    private Date lastModified;

    /**
     * Convenience class to allow code to run on one  or more rows of the csv file, based on a lookup
     * key. builder = Enrichment.getEnrichBuilder( indx col )
     * builder.enrich( "some lookup value", row -> { what you want to do with any matching row in csv } )
     * reuse the builder, it is keeping all data in memory. 
     * @author arne
     *
     */
    public static class EnrichBuilder {
		public Map<String,List<String[]>> enrichmentIndex;

		public EnrichBuilder( Enrichment e, int idx ) {
			enrichmentIndex = e.asHash(idx);
		}
		
		public void enrich( String key, ThrowingConsumer< String[]> rowProcessor ) {
			try {
				List<String[]> rows = enrichmentIndex.get(key);
				if( rows == null ) return;
				for( String[] row: rows )
					rowProcessor.accept(row);
			} catch( Exception e ) {
				log.debug( "Row processor threw exception" ,e );
			}
		}
		
		// for classic loops
		// for( String[] row: enrich.getWithKey( key )) {
		//   something with row[] }
		public List<String[]> getWithKey( String key ) {
			List<String[]> rows = enrichmentIndex.get(key);
			if( rows == null ) return Collections.emptyList();
			return rows;
		}
		
		// What is better? Not sure, but the latter again is easier to read, less hidden magic.
	}
	
    public static class EnrichBuilderTrie {
 		public Trie<List<String[]>> enrichmentIndex;
 		float threshhold;
 		
 		public EnrichBuilderTrie( Enrichment e, int idx ) {
 			this( e, idx, 0.999f );
 		}
 		
 		// only accept hits with >threshhold confidence 		
 		public EnrichBuilderTrie( Enrichment e, int idx, float threshhold ) {
 			enrichmentIndex = new Trie<>();
 			e.asHash(idx).forEach( ( str, list ) -> enrichmentIndex.insertValue(str, list));
 			this.threshhold = threshhold;
 		}
 		
 		public void enrich( String key, ThrowingConsumer< String[]> rowProcessor ) {
 			try {
 				Optional<Trie.TrieLookup<List<String[]>>> optRes = enrichmentIndex.lookup(key);
 				if( !optRes.isPresent()) return;
 				if( optRes.get().confidence < threshhold ) return;
 				
 				List<String[]> rows = optRes.get().result;
 				if( rows == null ) return;
 				for( String[] row: rows )
 					rowProcessor.accept(row);
 			} catch( Exception e ) {
 				log.debug( "Row processor threw exception" ,e );
 			}
 		}
 		
 		// for classic loops
 		// for( String[] row: enrich.getWithKey( key )) {
 		//   something with row[] }
 		public List<String[]> getWithKey( String key ) {
			Optional<Trie.TrieLookup<List<String[]>>> optRes = enrichmentIndex.lookup(key);
			if( !optRes.isPresent()) return Collections.emptyList();
			if( optRes.get().confidence < threshhold ) return Collections.emptyList();
			
			List<String[]> rows = optRes.get().result;
			if( rows == null ) return Collections.emptyList();
 			return rows;
 		}
 		
 		// What is better? Not sure, but the latter again is easier to read, less hidden magic.
 	}
 	

    public boolean initialize (File tmp, String enrichmentName, User u) throws IOException {
        //extract headers
        this.setCreator(u);
        this.setOrganization(u.getOrganization());
        this.setCreationDate(new Date());
        this.setLastModified(new Date());
        this.setName(enrichmentName);

        Reader read = FileUtils.asBufferedUTF8( new FileInputStream( tmp ));
        
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(read);
        CSVRecord header = records.iterator().next();
        this.setHeaders(header.toString());

//        int headerCount = header.size();
        Files.deleteIfExists(tmp.toPath());
        tmp = File.createTempFile("tmpResult", ".csv");

        long lineCount = 0;
        try (CSVPrinter printer = new CSVPrinter(FileUtils.asUTF8( new FileOutputStream(tmp)), CSVFormat.DEFAULT)) {
            for (CSVRecord record : records) {
                lineCount++;
                printer.printRecord(record);
            }
        } catch (IOException ex) {
            log.error( "Reading supplied CSV failed", ex);
            return false;
        }
        this.setLineCount(lineCount);

        byte[] gzip = makeGzippedCsv(tmp);
        // make gzipped blob
        this.setGzippedCsv(gzip);
        this.setBytesize((long)gzip.length);

        Files.deleteIfExists(tmp.toPath());
        return true;
    }
    
    public byte[] makeGzippedCsv (File tmp) {
        ByteArrayOutputStream baos = null;
        GzipCompressorOutputStream gz = null;

        try {
            baos = new ByteArrayOutputStream();
            gz = new GzipCompressorOutputStream(baos);
            gz.write(Files.readAllBytes(tmp.toPath()));
            gz.flush();
            gz.close();
        }
        catch ( Exception e ) {
            log.error( "Unexpected Error on gzipping content", e );
        }
        finally {
            try {if( gz!= null ) gz.close();} catch( Exception e ){}
            try {if( baos!= null ) baos.close();} catch( Exception e ){}
        }
        return baos.toByteArray();
    }
    
    /**
     * Create an XSL file from the config. 
     * Initially we assume some form of exact match happens with some unordered insert of xml fragment
     *  
     * @param config matchPath - which xpath 
     * @param namespaces  prefix to uri map of available namespaces for the match and insert statements 
     * @return
     */
    public Optional<String> getXsl( ArrayNode config, Map<String, String> namespaces ) throws Exception {
    	
    	Optional<Document> resultXslDom = Optional.empty();
    	try {
	    	XMLReader parser = org.xml.sax.helpers.XMLReaderFactory.createXMLReader(); 
	    	parser.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
	
	    	Builder builder = new Builder(parser);
	    	
	    	Document dom = builder.build( Config.getProjectFile("WEB-INF/xsl/enrich.xsl"));
	    	addNamespaces( dom, namespaces );

	    	insertCsv( dom );
	    	
	    	matchingRows( config, dom, builder, namespaces );
	    	insertValues( config, dom, builder, namespaces );
	    	
	    	if( log.isDebugEnabled() ) {
	    		ByteArrayOutputStream bos = new ByteArrayOutputStream();
	    		Serializer s = new Serializer( bos, "UTF-8");
	    		s.setIndent(2);
	    		s.write( dom );
	    		String output = new String( bos.toByteArray(), "UTF-8");
	    		log.debug( "Enrich XSL:\n\n" + output + "\n\n");
	    	}
	    	resultXslDom = Optional.of( dom );

    	} catch( Exception e ) {
    		log.error( "Enrich XSL generation failed.", e );
    		throw e;
    	}
    	
    	// Optional Document becomes Optional String 
    	return resultXslDom.map( Document::toXML );
    }
    
     // get the config for the matching column out, find the macthing rows in the cvs
    private void matchingRows( ArrayNode config, Document dom, Builder builder, Map<String, String> namespaces ) throws Exception {
    	for( JsonNode node: config ) {
    		String type = node.get("type").asText();
			int colNum = node.get("col").intValue();
    		if( type.equals( "exact")) {
    			String matchPath = node.get( "matchPath").asText();
    	    	replaceFragment( ENRICH_MATCH_VALUE, "<xsl:variable name=\"matchValue\" select=\"" + matchPath + "\" />", dom, builder, namespaces );
    	    	replaceFragment( ENRICH_INSERT_ROWS, "<xsl:variable name=\"inserts\" select=\"$csv/csv/row[ col["+ colNum +"] = $matchValue]\" />", dom, builder, namespaces );
    			break;
    		}

    		if( type.equals( "xpathMatch")) {
    			String matchExpression = node.get( "matchExpression").asText();
    			String matchPath = node.get( "matchPath").asText();
    	    	replaceFragment( ENRICH_MATCH_VALUE, "<xsl:variable name=\"matchValue\" select=\"" + matchPath + "\" />", dom, builder, namespaces );
    	    	replaceFragment( ENRICH_INSERT_ROWS, "<xsl:variable name=\"inserts\" select=\"" + XMLUtils.xmlAttributeValueEscape(matchExpression) + "\" />", dom, builder, namespaces );
    			break;
    		}
    		if( type.equals( "xpathKey")) {
    			String matchExpression = node.get( "matchExpression").asText();
    			String matchPath = node.get( "matchPath").asText();
    	    	replaceFragment( ENRICH_MATCH_VALUE, "<xsl:variable name=\"matchValue\" select=\"" + matchPath + "\" />", dom, builder, namespaces );
    	    	replaceFragment( ENRICH_INSERT_ROWS, "<xsl:key name=\"enrichKey\" match=\"row\" use=\"col[" 
    	    	  + colNum + "]\" /> \n  <xsl:variable name=\"inserts\"" 
    	    	+ " select=\"key( 'enrichKey'," + XMLUtils.xmlAttributeValueEscape(matchExpression) + ", $csv )\" /> "
    	    	  , dom, builder, namespaces );
    			break;
    		}
    	}
    }
    
    private void insertValues( ArrayNode config, Document dom, Builder builder, Map<String, String> namespaces ) throws Exception {
    	for( JsonNode node: config ) {
    		String type = node.get("type").asText();
    		if( type.equals( "insert")) {
    			String parentPath = node.get( "parentPath").asText();
    			parentPath = parentPath.replaceAll("(^/|/$)", "" );
    			String xmlFragment = node.get( "xml").asText();

    			ArrayNode beforeElements = (ArrayNode) node.get( "beforeElements");
    			StringBuffer listedNames = new StringBuffer();
    			if( beforeElements != null ) {
    				for( JsonNode elem: beforeElements ) {
    					if( listedNames.length() >0 ) listedNames.append( ", " );
    					String name = elem.asText().replaceAll( "'", "").trim();
    					listedNames.append( "'" + name + "'");
    				}
    			}
    			
    			replaceFragment( ENRICH_BEFORE_ELEMENTS, "<xsl:variable name=\"names\" select=\"(" 
    			  + XMLUtils.xmlAttributeValueEscape(listedNames.toString())
    			  + ")\"/>", dom, builder, namespaces );
    	    	replaceFragment( ENRICH_INSERT_XML, xmlFragment, dom, builder, namespaces );
    	    	replaceFragment( ENRICH_PARENT_PATH, "<xsl:variable name=\"parentPathString\" select=\"'" + XMLUtils.xmlAttributeValueEscape(parentPath) + "'\" />", dom, builder, namespaces );
    		}
    	}    	
    }

    private void insertCsv( Document dom ) throws Exception {
    	Element csv = new Element( "csv" );
    	for( String[] row: getCsv()) {
    		Element rowElem = new Element( "row");
    		csv.appendChild(rowElem);
    		for( String col: row )  {
    			Element colElem = new Element( "col");
    			colElem.appendChild( new Text( col ));
    			rowElem.appendChild( colElem );
    		}
    	}
    	if( csv.getChildCount() == 0 ) return;
    	
    	Nodes nodes = dom.query("//*[local-name()='"+ENRICH_CSV+"']");
    	Node replaceMe = nodes.get(0);
    	ParentNode parent = replaceMe.getParent();
    	parent.replaceChild(replaceMe, csv );
    }

    public List<String[]> getCsv() {
    	List<String[]> result = new ArrayList<>();
    	try (
    		ByteArrayInputStream bis = new ByteArrayInputStream( getGzippedCsv());
    		GzipCompressorInputStream gzin = new GzipCompressorInputStream(bis)
    	) {
    		
    		Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(new InputStreamReader(gzin, "UTF8"));
    		for( CSVRecord record: records ) {
    			String[] row = new String[ record.size() ];
    			for( int i=0; i<record.size(); i++ )
    				row[i] = record.get( i );
    			result.add( row );
    		}
    		
    	} catch( Exception e ) {
    		log.error( "Csv from db read failed", e );
    	}
    	return result;
    }
    
    // write headers back, then the blob
    // not closing the stream
    public void outputCsvToStream( OutputStream out ) {
		ByteArrayInputStream bis = new ByteArrayInputStream( getGzippedCsv());
    	try(GzipCompressorInputStream gzin = new GzipCompressorInputStream(bis)) {
    		
    		out.write( (getHeaders()+"\n").getBytes("UTF-8"));
    		int input;
    		while(( input = gzin.read()) != -1 ) {
    			out.write( input );
    		}
    		out.flush();
    	} catch( Exception e ) {
    		log.error( "", e );
    	}
    }
    
    
    public Map<String, List<String[]>> asHash( int keyIdx ) {
    	return getCsv().stream().collect(Collectors.groupingBy( (String[] row) -> row[keyIdx]));
    }
    
    public EnrichBuilder getEnrichBuilder( int idxColumn ) {
    	return new EnrichBuilder( this, idxColumn);
    }
    
    //add given namespaces to the dom (exclude xsl and local prefix)
    private void addNamespaces( Document dom, Map<String, String> namespaces ) throws Exception {
    	Element root = dom.getRootElement();
    	// dont add or change xsl and local
    	for( Entry<String,String> entry: namespaces.entrySet()) {
    		if( entry.getKey().equalsIgnoreCase("xsl")) continue;
    		if( entry.getKey().equalsIgnoreCase("local")) continue;
    		root.addNamespaceDeclaration(entry.getKey(), entry.getValue());
    	}
    }
    
    // This method looks for local elements with given fragmentName
    // and replaces them with whatever xml is parsing too
    private void replaceFragment( String fragmentName, String xml, Document dom, Builder builder, Map<String,String> namespaces ) throws Exception {
    	Elements newElements = fragmentParser( builder, xml, namespaces );
    	Nodes nodes = dom.query("//*[local-name()='"+fragmentName+"']");
    	
    	Node replaceMe = nodes.get(0);
    	ParentNode parent = replaceMe.getParent();
    	int idx = parent.indexOf(replaceMe);
    	parent.removeChild(idx);
    	int count = newElements.size();
    	while( count > 0 ) {
    		Element elem = newElements.get( newElements.size() - count );
    		elem.detach();
    		parent.insertChild(elem, idx);
    		idx++;
    		count--;
    	}    	
    }
    
    // xmlFragment is not valid yet, needs namespaces and a root element around it, so that the Builder can make
    // nodes out of it.
    private Elements fragmentParser( Builder builder, String xmlFragment, Map<String, String> namespaces ) throws Exception {
    	String xml = "<root ";
    	for ( Entry<String,String> entry : namespaces.entrySet() ) {
    		xml += "xmlns:"+entry.getKey()+"=\""+entry.getValue()+"\" ";
    	}
    	xml += "xmlns:xsl = \"http://www.w3.org/1999/XSL/Transform\" "; 
    	xml += ">";
    	xml += xmlFragment;
    	xml += "</root>";
    	Document tmp = builder.build( xml, null );
    	return tmp.getRootElement().getChildElements();	
    }

    // getters-setters
    public Long getDbID() {
        return dbID;
    }

    public void setDbID(Long dbID) {
        this.dbID = dbID;
    }

    public String getHeaders() {
        return headers;
    }

    public void setHeaders(String headers) {
        this.headers = headers;
    }

    public User getCreator() {
        return creator;
    }

    public void setCreator(User creator) {
        this.creator = creator;
    }

    public Organization getOrganization() {
        return organization;
    }

    public void setOrganization(Organization organization) {
        this.organization = organization;
    }

    public byte[] getGzippedCsv() {
        return gzippedCsv;
    }

    public void setGzippedCsv(byte[] gzippedCsv) {
        this.gzippedCsv = gzippedCsv;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }

    public Long getBytesize() {
        return bytesize;
    }

    public void setBytesize(Long bytesize) {
        this.bytesize = bytesize;
    }

    public Long getLineCount() {
        return lineCount;
    }

    public void setLineCount(Long lineCount) {
        this.lineCount = lineCount;
    }
    public List<Integer> getProjectIds() {
        return (projectIds == null) ? new ArrayList<Integer>() : projectIds;
    }

    public void setProjectIds(List<Integer> projectIds) {
        this.projectIds = projectIds;
    }
    //Getters-Setters
}
