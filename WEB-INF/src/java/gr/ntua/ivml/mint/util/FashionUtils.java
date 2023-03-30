package gr.ntua.ivml.mint.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.function.Function;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateProcessor;
import org.apache.jena.update.UpdateRequest;
import org.apache.log4j.Logger;
import org.xml.sax.XMLReader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.jdbc.Driver;
import com.opensymphony.xwork2.util.TextParseUtil;

import gr.ntua.ivml.mint.OAIServiceClient;
import gr.ntua.ivml.mint.RecordMessageProducer;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Item;
import gr.ntua.ivml.mint.persistent.XmlSchema;
import gr.ntua.ivml.mint.pi.messages.ExtendedParameter;
import gr.ntua.ivml.mint.pi.messages.ItemMessage;
import gr.ntua.ivml.mint.pi.messages.Namespace;
import gr.ntua.ivml.mint.xml.transform.XSLTransform;
import gr.ntua.ivml.mint.xsd.ReportErrorHandler;
import gr.ntua.ivml.mint.xsd.SchemaValidator;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import nu.xom.Attribute;
import nu.xom.Builder;
import nu.xom.Element;
import nu.xom.Node;
import nu.xom.Nodes;

public class FashionUtils {

	private static final Logger log = Logger.getLogger(FashionUtils.class);

	// Number of records to copy
	private String recordsNum = "-1";

	// Database properties
	String url = "jdbc:mysql://panic.image.ntua.gr:3306/"; // the portal
	String dbName = "portal"; // the DB name
	String driver = "com.mysql.jdbc.Driver"; // the DB driver
	String userName = "fashion"; // the portal username
	String password = "portal"; // the portal password
	
	// Paths
	String xslPath = Config.getXSLDir()+System.getProperty("file.separator");

	String edmSchemaPrefix = "rdf"; // the prefix used for the OAI
	String edmSchemaUri = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	String edmFPSchemaPrefix = "edm_fp"; // the prefix used for the OAI
	String edmFPSchemaUri = "http://www.europeanafashion.eu/edmfp/";
//	String queueHost = Config.get("queue.host");
//	String queueRoutingKey = Config.get("queue.routingKey");
//	String oaiServerHost = Config.get( "OAI.server.host");
//	String oaiServerPort = Config.get("OAI.server.port");
	
	String queueHost,queueRoutingKey ,oaiServerHost ,oaiServerPort ;
	
	String dcUriNamespace = "http://purl.org/dc/elements/1.1/";
	String dctermsUriNamespace = "http://purl.org/dc/terms/" ;
	String rdfUriNamespace = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	
//	String tripleStore = "http://oreo.image.ntua.gr:3030/fashion_18_07_2016/";
	String tripleStore = "http://localhost:3030/fashionBenchmark/";
	
	RecordMessageProducer rmp;
	
	String updateColumn = "json";

	private XSLTransform edmTransformer;

	private Builder builder=null;
	
	private static Map<String, String> creatorEnrich = new HashMap<>();
	private static Map<String, String> spatialEnrich = new HashMap<>();
	private static long lastAccess = 0;
	
	private Map<String, Integer> enrichCounter = new HashMap<>();
	
	private ObjectMapper om = new ObjectMapper();
	
	public void inintRMP() {
		try {
			rmp = new RecordMessageProducer(queueHost, "test_exchange");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public FashionUtils() {
		try {
			queueHost = Config.get( "queue.host" );
			queueRoutingKey = Config.get( "queue.routingKey" );

			oaiServerHost = Config.get("OAI.server.host");
			oaiServerPort = Config.get("OAI.server.port");

			rmp = new RecordMessageProducer(queueHost, "test_exchange");
		} catch (Exception e) {
			log.error( "", e );
		}
	}
	
	public static void main(String[] args) {
		FashionUtils fashUtils = new FashionUtils();
		
		
//		//####################  OAI PUBLICATION ############################################################
//		//OAI publication is for Europeana.
//		//***IMPORTANNT NOTE: This execution of OAI publication code takes ages when executed on
//      //guinness. On the contrary it is much much faster when executed locally**** 
//		
//		fashUtils.inintRMP(); // /Initializes the Record message Producer
//		
//		//These lines of code publish on OAI all records of the DATASETS that are passed as parameter.
//		//A report per dataset is created
//		int[] dsArray = new int[]{7282};
//		fashUtils.oaiPublishDatasets(fashUtils.convertToArrayList(dsArray));
//
//		//These lines of code publish on OAI all records of the ORGANIZATIONS that are passed as parameter.
//		//A report per dataset is created
//		int[] orgsArray = new int[]{1050};
//		fashUtils.oaiPublishOrgs(fashUtils.convertToArrayList(orgsArray));
//		
//		//This has been used for the creation of a report per dataset. Initially a report per record was
//		//produced and this was problematic. The issue has been also fixed on the code so normally this
//		//method should not be again used.
////		fashUtils.createReportsPerDataset();
		
		//####################  TRIPLESTORE PUBLICATION ####################################################
//		//Triplestore publication is need for the semantic analysis. Metadata are first published on a 
//		//triplestore and then processed by Alexandros algorithms for linking to external data sources.
//		//***IMPORTANNT NOTE: This execution of Triplestore publication using a triplestore on oreo again
//		//was taking ages. On the contrary when fuseki has been set up locally and files have been uploaded
//		//there the whole process was much faster. In addition two methods for uploading triples have been
//		implemented see method triplestorePublishItem for more details**** 


//		//These lines of code publish on Triplestore all records of the DATASETS that are passed as parameter.
//		int[] orgsArray = new int[]{1004,1029};
//		fashUtils.triplestorePublishOrgs(fashUtils.convertToArrayList(orgsArray));

		//These lines of code publish on Triplestore all records of the ORGANIZATIONS that are passed as parameter.
//		int[] dsArray = new int[]{6807,1303,6340,6295,4486,1290};
//		fashUtils.triplestorePublishDatasets(fashUtils.convertToArrayList(orgsArray));


		
//		//####################  IMAGE ENRICHMENTS ##########################################################
//      //Image enrichments method updates the json column of record table according to the produced image
//		//analysis results. These results should be stored on table image_analysis.
//		//****MAKE SURE THE updateColumn variable is set "json" before the final execution****
//		1020 records = 457818 / 4 = 114455

		fashUtils.performImageEnrichments(114455,0);      // limit 114455 offset 0
		fashUtils.performImageEnrichments(114455,114455); // limit 114455  offset = pr_offset + pr_limit = 114455
		fashUtils.performImageEnrichments(114455,228910); // limit 114455  offset = pr_offset + pr_limit = 228910
		fashUtils.performImageEnrichments(-1,343365);     // limit -1  offset = pr_offset + pr_limit = 343365
		
		//####################  SEMANTIC ENRICHMENTS ##########################################################
//      //Semantic enrichments method updates the json column of record table according to the produced semantic
//		//analysis results. These results should be stored on table enrichments.
//		//****MAKE SURE THE updateColumn variable is set "json" before the final execution****
//		//enrichments count(*) = 2161619 / 16 = 135101
//		fashUtils.performSemanticEnrichments(135101,0);          // limit 135101 offset 0
//		fashUtils.performSemanticEnrichments(135101, 135101);     // limit 135101 offset = pr_offset + pr_limit = 135101
//		fashUtils.performSemanticEnrichments(135101, 270202);     // limit 135101 offset = pr_offset + pr_limit = 270202
//		fashUtils.performSemanticEnrichments(135101, 405303);    // limit 135101 offset = pr_offset + pr_limit = 405303
//		fashUtils.performSemanticEnrichments(135101, 540404);    // limit 135101 offset = pr_offset + pr_limit = 540404
//		fashUtils.performSemanticEnrichments(135101, 675505);          // limit 135101 offset = pr_offset + pr_limit = 675505
//		fashUtils.performSemanticEnrichments(135101, 810606);     // limit 135101 offset = pr_offset + pr_limit = 810606
//		fashUtils.performSemanticEnrichments(135101, 945707);    // limit 135101 offset = pr_offset + pr_limit = 945707
//		fashUtils.performSemanticEnrichments(135101, 1080808);    // limit 135101 offset = pr_offset + pr_limit = 1080808
//		fashUtils.performSemanticEnrichments(135101, 1215909);          // limit 135101 offset = pr_offset + pr_limit = 1215909

//		fashUtils.performSemanticEnrichments(135101, 1351010);     // limit 135101 offset = pr_offset + pr_limit = 1351010
//		fashUtils.performSemanticEnrichments(135101, 1486111);    // limit 135101 offset = pr_offset + pr_limit = 1486111
//		fashUtils.performSemanticEnrichments(135101, 1621212);    // limit 135101 offset = pr_offset + pr_limit = 1621212
//		fashUtils.performSemanticEnrichments(135101, 1756313);          // limit 135101 offset = pr_offset + pr_limit = 1756313
//		fashUtils.performSemanticEnrichments(135101, 1891414);     // limit 135101 offset = pr_offset + pr_limit = 1891414
//		fashUtils.performSemanticEnrichments(135101, 2026565);    // limit 135101 offset = pr_offset + pr_limit = 2026565
//		fashUtils.performSemanticEnrichments(-1, 2161616);        // limit -1  offset = pr_offset + pr_limit = 2161616
		
		
		
	}

	private ArrayList<Integer> convertToArrayList(int[] orgsArray) {
		ArrayList<Integer> orgs = new ArrayList<Integer>();
		for(int i=0; i < orgsArray.length; i++)
			orgs.add(new Integer(orgsArray[i]));
		return orgs;
	}

	public  Connection getMySqlDBConnection() {
		try {
			DriverManager.registerDriver( (Driver) Class.forName(driver).newInstance());
			Connection conn = DriverManager.getConnection(url + dbName + "?useUnicode=yes&characterEncoding=UTF-8",
					userName , password);
			log.info( "MySQL connection aquired");
			return conn;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}


	public ArrayList<Integer> getMySqlDBDatasets() {
		Connection conn = null;
		try {
			conn = getMySqlDBConnection();
			Statement st = conn.createStatement();
			ResultSet res = st
					.executeQuery("select mint_dataset_id,count(*) as count from record group by mint_dataset_id order by count asc");
			ArrayList<Integer> dss = new ArrayList<Integer>();

			while (res.next()) {
				int mintOrgID = res.getInt("mint_dataset_id");
				if (mintOrgID > 1)
					dss.add(new Integer(mintOrgID));
			}
			return dss;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	private ArrayList<Integer> getMySqlDBOrgs() {
		Connection conn = null;
		try {
			conn = getMySqlDBConnection();
			Statement st = conn.createStatement();
			ResultSet res = st
					.executeQuery("select distinct mint_org_id from record");
			ArrayList<Integer> orgs = new ArrayList<Integer>();

			while (res.next()) {
				int mintOrgID = res.getInt("mint_org_id");
				if (mintOrgID > 1)
					orgs.add(new Integer(mintOrgID));
			}
			return orgs;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Published all the records of the given Orgs.
	 * @param orgs the list that contains the org ids to be published.
	 */
	public void oaiPublishOrgs(ArrayList<Integer> orgs){
			oaiPublish(-1,orgs,true);
	}
	
	/**
	 * Published all the records of the given datasets.
	 * @param ds the list that contains the datasets ids to be published.
	 */
	public void oaiPublishDatasets(ArrayList<Integer> ds){
		oaiPublish(-1,ds,false);
}
	/**
	 * Published all the records of the given org.
	 * @param mintRecordId the org id to be published.
	 */
	public void oaiPublishOrg(int mintRecordId) {
		ArrayList<Integer> orgs = new ArrayList<Integer>();
		orgs.add(new Integer(mintRecordId));
		oaiPublish(-1,orgs,true);
	}
	
	/**
	 * Published all the organizations. This may take days...use the publishOrgs or publishDatasets instead.
	 */
	public void oaiPublishAll() {
		ArrayList<Integer> orgs = new ArrayList<Integer>();
		oaiPublish(-1,orgs,true);
	}
	
	/**
	 * Published all the records of the given Orgs.
	 * @param orgs the list that contains the org ids to be published.
	 */
	public void triplestorePublishOrgs(ArrayList<Integer> orgs){
		triplestorePublish(-1,orgs,true,0);
	}
	
	/**
	 * Published all the records of the given Orgs.
	 * @param orgs the list that contains the org ids to be published.
	 */
	public void triplestorePublishOrgs(ArrayList<Integer> orgs,int startFrom){
		triplestorePublish(-1,orgs,true,startFrom);
	}
	
	/**
	 * Published all the records of the given datasets.
	 * @param ds the list that contains the datasets ids to be published.
	 */
	public void triplestorePublishDatasets(ArrayList<Integer> ds){
		triplestorePublish(-1,ds,false,0);
}
	/**
	 * Published all the records of the given org.
	 * @param mintRecordId the org id to be published.
	 */
	public void triplestorePublishOrg(int mintOrgId) {
		ArrayList<Integer> orgs = new ArrayList<Integer>();
		orgs.add(new Integer(mintOrgId));
		triplestorePublish(-1,orgs,true,0);
	}
	
	/**
	 * Published all the organizations. This may take days...use the publishOrgs or publishDatasets instead.
	 */
	public void triplestorePublishAll() {
		ArrayList<Integer> orgs = new ArrayList<Integer>();
		triplestorePublish(-1,orgs,true,0);
	}
	
	/**
	 * Published specific number of records from all the organizations. 
	 */
	public void triplestorePublishAll(int amoungOfRecords) {
		ArrayList<Integer> orgs = new ArrayList<Integer>();
		triplestorePublish(amoungOfRecords,orgs,true,0);
	}
	
	/**
	 * Creates a OAI report per dataset for all the fashion orgs
	 */
	public void createOaiReportsPerDataset(){
		Connection conn =  getMySqlDBConnection();
		try {
			String query = "select distinct mint_dataset_id, mint_org_id from record";
			Statement st = conn.createStatement();
			ResultSet res = st.executeQuery(query);
			while (res.next()) 
				getOaiReport(res.getInt("mint_dataset_id"), res.getInt("mint_org_id"));
			conn.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private String fixNodesUrl( nu.xom.Nodes nodes, String provider ) {
		String resUrl = "";
		for( int i=0; i<nodes.size(); i++ ) {
			nu.xom.Node n = nodes.get(i);
			if( n instanceof nu.xom.Attribute ) {
				nu.xom.Attribute att = (nu.xom.Attribute) n;
				String oldUrl = att.getValue();
				// fix url				
				String newUrl = reposUrl( provider,  oldUrl) ;
				att.setValue( newUrl );
				resUrl = newUrl;
			}
		}		
		// we might want that
		return resUrl;
	}

	private void replaceShownBy( nu.xom.Nodes nodes, String shownByUrl ) {
		for( int i=0; i<nodes.size(); i++ ) {
			nu.xom.Node n = nodes.get(i);
			if( n instanceof nu.xom.Attribute ) {
				nu.xom.Attribute att = (nu.xom.Attribute) n;
				String oldUrl = att.getValue();
				String newUrl = oldUrl.replace("%isShownBy%", shownByUrl );
				// fix url				
				att.setValue( newUrl );
			}
		}		
	}
	
	private String repairXml( String edm) throws Exception {
		// parse 
		nu.xom.Document doc = getXomBuilder().build(edm, null);
		// get the provider
		//nu.xom.Nodes nodes = XQueryUtil.xquery( doc , "//*[local-name()='Aggregation']/*[local-name()='dataProvider']");
		nu.xom.Nodes nodes = doc.query("//*[local-name()='Aggregation']/*[local-name()='dataProvider']");
		if( nodes.size() == 0 ) {
			log.error( "No provider!");
			return null;
		}
		
		String provider = nodes.get(0).getValue().trim();
		// get the nodes that need fixing
		//nodes = XQueryUtil.xquery( doc , "//*[local-name()='isShownBy']/@*[local-name()='resource']");
		nodes = doc.query("//*[local-name()='isShownBy']/@*[local-name()='resource']");
		String isShownBy = fixNodesUrl( nodes, provider );

		// more to fix ?
		//nodes = XQueryUtil.xquery( doc , "//*[local-name()='Aggregation']/*[local-name()='hasView']/@*[local-name()='resource']");
		nodes = doc.query("//*[local-name()='Aggregation']/*[local-name()='hasView']/@*[local-name()='resource']");
		fixNodesUrl( nodes, provider );

		if( StringUtils.empty(isShownBy)) {
			log.error( "No isShownBy found !");
			// is invalid most likely
		} else {
			nodes = doc.query("//*[local-name()='object']/@*[local-name()='resource']");
			replaceShownBy(nodes, isShownBy );
		}
		return doc.toXML();
	}
	
	
	private Builder getXomBuilder() {
		if( builder == null ) {
			try {
				XMLReader parser = org.xml.sax.helpers.XMLReaderFactory.createXMLReader(); 
				parser.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

				builder = new Builder(parser);
			} catch( Exception e ) {
				log.error( "Cannot build xml parser.", e );
			}
		}
		return builder;
	}


	
	/**
	 * Publishes to the OAI the given amount of records either of the given orgs or of the given datasets.
	 * @param recordsFromEachOrg the amount of records to be published. If -1 all the records are published.
	 * @param queryArgs this list either contains the mint_org_ids or the mint_dataset_ids to be published.
	 * @param orgsParam the type of values the queryArgs list holds. if true then queryArgs holds org ids else it holds dataset ids.
	 */
	public void oaiPublish(int recordsFromEachOrg, ArrayList<Integer> queryArgs, boolean orgsParam) {
		Connection conn = null;
		
		try {
			XmlSchema xs = DB.getXmlSchemaDAO().getByName("EDM"); 
			String queryParamType = "org";
			if(queryArgs.size()==0){
				queryArgs = getMySqlDBOrgs();
				orgsParam = true;
			}
			if(!orgsParam)
				queryParamType = "dataset";
				
			String report = "\n\nOrg_id	OAI	Published	Invalid\n";
			for (int i = 0; i < queryArgs.size(); i++) {
				log.info("	OAI Publication for " + queryParamType + " "+ queryArgs.get(i) + " has just started");
				conn = getMySqlDBConnection();
				String query;
				if (recordsFromEachOrg > 0)
					query = "select * from record where mint_"+queryParamType+"_id="
							+ queryArgs.get(i) + " limit " + recordsFromEachOrg + "";
				else
					if(!orgsParam)
						query = "select * from record where mint_"+queryParamType+"_id="
							+ queryArgs.get(i);
					else
						query = "select * from record where mint_"+queryParamType+"_id="
								+ queryArgs.get(i)+" order by mint_dataset_id";
				String countQuery = "select count(*) from record where mint_"+queryParamType+"_id="
						+ queryArgs.get(i);

				Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				st.setFetchSize(Integer.MIN_VALUE);
				ResultSet res = st.executeQuery(countQuery);
				res.next();
				int orgTotal = res.getInt("count(*)");
				res.close();
				int orgCount = 0;
				int published = 0;
				int invalid = 0;
				res = st.executeQuery(query);
                int part = orgTotal/10;
                int datasetId=0;
                
                ArrayList<ExtendedParameter> params = new ArrayList<ExtendedParameter>();
				while (res.next()) {
					orgCount++;
					
					//Create one report per dataset either orgs or datasets are passed on to this method
					if(!orgsParam && orgCount==1)
						params = getOaiReport(res.getInt("mint_dataset_id"), res.getInt("mint_org_id"));
					
					if(orgsParam){
						if(datasetId != res.getInt("mint_dataset_id")){
							datasetId = res.getInt("mint_dataset_id");
							params = getOaiReport(res.getInt("mint_dataset_id"), res.getInt("mint_org_id"));
						}
					}
					String edmFp = res.getString("xml");
					String edm =  edmFp2edmTransformation(edmFp,res.getString("hash"),res.getInt("mint_org_id") );
					
					
					String edmFixed = repairXml( edm );
					
					//Publish EDM FP
					oaiPublishItem(xs,res.getInt("mint_dataset_id"),
							res.getInt("mint_org_id"),
							res.getInt("mint_record_id"), edmFp, edmFPSchemaPrefix, edmFPSchemaUri,params);
					
					//Publish EDM
					if(( edmFixed != null ) && 
							(edmValidate(edmFixed,xs))) {
						oaiPublishItem(xs,res.getInt("mint_dataset_id"),
								res.getInt("mint_org_id"),
								res.getInt("mint_record_id"), edmFixed, edmSchemaPrefix, edmSchemaUri,params);
					} else {
						log.info( "EDM invalid");
						invalid++;
					}
					if(orgCount%1000 == 0)
						log.info("	Published " + orgCount + "/" + orgTotal
							+ " from " + queryParamType + " " +queryArgs.get(i) + " "
							+ (i + 1) + "/" + queryArgs.size() );
					
					published++;	
				}
				conn.close();
				report = report + queryArgs.get(i) + "	Records:" + orgTotal
						+ "	OAI Published:" + published + "	Invalid:" + invalid
						+ "\n";	
				log.info(report);
			}

		} catch (Throwable e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Read two column tsv file into given Map. TSV should be literal\turi\n
	 * @param resourceName
	 * @param targetMap
	 * @throws Exception
	 */
	private void tsvToMap( String resourceName, Map<String,String> targetMap ) throws Exception  {
		InputStream tsvStream = null;
		try {
			synchronized( targetMap ) {
				tsvStream = FashionUtils.class.getClassLoader().getResourceAsStream( resourceName );
				List<String> lines = IOUtils.readLines(tsvStream, "UTF8");
				targetMap.clear();
				for( String line : lines ) {
					String columns[] = line.split("\\t",2);
					if( !StringUtils.empty(columns[0]) && !StringUtils.empty( columns[1]) ) {
						targetMap.put( columns[0],  columns[1]);
					}
				}
			}
		} catch( Exception e ) {
			log.error("",e );
			throw e;
		} finally {
			IOUtils.closeQuietly(tsvStream);
		}
	}
	
	private void readManualEnrichments() {
		try {
			tsvToMap( "/creator_enrich.tsv", creatorEnrich );
			tsvToMap( "/spatial_enrich.tsv", spatialEnrich );
			lastAccess = System.currentTimeMillis();
		} catch( Exception e ) {
			log.error("Enrich IO problem", e );
		} 
	}
	
	private String getEnrichUri( String literal, Map<String, String> enrichMap ) {
		// outdated, older than 1h?
		if( System.currentTimeMillis() - lastAccess > 3600l*1000 ) {
			readManualEnrichments();
		} else {
			lastAccess = System.currentTimeMillis();
		}
		synchronized( enrichMap ) {
			return enrichMap.get( literal ); 
		} 			
	}
		
	private void swapLiteralForResource( Node element, Map<String,String> map, Function<String, String> modifyLiteral, boolean keepLiteral ) {
		Element elem = (Element) element;
		String literal = elem.getValue();
		// log.info( "Literal: '" + literal + "'");
		literal = modifyLiteral.apply( literal );
		String resource = getEnrichUri( literal, map );
		if( resource != null) {
		
			if( keepLiteral ) {
				Element newElem = (Element) elem.copy();
				int index = elem.getParent().indexOf(elem);
				elem.getParent().insertChild(newElem, index );
			}
			
			// remove literal, children and attributes
			elem.removeChildren();
			for( int i=elem.getAttributeCount(); i-->0;) {
				elem.removeAttribute(elem.getAttribute(i));
			}
			
			// create rdf:resource attribute 
			elem.addAttribute( new Attribute( "rdf:resource", rdfUriNamespace, resource ));

			// counts  enrichments
			int count = enrichCounter.getOrDefault(resource, 0 );
			enrichCounter.put( resource, count+1 );
		}
	}
	
	
	/**
	 * Applies changes to dc:creator and dc:spatial according to the tsv files
	 * creator_enrich.tsv and spatial_enrich.tsv
	 * @param xml
	 * @return xml with creator and spatial replaced with rdf:references
	 */
	public String manualEnrich( String edmXml ) {
		String result = edmXml;
		try {
			nu.xom.Document doc = getXomBuilder().build(edmXml, null);
			// modify dc:spatials
			// modify dc:creator
			Nodes nodes = doc.query( "//*[local-name()='spatial' and namespace-uri()='"+ dctermsUriNamespace+"']" );
			for( int i=0; i<nodes.size(); i++ ) {
				nu.xom.Node n = nodes.get( i );
				swapLiteralForResource(n,  spatialEnrich, str->str , false );
			}
			
			// if found replace 
			nodes = doc.query( "//*[local-name()='creator' and namespace-uri()='"+ dcUriNamespace+"']" );
			for( int i=0; i<nodes.size(); i++ ) {
				nu.xom.Node n = nodes.get( i );
				swapLiteralForResource(n,  creatorEnrich, 
						(String str) -> str.replaceAll(" +(\\(Photographer\\))$", ""), false
				);
				swapLiteralForResource(n,  creatorEnrich, 
						(String str) -> str.replaceAll(" +(\\(Designer\\))$", ""), true
				);
			}
			
			nodes = doc.query( "//*[local-name()='contributor' and namespace-uri()='"+ dcUriNamespace+"']" );
			for( int i=0; i<nodes.size(); i++ ) {
				nu.xom.Node n = nodes.get( i );
				swapLiteralForResource(n,  creatorEnrich, 
						(String str) -> str.replaceAll(" +(\\(Stylist\\)|\\(Model\\)|\\(Curator\\)|" 
								+ "\\(Author\\)|\\(Illustrator\\)|\\(Set designer\\)|"
								+ "\\(Hair stylist\\)|\\(Make up artist\\)|\\(Editor\\))$", ""), false
				);
			}
			
			result = doc.toXML();
		} catch( Exception e ) {
			log.error( "Manual enrich failed", e);
		}
		return result;
	}
	
	
	
	/**
	 * Send EDMfp and EDM to the rabbitmq for storing in the Fashion OAI.
	 * Dataset is NOT checked. Needs to be EMD FP to work. Only valid EDM Fp records
	 * are processed.
	 * 
	 * This is not yet the best way. SHould be a regular publication with crosswalk and
	 * dataset in edm stored in mint.
	 * 
	 * @param ds
	 */
	public void oaiPublishDatasetDirectly( Dataset ds ) {
		XmlSchema xs = DB.getXmlSchemaDAO().getByName("EDM"); 
		try {
			ArrayList<ExtendedParameter> params = getOaiReport(ds.getDbID().intValue(), (int) ds.getOrganization().getDbID());
			
			ds.processAllValidItems( (Item item) -> {
				String edmFp = item.getXml();
				String edm =  edmFp2edmTransformation(edmFp,"", (int)ds.getOrganization().getDbID() );
//				String edmFixed = repairXml( edm );
				
				String enrichedEdm = manualEnrich( edm );
				// seems the direct new publish doesn't need fixing
				String edmFixed = edm;
				//Publish EDM FP
				oaiPublishItem(xs, ds.getDbID().intValue(),
						(int) ds.getOrganization().getDbID(),
						item.getDbID(), edmFp, edmFPSchemaPrefix, edmFPSchemaUri,params);
				
				//Publish EDM
				if(( enrichedEdm != null ) && 
						(edmValidate(enrichedEdm,xs))) {
					oaiPublishItem(xs, ds.getDbID().intValue(),
							(int) ds.getOrganization().getDbID(),
							item.getDbID(), enrichedEdm, edmSchemaPrefix, edmSchemaUri,params);
				} else {
					log.info( "EDM invalid "+ item.getLabel());
				}
			}, false);
			log.info( "DS #"+ ds.getDbID()+" EnrichCounter:\n"+om.writeValueAsString(enrichCounter));
			enrichCounter.clear();
		} catch( Exception e ) {
			log.error("", e );
		} 
	}
	
	/**
	 * Publishes to the triplestore the given amount of records either of the given orgs or of the given datasets.
	 * @param recordsFromEachOrg the amount of records to be published. If -1 all the records are published.
	 * @param queryArgs this list either contains the mint_org_ids or the mint_dataset_ids to be published.
	 * @param orgsParam the type of values the queryArgs list holds. if true then queryArgs holds org ids else it holds dataset ids.
	 */
	public void triplestorePublish(int recordsFromEachOrg, ArrayList<Integer> queryArgs, boolean orgsParam, int startFrom) {
		Connection conn = null;
		
		try {
			
			String queryParamType = "org";
			if(queryArgs.size()==0){
				queryArgs = getMySqlDBOrgs();
				orgsParam = true;
			}
			if(!orgsParam)
				queryParamType = "dataset";
				
			String report = "\n\nOrg_id\tTotal Records\tPublished\tInvalid\tRetrieval\tTransformation per Record\tPublication per record\n";
			for (int i = 0; i < queryArgs.size(); i++) {
				
				log.info("	Triplestore Publication for " + queryParamType + " "+ queryArgs.get(i) + " has just started");
				conn = getMySqlDBConnection();
				String query;
				if (recordsFromEachOrg > 0)
					query = "select * from record where mint_"+queryParamType+"_id="
							+ queryArgs.get(i) + " limit " + recordsFromEachOrg + "";
				else
					if(!orgsParam)
						query = "select * from record where mint_"+queryParamType+"_id="
							+ queryArgs.get(i);
					else
						query = "select * from record where mint_"+queryParamType+"_id="
								+ queryArgs.get(i)+" order by mint_dataset_id";
				String countQuery = "select count(*) from record where mint_"+queryParamType+"_id="
						+ queryArgs.get(i);

				
				Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				ResultSet res = st.executeQuery(countQuery);
				res.next();
				
				int orgTotal = res.getInt("count(*)");
				System.out.println("Total records "+orgTotal);
				int orgCount = 0;
				int published = 0;
				int invalid = 0;
				long start = System.nanoTime();
				System.out.println("Retrieving records from "+queryArgs.get(i));
				res = st.executeQuery(query);
                int part = orgTotal/10;
                long getRecords = System.nanoTime() - start;
                long totalTransform = 0 , totalTriplestore = 0;
                System.out.println("Retrieval of records:"+getRecords);
				while (res.next()) {
					orgCount++;
					if(orgCount > startFrom){
//					if(orgCount < 10){
						//System.out.println(orgCount+"/"+orgTotal);
						String edmFp = res.getString("xml");
						String isShownBy = getIsShownBy(edmFp);
						String dataProvider = getDataProvider(edmFp);
						//String newURI = reposUrl(dataProvider, isShownBy);

						//Publish EDM FP to triplestore
						if(!isShownBy.equals("")){
							start = System.nanoTime();
							String edmFpRdf = edmFp2RDFTransformation(edmFp,res.getString("hash"),res.getInt("mint_org_id"), res.getInt("mint_dataset_id"), res.getInt("mint_record_id"));
							long transform = (System.nanoTime() - start);
							
							start = System.nanoTime();
							boolean itemPublished = triplestorePublishItem(edmFpRdf,res.getString("hash"));
							long triplePublish = (System.nanoTime() - start);		
							totalTransform = totalTransform + transform;
							totalTriplestore = totalTriplestore + triplePublish;
//							System.out.println(orgCount+"\t"+transform+"\t"+triplePublish);
//							System.out.println(orgCount+"\t"+totalTransform+"\t"+totalTriplestore);
							if(!itemPublished)
								invalid++;
							
						}else
							invalid++;

						if(orgCount%part == 0){
							log.info("	Triplestore Published " + orgCount + "/" + orgTotal
									+ " from " + queryParamType + " " +queryArgs.get(i) + " "
									+ (i + 1) + "/" + queryArgs.size()+ " "+(orgCount/part)*10+" %");
							log.info(totalTransform/orgCount + "\t" + totalTriplestore/orgCount+"\n");
						}

						published++;
					}

				}
				conn.close();
				report = report + queryArgs.get(i) + "\t" + orgTotal
						+ "\t" + published + "\t" + invalid
						+ "\t" 
						+getRecords+"\t"+(totalTransform/orgCount) + "\t" + (totalTriplestore/orgCount)
						+ "\n";	
				
				log.info(report);
				
			}
			log.info(report);
//			System.exit(0);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private boolean triplestorePublishItem(String edmFpRdf, String hash) {
		
//		This publication method uses the s-post script. Fuseki has to be installed locally and 
//		a temp deirectory is required. See sPost method and set the paths accordingly
//		return sPost(edmFpRdf, hash);
		
//		This publication method uses the SPARQL update enpoint and the insert data command. 
		return sparqlUpdate(file2Triples(edmFpRdf));
		
	}

	private ArrayList<ExtendedParameter> getOaiReport(int mintDatasetID, int mintOrgID ){
		
		OAIServiceClient osc = new OAIServiceClient(oaiServerHost, Integer.parseInt(oaiServerPort));
		ArrayList<Integer> datasetIds = new ArrayList<Integer>();
		datasetIds.add(new Integer(mintDatasetID));
		final String reportId = osc.createReport(queueRoutingKey, 1000, mintOrgID, datasetIds );
		ExtendedParameter ep = new ExtendedParameter();
		ep.setParameterName("reportId");
		ep.setParameterValue("" + reportId);
		final ArrayList<ExtendedParameter> params = new ArrayList<ExtendedParameter>();
		params.add(ep);
		return params;
	}

	private boolean edmValidate(String xml,XmlSchema xs) {
		try {
			ReportErrorHandler report = SchemaValidator.validate(xml, xs);
			if (report.isValid())
				return true;
			else {
				log.info( "EDM validate problem: " + report.getReportMessage());
				return false;
			}
		} catch (Exception e) {
			log.error( "Exception in edm validate", e );
			return false;
		}
	}
	
	private boolean edmFpValidate(String xml) {
		return true;
	}

	/**
	 * Transforms/Normalizes the input xml to a one that can be imported in a triplestore.
	 * @param input the input xml in EDM FP.
	 * @param hash the hash of the record taken from the staging server.
	 * @param orgID the orgID to which the xml belongs to.
	 * @param datasetId the dataset id
	 * @param recordId the record id
	 * @return the edmFP xml that will be sent to OAI for Europeana.
	 */
	private String edmFp2RDFTransformation(String input, String hash, int orgID, int datasetId, int recordId) {
		try {
			XSLTransform transformer = new XSLTransform();

			File xslPart1File = new File(xslPath + "EDMFP2EDMFPPart1.xsl");
			File xslPart2File = new File(xslPath + "EDMFP2EDMFPPart2.xsl");

			String xslPart1 = FileUtils.readFileToString(xslPart1File, "UTF-8");
			String xslPart2 = FileUtils.readFileToString(xslPart2File, "UTF-8");
			String xsl = xslPart1 +
					"	<xsl:param name=\"var2\" select=\"'" + hash + "'\" /> \n  "
					+ "	<xsl:param name=\"var3\" select=\"'" + orgID+ "'\" />  \n"
					+ "	<xsl:param name=\"var4\" select=\"'" + datasetId+ "'\" />  \n"
					+ "	<xsl:param name=\"var5\" select=\"'" + recordId+ "'\" />  \n"
					+ xslPart2;
			
//			System.out.println(input);
//			System.out.println(xsl);
			return transformer.transform(input, xsl);

		} catch (Exception e) {
			e.printStackTrace();
			return "";
		}
	}
	
	/**
	 * Transforms the input xml of the orgID organization using the has and the newURI
	 * @param input the input xml in EDM FP .
	 * @param hash the hash of the record taken from the staging server.
	 * @param orgID the orgID to which the xml belongs to.
	 * @param newURI the newURI to be used for edm:isShownBy and edm:object values 
	 * @return the edm xml that will be sent to OAI for Europeana.
	 */
	private String edmFp2edmTransformation(String input, String hash, int orgID ) {
		try {
			if( edmTransformer == null  ) {
				edmTransformer = new XSLTransform();
				String xsl = FileUtils.readFileToString(
						new File(xslPath + "EDMFP2EDMSingle.xsl"), "UTF-8");
				edmTransformer.setXSL(xsl);
			}

			String printDescription = "true";
			if (orgID == 1003) // SPK
				printDescription = "false";

			Map<String, String> parameters = new HashMap<String, String>();
			parameters.put( "var2", hash );
			parameters.put( "var3", printDescription );
			
			edmTransformer.setParameters(parameters);
			return edmTransformer.transform(input );

		} catch (Exception e) {
			e.printStackTrace();
			return "";
		}
	}

	
	/**
	 * Gets the edm:dataProvider from the given xml
	 * @param xml the xml in edm_fp 
	 * @return the value of the edm:dataProvider
	 */
	private String getDataProvider(String xml) {
		String dataProvider = "";
		try {
			nu.xom.Document doc = getXomBuilder().build(xml, null);

			// Document doc = parseXML(xml);
			Nodes labels = doc.query( "*[local-name()='dataProvider']//*[local-name()='prefLabel']" );
			if( labels.size() > 0 ) dataProvider = labels.get(0).getValue();
		} catch( Exception e ) {
			log.error( "EDMFP parse error" ,e  );
		}
		return dataProvider;
	}
	
	/**
	 * Gets the edm:isShownBy value from the given xml
	 * @param xml the xml in edm_fp 
	 * @return the value of the edm:dataProvider
	 */
	private String getIsShownBy(String xml) {
		String isShownBy = "";
		try {
			nu.xom.Document doc = getXomBuilder().build(xml, null);
			Nodes uris = doc.query( "*[local-name()='isShownBy']/@*[local-name()='about']" );
			if( uris.size() > 0 ) isShownBy = uris.get(0).getValue();
		} catch( Exception e ) {
			log.error( "EDMFP parse error" ,e  );
		}
		return isShownBy;	
	}

	private void oaiPublishItem(XmlSchema xs, int mintDatasetID, int mintOrgID,
			long mintRecordID, String transformation, String schemaPrefix, String schemaUri, ArrayList<ExtendedParameter> params) {
		try {
			Namespace ns = new Namespace();
			int schemaId = xs.getDbID().intValue(); // EDM
			String routingKeysConfig = queueRoutingKey;
			Set<String> routingKeys = TextParseUtil.commaDelimitedStringToSet(routingKeysConfig);
			ns.setPrefix(schemaPrefix);
			ns.setUri(schemaUri);

			ItemMessage im = new ItemMessage();
			im.setDataset_id(mintDatasetID);
			im.setDatestamp(System.currentTimeMillis());
			im.setItem_id(mintRecordID);
			im.setOrg_id(mintOrgID);
			im.setPrefix(ns);
			im.setProject("");
			im.setSchema_id(schemaId);
			im.setSourceDataset_id(mintDatasetID);
			im.setSourceItem_id(mintRecordID);
			im.setUser_id(1);
			im.setXml(transformation);
			im.setParams(params);

			for (String routingKey : routingKeys)
				rmp.send(im, routingKey);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
	
	/**
	 * Calculates the newURI that will be used for edm:isShownBy and edm:object values 
	 * @param provider the edm:dataProvider
	 * @param image the edm:isShwonBy value
	 * @return the newURI for the isShownBy
	 */
	public static String reposUrl(String provider, String image) {
		String reposUri = image;
				
		if(image.indexOf("repos.europeanafashion.eu") < 0) {
			try {
				String encodedProvider = URLEncoder.encode(provider, "UTF-8").replace("+", "%20");
				String md5Hash = generateHash(image);
				String hashPrefix = md5Hash.substring(0, 2);
				reposUri = "http://repos.europeanafashion.eu/ext2/" + encodedProvider + "/" + hashPrefix + "/" + md5Hash + ".jpg"; 
			} catch (Exception e) {
				log.error( "reposUrl not calculated well" ,e );
			}
		}
		
		if(reposUri.endsWith(".pdf")) {
			reposUri = reposUri.replace(".pdf", ".jpg").replace("europeanafashion.eu/", "europeanafashion.eu/thumbs/");
		}
		
		return reposUri.replace(".pdf", ".jpg");
	}
	
	public static String generateHash(String txt) throws NoSuchAlgorithmException{
		byte[] md5 = MessageDigest.getInstance("MD5").digest(txt.getBytes());
		return Hex.encodeHexString(md5);
	}
	
	public String file2Triples(String fashionEDMFP){
		try {
			InputStream stream = new ByteArrayInputStream(fashionEDMFP.getBytes("UTF8"));
			Model model = ModelFactory.createDefaultModel() ; 
			model.read(stream, "RDF/XML");
			StringWriter out = new StringWriter();
			model.write(out, "N-TRIPLES");
			return out.toString();
		} catch( Exception  e ) {
			log.error( "WTF UTF8 encoding not known?", e );
			return "";
		}
	}
	
	public boolean sparqlUpdate(String data){
		try{
			UpdateRequest queryObj = UpdateFactory.create("INSERT DATA {"+data+"}"); 
			UpdateProcessor qexec = UpdateExecutionFactory.createRemote(queryObj, tripleStore+"update"); 
			qexec.execute();
			return true;
		}catch(Exception e){
			return false;
		}
	}
	
	public boolean sPost(String data, String hash){
		// Change cmdPath and tmpDir according to your local setup
		String cmdPath = "/Users/nsimou/Documents/apache-jena-fuseki-2.4.0/bin";
		String tmpDir = "/Users/nsimou/Desktop/FashionNtriples/Spost/";
		
		String rep = tripleStore+"data default";
		
		try{
			//Save temp file
			PrintWriter out = new PrintWriter( tmpDir+hash+".rdf" );
		    out.println( data );
		    out.close();
		    
		    //Execute script for uploading to repo
		    String cmd = cmdPath+"/s-post "+rep+" "+tmpDir+hash+".rdf";
		    executeCommand(cmd);
		    executeCommand("rm "+tmpDir+hash+".rdf");
		    return true;
		}catch(Exception e){
			e.printStackTrace();
			return false;
		}
	}


	private String executeCommand(String command) {
		StringBuffer output = new StringBuffer();
		Process p;
		try {
			p = Runtime.getRuntime().exec(command);
			p.waitFor();
			BufferedReader reader = 
                            new BufferedReader(new InputStreamReader(p.getInputStream()));

                        String line = "";			
			while ((line = reader.readLine())!= null) {
				output.append(line + "\n");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return output.toString();
	}
	
	public void performSemanticEnrichments(int enrichRecs, int offset){
		Connection conn = null;
		try{
			//Query to get the all enrichments files 
			String query;
			conn = getMySqlDBConnection();
			if (enrichRecs > 0)
				query = "select * from enrichments limit " + enrichRecs + "";
			else
				query = "select * from enrichments";
			String countQuery = "select count(*) from enrichments";


			Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			Statement st2 = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			ResultSet res;
			int orgTotal;
			if(enrichRecs < 0){
				res = st.executeQuery(countQuery);
				res.next();
				orgTotal = res.getInt("count(*)")-offset;
				System.out.println("Total records "+orgTotal);
			}else{
				orgTotal = enrichRecs;
				System.out.println("Total records "+(enrichRecs));
			}
				
			System.out.println("Retrieving records from enrichments");
			if(enrichRecs < 0)
				query = query + " limit "+orgTotal+" offset "+offset;
			else 
				query = query + " offset "+offset;
			
			System.out.println(query);
			res = st.executeQuery(query);
			int part = orgTotal/10;
			
			if(part==0)
				part = 1;
			
			int orgCount = 0;

			//for each record
			while (res.next()) {
				orgCount++;
				
				//Find the file to update
				String key = res.getString("hash");
				if(orgCount%1000 == 0)
					System.out.println(key+" "+(orgCount + offset)+"/" + (orgTotal + offset));
				String getRecord = "select * from record where hash='"+key+"'"; 
				ResultSet res2 = st2.executeQuery(getRecord);
				while (res2.next()) {
					String updatedJson = semEnrichment(res2.getString("json"),
							res.getString("property"),res.getString("value"),res.getString("label"));
		
					String upQuery = "update record set "+updateColumn+" = ?, created=? where hash = ?";
					Timestamp date = new Timestamp(new java.util.Date().getTime());
				    PreparedStatement preparedStmt = conn.prepareStatement(upQuery);
				    preparedStmt.setString(1, updatedJson);
				    preparedStmt.setTimestamp(2, date);
				    preparedStmt.setString(3, res2.getString("hash"));
				    preparedStmt.executeUpdate();

				}
				if(orgCount%part == 0)
					log.info("	Semantic  enriched " + (orgCount + offset)+"/" + (orgTotal + offset)
								+ " "+(orgCount/part)*10+" %");
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private String semEnrichment(String json, String property, String value, String label) {
		JSONObject jsonObj = JSONObject.fromObject(json);
		semEnrichmentJson(jsonObj, property.substring(1,property.length()-1), value, label);
		return jsonObj.toString(2);
	}

	private void semEnrichmentJson(JSONObject json, String property,String valueURI, String label){

		JSONObject classObject;
		JSONArray values;

		if(json.containsKey(("enriched_"+getClassOrProp(property)))){
			classObject = json.getJSONObject("enriched_"+getClassOrProp(property));
			values = classObject.getJSONArray("values");
		}
		else{
			classObject = new JSONObject();
			values = new JSONArray();
		}
		
		
		
		JSONObject valueObj = new JSONObject();
		if(!label.equals("")){
			valueObj.put( "lang", "en");
			valueObj.put( "text", label);
			valueObj.put( "uri", valueURI);
		}else{
			valueObj.put( "lang", "en");
			valueObj.put( "text", valueURI);
		}
		

		if(!json.containsKey(("enriched_"+getClassOrProp(property)))){
			values.add(valueObj);
			
			classObject.put( "ns", property);
			classObject.put( "edm", isEdm(property));
			classObject.put( "hasDomain", "http://www.europeana.eu/schemas/edm/ProvidedCHO");
			classObject.put( "values", values);

			json.put( "enriched_"+getClassOrProp(property), classObject);
			
		}else{
			Iterator<JSONObject> it = values.iterator();
			boolean duplicate = false;
			while(it.hasNext() && !duplicate){
				JSONObject entry = it.next();
				if(entry.containsKey("uri")){
					if (entry.get("text").equals(valueObj.get("text")) &&
						entry.get("uri").equals(valueObj.get("uri")))
						duplicate = true;
				}
				else
					if (entry.get("text").equals(valueObj.get("text")))
						duplicate = true;
			}
			
			if(!duplicate)
				values.add(valueObj);
		}
	}
	public void performImageEnrichments(int enrichRecs, int offset){
		Connection conn = null;
		try{
			//Query to get the catwalk files 
			String query;
			conn = getMySqlDBConnection();
			if (enrichRecs > 0)
				query = "select * from record where mint_org_id=1020 limit " + enrichRecs + "";
			else
				query = "select * from record where mint_org_id=1020";
			String countQuery = "select count(*) from record where mint_org_id=1020";


			Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			Statement st2 = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			ResultSet res;
			int orgTotal;
			if(enrichRecs < 0){
				res = st.executeQuery(countQuery);
				res.next();
				orgTotal = res.getInt("count(*)")-offset;
				System.out.println("Total records "+orgTotal);
				query = query + " limit "+orgTotal+" offset "+offset;
			}else{
				orgTotal = enrichRecs;
				System.out.println("Total records "+(enrichRecs));
				query = query + " offset "+offset;
			}
				
			System.out.println("Retrieving records from 1020");
			System.out.println(query);
			res = st.executeQuery(query);
			int part = orgTotal/10;
			
			if(part==0)
				part = 1;


			//for each record
			System.out.println("Updating records...");
			int orgCount = 0;
			while (res.next()) {
				orgCount++;

				//Find the file to update
				String edmFp = res.getString("xml");
				String key = getIsShownBy(edmFp).
						replaceAll("http://repos.europeanafashion.eu/catwalk/", "");
				String getEnrichments = "select * from image_analysis where filename='"+key+"'"; 
				ResultSet res2 = st2.executeQuery(getEnrichments);
				while (res2.next()) {
					String[] regions = new String[11];
					for(int i=0; i<11; i++)
						regions[i] = res2.getString("region"+(i+1));
					String updatedJson = imageEnrichment(res.getString("json"),regions);

					String upQuery = "update record set "+updateColumn+" = ?, created=? where hash = ?";
					Timestamp date = new Timestamp(new java.util.Date().getTime());
					PreparedStatement preparedStmt = conn.prepareStatement(upQuery);
					preparedStmt.setString(1, updatedJson);
					preparedStmt.setTimestamp(2, date);
					preparedStmt.setString(3, res.getString("hash"));
					preparedStmt.executeUpdate();

				}
				if(orgCount%part == 0)
					log.info("	Image analysis enriched " + (orgCount + offset)+"/" + (orgTotal + offset)
								+ " from 1020 "+(orgCount/part)*10+" %");

			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private String imageEnrichment(String json, String[] regions) {
		JSONObject jsonObj = JSONObject.fromObject(json);
		imageEnrichmentJson(jsonObj, 4, regions);
		return jsonObj.toString(2);
		
	}
	
	private void imageEnrichmentJson(JSONObject json, int regions, String[] regionsVal){

		JSONObject classObject = new JSONObject();
		JSONArray values = new JSONArray();
		
		if(json.containsKey(("enriched_image_color"))){
			json.remove("enriched_image_color");
		}

		for(int i = 0; i < regions; i++){
			String color = regionsVal[i].substring(0, regionsVal[i].indexOf(':'));
			
			JSONObject value = new JSONObject();
			value.put( "lang", "en");
			value.put( "text", color);
			value.put( "uri", getColorURI(color));
			values.add(value);
		}

		classObject.put( "ns", "http://www.heppnetz.de/ontologies/goodrelations/v1#color");
		classObject.put( "edm", false);
		classObject.put( "hasDomain", "http://www.europeana.eu/schemas/edm/ProvidedCHO");
		classObject.put( "values", values);

		json.put( "enriched_image_color", classObject);

	}
	
	private String getColorURI(String color){
		String colorId;
		if      (color.equals("black")) colorId = "10401";
		else if (color.equals("colours")) colorId = "10400";
		else if (color.equals("red")) colorId = "10414";
		else if (color.equals("silver")) colorId = "10408";
		else if (color.equals("metallic")) colorId = "10406";
		else if (color.equals("brown")) colorId = "10403";
		else if (color.equals("white")) colorId = "10416";
		else if (color.equals("orange")) colorId = "10411";
		else if (color.equals("beige")) colorId = "10778";
		else if (color.equals("grey")) colorId = "10405";
		else if (color.equals("purple")) colorId = "10413";
		else if (color.equals("gold")) colorId = "10407";
		else if (color.equals("blue")) colorId = "10402";
		else if (color.equals("transparent")) colorId = "10415";
		else if (color.equals("multicoloured")) colorId = "10410";
		else if (color.equals("copper")) colorId = "10409";
		else if (color.equals("green")) colorId = "10404";
		else if (color.equals("yellow")) colorId = "10417";
		else if (color.equals("pink")) colorId = "10412";
		else colorId = "11085";

				
		return "http://thesaurus.europeanafashion.eu/thesaurus/"+colorId;
	}
	
	private String getClassOrProp(String ns){
		if(ns.contains("#"))
			return ns.substring(ns.indexOf("#")+1);
		else{
			StringTokenizer st = new StringTokenizer(ns,"/");
			String result = "";
			while(st.hasMoreTokens())
				result = st.nextToken();
			return result;
		}
			
	}
	
	private boolean isEdm(String property){
		if(property.contains("id.loc.gov") || property.contains("europeanafashion"))
			return false;
		else return true;
	}
	
	
	
}
