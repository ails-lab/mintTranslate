package gr.ntua.ivml.mint.projects.fashion;

import java.io.File;
import java.io.InputStream;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.xml.sax.XMLReader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opensymphony.xwork2.util.TextParseUtil;

import gr.ntua.ivml.mint.OAIServiceClient;
import gr.ntua.ivml.mint.RecordMessageProducer;
import gr.ntua.ivml.mint.concurrent.GroovyTransform;
import gr.ntua.ivml.mint.concurrent.Queues;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.f.Interceptor;
import gr.ntua.ivml.mint.f.ThrowingConsumer;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Item;
import gr.ntua.ivml.mint.persistent.Mapping;
import gr.ntua.ivml.mint.persistent.PublicationRecord;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.persistent.XmlSchema;
import gr.ntua.ivml.mint.pi.messages.ExtendedParameter;
import gr.ntua.ivml.mint.pi.messages.ItemMessage;
import gr.ntua.ivml.mint.pi.messages.Namespace;
import gr.ntua.ivml.mint.util.Config;
import gr.ntua.ivml.mint.util.Counter;
import gr.ntua.ivml.mint.util.Interceptors;
import gr.ntua.ivml.mint.util.Interceptors.EndEstimateInterceptor;
import gr.ntua.ivml.mint.util.Interceptors.ProgressInterceptor;
import gr.ntua.ivml.mint.util.PublicationHelper;
import gr.ntua.ivml.mint.util.PublicationHelper.OaiItemPublisher;
import gr.ntua.ivml.mint.xml.transform.XSLTransform;
import gr.ntua.ivml.mint.xsd.ReportErrorHandler;
import gr.ntua.ivml.mint.xsd.SchemaValidator;
import nu.xom.Attribute;
import nu.xom.Builder;
import nu.xom.Element;
import nu.xom.Node;
import nu.xom.Nodes;

public class FashionEdmPublisher {

	private static final Logger log = Logger.getLogger(FashionEdmPublisher.class);

	public static final String FASHION_DEFAULT_ENRICHMENT_NAME = "Fashion default Enrichment";
	public static final String FASHION_XX_ENRICHMENT_NAME = "Fashion XX Enrichment";
	
	// Paths
	String xslPath = Config.getXSLDir()+System.getProperty("file.separator");

	final static String edmSchemaPrefix = "rdf"; // the prefix used for the OAI
	final static String edmSchemaUri = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	final static String edmFPSchemaPrefix = "edm_fp"; // the prefix used for the OAI
	final static String edmFPSchemaUri = "http://www.europeanafashion.eu/edmfp/";
	
	String queueHost,queueRoutingKey;
	String queueExchange;
	String oaiServerHost ,oaiServerPort ;
	String reportId;
	
	final static String dcUriNamespace = "http://purl.org/dc/elements/1.1/";
	final static String dctermsUriNamespace = "http://purl.org/dc/terms/" ;
	final static String rdfUriNamespace = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

	
	
	RecordMessageProducer rmp;
	
	String updateColumn = "json";

	private XSLTransform edmTransformer;

	private Builder builder=null;
	private OAIServiceClient osc;
	
	private static Map<String, String> creatorEnrich = new HashMap<>();
	private static Map<String, String> spatialEnrich = new HashMap<>();
	private static long lastAccess = 0;
	
	private Map<String, Integer> enrichCounter = new HashMap<>();
	
	private ObjectMapper om = new ObjectMapper();
	

	static class LongCounter {
		private long l;
		public LongCounter( long l ) {
			set( l );
		}
		public long get( ) { return l; }
		public void set( long l ) {
			this.l = l;
		}
		public long inc() {
			l+=1; return l;
		}
		public boolean lower( long limit ) {
			return l<limit;
		}
	}
	
	public FashionEdmPublisher() {
		try {
			queueHost = Config.get( "queue.host" );
			queueRoutingKey = Config.get( "fashion.queue.routingKey" );
			queueExchange = Config.get( "fashion.queue.exchange" );
			
			oaiServerHost = Config.get("fashion.oai.server");
			oaiServerPort = Config.get("fashion.oai.port");

			rmp = new RecordMessageProducer(queueHost, queueExchange );
			osc = new OAIServiceClient(oaiServerHost, Integer.parseInt( oaiServerPort));

		} catch (Exception e) {
			log.error( "", e );
		}
	}
		

	/*
	 * private static String fixNodesUrl( nu.xom.Nodes nodes, String provider ) {
	 * String resUrl = ""; for( int i=0; i<nodes.size(); i++ ) { nu.xom.Node n =
	 * nodes.get(i); if( n instanceof nu.xom.Attribute ) { nu.xom.Attribute att =
	 * (nu.xom.Attribute) n; String oldUrl = att.getValue(); // fix url String
	 * newUrl = reposUrl( provider, oldUrl) ; att.setValue( newUrl ); resUrl =
	 * newUrl; } } // we might want that return resUrl; }
	 * 
	 * private static void replaceShownBy( nu.xom.Nodes nodes, String shownByUrl ) {
	 * for( int i=0; i<nodes.size(); i++ ) { nu.xom.Node n = nodes.get(i); if( n
	 * instanceof nu.xom.Attribute ) { nu.xom.Attribute att = (nu.xom.Attribute) n;
	 * String oldUrl = att.getValue(); String newUrl = oldUrl.replace("%isShownBy%",
	 * shownByUrl ); // fix url att.setValue( newUrl ); } } }
	 */	
	/*
	 * private String repairXml( String edm) throws Exception { // parse
	 * nu.xom.Document doc = getXomBuilder().build(edm, null); // get the provider
	 * //nu.xom.Nodes nodes = XQueryUtil.xquery( doc ,
	 * "//*[local-name()='Aggregation']/*[local-name()='dataProvider']");
	 * nu.xom.Nodes nodes =
	 * doc.query("//*[local-name()='Aggregation']/*[local-name()='dataProvider']");
	 * if( nodes.size() == 0 ) { log.error( "No provider!"); return null; }
	 * 
	 * String provider = nodes.get(0).getValue().trim(); // get the nodes that need
	 * fixing //nodes = XQueryUtil.xquery( doc ,
	 * "//*[local-name()='isShownBy']/@*[local-name()='resource']"); nodes =
	 * doc.query("//*[local-name()='isShownBy']/@*[local-name()='resource']");
	 * String isShownBy = fixNodesUrl( nodes, provider );
	 * 
	 * // more to fix ? //nodes = XQueryUtil.xquery( doc ,
	 * "//*[local-name()='Aggregation']/*[local-name()='hasView']/@*[local-name()='resource']"
	 * ); nodes = doc.query(
	 * "//*[local-name()='Aggregation']/*[local-name()='hasView']/@*[local-name()='resource']"
	 * ); fixNodesUrl( nodes, provider );
	 * 
	 * if( StringUtils.isEmpty(isShownBy)) { log.error( "No isShownBy found !"); //
	 * is invalid most likely } else { nodes =
	 * doc.query("//*[local-name()='object']/@*[local-name()='resource']");
	 * replaceShownBy(nodes, isShownBy ); } return doc.toXML(); }
	 * 
	 */	
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
	 * Read two column tsv file into given Map. TSV should be literal\turi\n
	 * @param resourceName
	 * @param targetMap
	 * @throws Exception
	 */
	private void tsvToMap( String resourceName, Map<String,String> targetMap ) throws Exception  {
		InputStream tsvStream = null;
		try {
			synchronized( targetMap ) {
				tsvStream = FashionEdmPublisher.class.getClassLoader().getResourceAsStream( resourceName );
				List<String> lines = IOUtils.readLines(tsvStream, "UTF8");
				targetMap.clear();
				for( String line : lines ) {
					String columns[] = line.split("\\t",2);
					if( !StringUtils.isEmpty(columns[0]) && !StringUtils.isEmpty( columns[1]) ) {
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
		PublicationRecord pr = null;
		XmlSchema fashionEuropeanaEdm = DB.getXmlSchemaDAO().getByName( Config.get( "fashion.oai.schema" )); 
		try {
			LongCounter startTime = new LongCounter( System.currentTimeMillis());
			
			ArrayList<ExtendedParameter> params = getOaiReport(ds.getDbID().intValue(), (int) ds.getOrganization().getDbID());
			if( reportId != null )
				ds.logEvent( "Publication started", "Report id " + reportId );
			else {
				ds.logEvent( "Couldn't obtain oai reportId" );
				throw new Exception( "No OAI report id");
			}
			
			pr = DB.getPublicationRecordDAO().getByPublishedDatasetTarget( ds, "fashion.europeana" ).orElse(null);
			
			// in case this was started somewhere else, we dont do much
			if( pr == null ) {
				log.error( "Publication is not running");
				ds.logEvent("Publication is not running");
			}

			final Counter itemCounter = new Counter(0);
			final Counter insertCounter = new Counter(0);
			final Counter invalidEdm = new Counter(0);
			final int sourceDatasetId =  ds.getOrigin().getDbID().intValue();
			final boolean isEdmFp = PublicationHelper.isSchemaAndHasValidItems(ds, Config.get( "fashion.portal.schema" ));;
			
			ds.processAllValidItems( (Item item) -> {
				String edmFp = item.getXml();
				String edm = edmFp;
				

				// check if its edmfp /http://www.europeanafashion.eu/edmfp/ in it
				// probably better to check if the ds is Config fashion.portal.schema 
				if( isEdmFp ) {
					XmlSchema xs = ds.getSchema();
					edm =  edmFp2edmTransformation(edmFp,"", (int)ds.getOrganization().getDbID() );
//					String edmFixed = repairXml( edm );
					String enrichedEdm = manualEnrich( edm );
					edm = enrichedEdm;
					if( !edmValidate( edm,fashionEuropeanaEdm)) {
						invalidEdm.inc();
						edm = null;
					}

					//Publish EDM FP
					oaiPublishItem(xs, sourceDatasetId, ds.getDbID().intValue(),
							(int) ds.getOrganization().getDbID(),
							item.getDbID(), edmFp, edmFPSchemaPrefix, edmFPSchemaUri,params);
					insertCounter.inc();
				} 

				//Publish EDM
				if( edm != null ) {
					oaiPublishItem(fashionEuropeanaEdm, sourceDatasetId, ds.getDbID().intValue(),
							(int) ds.getOrganization().getDbID(),
							item.getDbID(), edm, edmSchemaPrefix, edmSchemaUri,params);
					itemCounter.inc();
					insertCounter.inc(); // edm
					insertCounter.inc(); // oai_dc

				} else {
					log.info( "EDM invalid "+ item.getLabel());
				}

				// if 2 min have passed, 
				if(( startTime.get()>0) &&
						 startTime.lower( System.currentTimeMillis() - 2*60*1000l )) {
					startTime.set( -1l);
					int percentage = itemCounter.get()*1000/ds.getValidItemCount();
					ds.logEvent( String.format("%2.1f", percentage/10.0 ) + "% processed.",
							itemCounter.get() + " / " + ds.getValidItemCount());
					DB.commit();
				}

			}, false);
			
			long lastChange = System.currentTimeMillis();
			int lastTotal=0;

			startTime.set( System.currentTimeMillis());
			
			while( true ) {
				int currentTotal = osc.getProgress(reportId).getTotalRecords();
				int inserted  = osc.getProgress(reportId).getInsertedRecords();
				int conflicts = osc.getProgress(reportId).getConflictedRecords();


				String report = "Total: " + currentTotal + " Inserted: " + inserted + " Conflicts: " + conflicts;
				if( invalidEdm.get() != 0 ) {
					report += " Invalid Edm " + invalidEdm.get();
				}

				if(( startTime.get()>0) &&
						 startTime.lower( System.currentTimeMillis() - 2*60*1000l )) {
					startTime.set( -1l);
					int percentage = (int) ((inserted+conflicts)*1000l/insertCounter.get());
					ds.logEvent( String.format("%2.1f", percentage/10.0 ) + "% inserted.",
							report );
					DB.commit();
				}

				
				// likely inserted/2+conflicts needs to be itemCounter
				// inserted dc and edm namespace and rejected items only once counted 
				if( currentTotal != lastTotal ) {
					lastChange = System.currentTimeMillis();
					lastTotal = currentTotal;
				}
				else {
					if(( System.currentTimeMillis() - lastChange ) > 1000l*60l*10l ) {
						log.warn( "Timeout occured in Publication.");
						ds.logEvent("Publication stopped", report );
						break;
					}
				}
				// should be equal, but lets be safe here
				if(( inserted + conflicts ) >= insertCounter.get()) {
					ds.logEvent("Publication finished", report );
					break;					
				}
				Thread.sleep( 10000l );
			}
			
			pr.setReport(osc.getProgress(reportId).toString());
			pr.setStatus(Dataset.PUBLICATION_OK);
			pr.setPublishedItemCount(itemCounter.get());

			log.info( "DS #"+ ds.getDbID()+" EnrichCounter:\n"+om.writeValueAsString(enrichCounter));
			enrichCounter.clear();
		} catch( Exception e ) {
			log.error("", e );
			pr.setStatus( Dataset.PUBLICATION_FAILED);
			pr.setReport(e.getMessage());
		} finally {
			if( osc != null ) { 
				if( reportId != null )
					osc.closeReport(reportId);
				osc.close();
			}
			
			try {
				if( rmp != null ) rmp.close();
			} catch( Exception e2) { log.error( "", e2);}
			
			if( pr != null ) {
				pr.setEndDate(new Date());
				DB.commit();	
			}
		}
	}
	
	private ArrayList<ExtendedParameter> getOaiReport(int mintDatasetID, int mintOrgID ){
		
		ArrayList<Integer> datasetIds = new ArrayList<Integer>();
		datasetIds.add(new Integer(mintDatasetID)); 
		reportId = osc.createReport(queueRoutingKey, 1000, mintOrgID, datasetIds );
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

	private void oaiPublishItem(XmlSchema xs, int sourceDatasetId, int mintDatasetID, int mintOrgID,
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
			im.setSourceDataset_id(sourceDatasetId);
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

	
	// makes the default enrichment on given dataset
	public static void defaultEnrichAsync( Dataset ds, boolean markEnrichAsSoftwareAgent ) throws Exception {
		List<XmlSchema> lxs = XmlSchema.getSchemasFromConfig("fashionxx.oai.schema");
		// should be only one
		XmlSchema annotatedTargetSchema = lxs.get(0);

		Interceptor<Item, Item> enrichInter = new Interceptors.ItemDocWrapInterceptor( 
				Interceptor.modifyInterceptor( doc -> ManualEnrich.manualEnrich( doc,  markEnrichAsSoftwareAgent )), true );
		GroovyTransform enrichTransform =  GroovyTransform.defaultFromDataset( ds, enrichInter, Optional.of( t -> {
			t.setSchema( annotatedTargetSchema );
			t.setName( FASHION_DEFAULT_ENRICHMENT_NAME );
		}));
		
		Queues.queue(enrichTransform, "db");
	}
	
	// prepare the fp shaped dataset for publication .. apply edmfp->edm and 
	// add the default enrichments
	// flag if you want the standard enrichments to come with "SoftwareAgent" edm:wasGeneratedBy
	public static void europeanaPrepareAsync( Dataset fpDataset, boolean markEnrichAsSoftwareAgent ) throws Exception  {
		// wired org 1003 needs var3 to be "false"
		DB.SessionRunnable r = () -> {
			// get into the local hibernate session
			Dataset localFpDataset = DB.getDatasetDAO().getById(fpDataset.getDbID(), false );
			try {
				Mapping m = DB.getMappingDAO().simpleGet( "name='fp2edm6'");
				if( m==null) throw new Exception( "Mapping fp2edm6 not available");
				Interceptor<Item, Item> enrichInter = new Interceptors.ItemDocWrapInterceptor( 
						Interceptors.interceptorFromMapping(m, localFpDataset)
						.into(
						  Interceptor.modifyInterceptor( doc -> ManualEnrich.manualEnrich( doc,  markEnrichAsSoftwareAgent )))
						, true );
				
				List<XmlSchema> lxs = XmlSchema.getSchemasFromConfig("fashionxx.oai.schema");
				// should be only one
				XmlSchema annotatedTargetSchema = lxs.get(0);
				
				GroovyTransform enrichTransform =  GroovyTransform.defaultFromDataset(localFpDataset, enrichInter, Optional.of( t -> {
					t.setSchema( annotatedTargetSchema );
					t.setName("Transformed to EDM and enriched");
				}));
				enrichTransform.runInThread();
			} catch( Exception e ) {
				log.error( "'Prepare for Europeana' failed" , e);
				localFpDataset.logEvent( "'Prepare for Europeana' failed", e.getMessage() );
			}
		};
		
		Queues.queue(r, "db");
	}
	
/**
 * Newest (2/22) version of fashion publish. Sends explicit and without change the edm dataset and edmfp parent to the OAI
 * After some prep can be used for XX datasets and normal ones.
 * @param ds
 * @param publisher
 * @throws Exception
 */
	public static void europeanaPublishAsync( Dataset ds, User publisher) throws Exception {
		// publish just ds and potentially a parent edm-fp set
		Set<Long> schemaIds = XmlSchema
				.getSchemasFromConfig("fashion.portal.schema")
				.stream()
				.map( XmlSchema::getDbID)
				.collect( Collectors.toSet());
		// find first parent with that schema
		Dataset parent = ds.getParentDataset();
		while( parent != null ) {
			XmlSchema parentSchema = parent.getSchema();
			if( parentSchema != null )
				if( schemaIds.contains(parent.getSchema().getDbID())) break;
			parent = parent.getParentDataset();
		}
		final Dataset edmFpDataset = parent;
		final PublicationRecord pr = PublicationRecord.createPublication(publisher, ds, "fashion.europeana");
		
		// create the OAIpublisher
		final OaiItemPublisher oaiItemPublisher = new OaiItemPublisher( Config.get( "fashion.queue.exchange" ), Config.get( "fashion.queue.routingKey"),
				Config.get( "fashion.oai.server"), Integer.parseInt(Config.get( "fashion.oai.port")));
		
		
		long tmpTotalCount = ds.getInvalidItemCount();
		if( edmFpDataset != null ) {
			tmpTotalCount += edmFpDataset.getValidItemCount();
			ds.logEvent("Sending two datasets to OAI");
		}
		final long totalCount = tmpTotalCount;
		
		DB.SessionRunnable r =() -> {
			// Get PubRec ds fpds in session
			
			Dataset localEdmFpDs = edmFpDataset==null?
					null:
					DB.getDatasetDAO().getById(edmFpDataset.getDbID(), false );
			Dataset localDs = DB.getDatasetDAO().getById(ds.getDbID(), false );
			PublicationRecord localPr = DB.getPublicationRecordDAO().getById(pr.getDbID(), false);

			// run it with end guess and progress markers in log
			try (ProgressInterceptor progressInterceptor = new ProgressInterceptor("Published 'fashion.europeana' %d of %d for Dataset #"+localDs.getDbID(), totalCount)) {

				// Set it up: endestimate->progress->script ( storingConsumer)
				oaiItemPublisher.setDataset(ds, edmSchemaPrefix, edmSchemaUri);
				ThrowingConsumer<Item> itemSink = item -> oaiItemPublisher.sendItem(item);

				final ThrowingConsumer<Item> sourceConsumer = new EndEstimateInterceptor(totalCount,
						localDs)
						.into(progressInterceptor)
						.intercept(itemSink);

				ds.processAllValidItems(item -> sourceConsumer.accept(item), true );
				if( localEdmFpDs != null ) {
					oaiItemPublisher.setDataset(localEdmFpDs, edmFPSchemaPrefix, edmFPSchemaUri);
					localEdmFpDs.processAllValidItems(item -> sourceConsumer.accept(item), true );
				}			
			} catch( Exception e ) {
				log.error( "Publication failed", e );
				pr.setStatus(PublicationRecord.PUBLICATION_FAILED);
				pr.setEndDate(new Date());
				pr.setPublishedItemCount(-1);
				pr.setReport(e.getMessage());
				DB.getPublicationRecordDAO().makePersistent(localPr);
			} finally {
				// this sets the pr
				oaiItemPublisher.finishPublication(localPr);
			}
		};
		
		Queues.queue(r, "db" );
	}
}
