package gr.ntua.ivml.mint.projects.euscreen;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.FlushMode;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opensymphony.xwork2.util.TextParseUtil;

import gr.ntua.ivml.mint.concurrent.Queues;
import gr.ntua.ivml.mint.concurrent.XSLTransform;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.AnnotatedDataset;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Item;
import gr.ntua.ivml.mint.persistent.Mapping;
import gr.ntua.ivml.mint.persistent.PublicationRecord;
import gr.ntua.ivml.mint.persistent.Transformation;
import gr.ntua.ivml.mint.persistent.XpathHolder;
import gr.ntua.ivml.mint.util.ApplyI;
import gr.ntua.ivml.mint.util.Config;
import gr.ntua.ivml.mint.util.Jackson;
import nu.xom.Attribute;
import nu.xom.Element;

public class CoreToEuropeana {

	static Logger log = Logger.getLogger( CoreToEuropeana.class );

	static String coreSchema = "EUscreenXL ITEM/CLIP v2";
		
	static String[] normOrgInit = new String[] { "DW", "Deutsche Welle",
				"KB" ,"Kungliga biblioteket",
				"TVC", "TV3 Televisió de Catalunya (TVC)",
				"NISV", "Netherlands Institute for Sound and Vision",
				"NINA", "Narodowy Instytut Audiowizualny",
				"SASE", "Screen Archive South East",
				"ERT", "ERT SA",
				"HeNAA" ,"ERT SA"
	};

	static HashMap<String,String> normalizeOrg = new HashMap<String, String>();
	static {
		for( int i=0; i<normOrgInit.length; i+= 2 ) 
			normalizeOrg.put( normOrgInit[i], normOrgInit[i+1]);
	};

	static List<String> screenshotInsertPos = Arrays.asList( 
			"{http://www.europeana.eu/schemas/edm/}:aggregatedCHO" ,
			"{http://www.europeana.eu/schemas/edm/}:dataProvider",
			"{http://www.europeana.eu/schemas/edm/}:hasView",
			"{http://www.europeana.eu/schemas/edm/}:isShownAt",
			"{http://www.europeana.eu/schemas/edm/}:isShownBy",
			"{http://www.europeana.eu/schemas/edm/}:object",
			"{http://www.europeana.eu/schemas/edm/}:preview",
			"{http://www.europeana.eu/schemas/edm/}:provider",
			"{http://purl.org/dc/elements/1.1/}:rights" ,
			"{http://www.europeana.eu/schemas/edm/}:rights",
			"{http://www.europeana.eu/schemas/edm/}:ugc"
	);
	
	//   xmlns:edm="http://www.europeana.eu/schemas/edm/"
	// rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#" local-name="Resource" attribute to the edm:object


	static List<String> linkInsertPos = Arrays.asList(
				// basetype in ProvidedCHO is unordered
				"{http://purl.org/dc/terms/}:hasPart",
				"{http://purl.org/dc/terms/}:isPartOf",

				// extension is ordered but has to come after base type anyway
				"{http://www.europeana.eu/schemas/edm/}:currentLocation",
				"{http://www.europeana.eu/schemas/edm/}:hasMet" ,
				"{http://www.europeana.eu/schemas/edm/}:hasType" ,
				"{http://www.europeana.eu/schemas/edm/}:incorporates",
				"{http://www.europeana.eu/schemas/edm/}:isDerivativeOf",
				"{http://www.europeana.eu/schemas/edm/}:isNextInSequence" ,
				"{http://www.europeana.eu/schemas/edm/}:isRelatedTo",
				"{http://www.europeana.eu/schemas/edm/}:isRepresentationOf",
				"{http://www.europeana.eu/schemas/edm/}:isSimilarTo",
				"{http://www.europeana.eu/schemas/edm/}:isSuccessorOf",
				"{http://www.europeana.eu/schemas/edm/}:realizes",

				
				"{http://www.europeana.eu/schemas/edm/}:type"
				
			);

	public static class NoterikInfo {
		public String id;
		public boolean isPublic;
		public String screenShot;
		public String provider;
		public String documentUrl;
		public String type;
	}
	

	/**
	 * Go over all core records and transform to edm with the given mapping.
	 * Wait after this for all transformations to happen (can't be very long ...60k items)
	 */
	public static void transformNotTransformedCore(final  Mapping mapping ) throws Exception {
		final Set<String> schemas= TextParseUtil.commaDelimitedStringToSet(Config.get("euscreen.portal.schema"));
		final String oaischema = Config.get( "oai.schema" );

		DB.getSession().setFlushMode(FlushMode.COMMIT);
		DB.getPublicationRecordDAO().onAll( new ApplyI<PublicationRecord>() {
			public void apply( PublicationRecord pr ) {
				if( schemas.contains( pr.getPublishedDataset().getSchema().getName())) {
					// is it transformed
					Dataset ds = pr.getPublishedDataset();
					Dataset ds2 = ds.getBySchemaName( oaischema );
					if( ds2 == null ) {
						// ok, there is no edm version yet
						Transformation tr = Transformation.fromDataset( ds, mapping);
						DB.getTransformationDAO().makePersistent( tr );
						DB.commit();
						log.info( "Created Transformation " + tr.getDbID());
						XSLTransform trans = new XSLTransform( tr );
						Queues.queue( trans, "db");
						log.info( "Queued " + ds.getName()+ " with " + ds.getItemCount() + " items" );
					}
				}
			}
		}, null, false );
		DB.getSession().setFlushMode(FlushMode.AUTO);
		log.info( "All datasets queued in db for transform.");
	}
	
	// find which records are not in Portal visible and mark them invalid
	public static void invalidateNotApprovedEdm( Dataset ds, final Map<String, NoterikInfo> noterik  ) {
		try {
			ds.processAllItems( new ApplyI<Item>() {

				@Override
				public void apply(Item i) throws Exception {
					Item source = i.getSourceItem();
					String id = source.getPersistentId();
					NoterikInfo ni = noterik.get(id);
					if( (ni == null ) || (!ni.isPublic)) {
						if( i.isValid()) {
							i.setValid( false );
							log.debug(id + " is not in approved list.");
						}
					}
				}						
			}, true );
		} catch( Exception e ) {
			log.error( "Problem invalidating in ds #" + ds.getDbID());
		}
	}
	
	
	
	// set missing item root path on annotated datasets, so they can be transformed
	public static void cleanAnnotated() {
		List<AnnotatedDataset> annotatedDatasets = DB.getAnnotatedDatasetDAO().findAll();
		for( Dataset ds: annotatedDatasets) {
			if( ds instanceof AnnotatedDataset ) {
				Dataset parent = ((AnnotatedDataset)ds).getParentDataset();
				XpathHolder xpath = ds.getItemRootXpath();
				if( xpath == null ) {
					if( parent != null ) {
						if( parent.getItemRootXpath() != null ) {
							XpathHolder alt = ds.getByPath( parent.getItemRootXpath().getQueryPath());
							if( alt != null ) {
								ds.setItemRootXpath( alt );
								log.info( "Fixed " + ds.getDbID() + " with itemRootPath");
							}
						}
					}
				}
			}
		}
	}
	

	// build series info for all core records (old, new and series records)
	// get info from all portal published records in a list of maps
	public static List<Map<String,?>> buildSeriesInfo ( ) {
		final List allRecords = new ArrayList<Map<String,?>>();

		// collect series information
		Set<String> schemas= TextParseUtil.commaDelimitedStringToSet(Config.get("euscreen.portal.schema"));

		List<Long> prs = DB.getPublicationRecordDAO().listIds( Optional.empty() );
		for( Long prId: prs  ) {
			PublicationRecord pr = DB.getPublicationRecordDAO().getById(prId, false);
			if( schemas.contains( pr.getPublishedDataset().getSchema().getName())) {
				try {
					pr.getPublishedDataset().processAllValidItems(new ApplyI<Item>() {
						public void apply( Item item ) {
							try {
								String seriesTitle= item.getValue( "//*[local-name()='TitleSetInEnglish']/*[local-name()='seriesOrCollectionTitle']");
								String provider = item.getValue("//*[local-name()='provider']" );
								String series = item.getValue( "//*[local-name()='recordType']");
								String eusId = item.getValue( "//*[local-name()='identifier']");
								boolean isSeries = "SERIES/COLLECTION".equals( series );
								Map<String, Object> info = new HashMap<String, Object>();
								if( normalizeOrg.containsKey( provider ))
									provider = normalizeOrg.get( provider );
								info.put( "itemId",  item.getDbID());
								info.put("provider",  provider);
								info.put("seriesTitle", seriesTitle);
								info.put( "id", eusId);
								// is it a clip or a series record
								info.put( "isSeries", isSeries );

								allRecords.add( info );
							}  catch( Exception e ) {
								log.error( "Exception during reading item #" + item.getDbID() );
							}
						}
					}
					, false);
				} catch( Exception e ) {
					log.error( "Not expecting exception in processing ds #" + pr.getPublishedDataset().getDbID());
				}
			}
			DB.getSession().clear();
		}
		return allRecords;
	}

	// fill the eusIdIndex, and seriesTitleIndex and allow only approved entries
	// you need to provide empty maps
	// noterik Map provides info on portal approved entries
	// seriesInfo is the series related info in Mint
	// eusIdIdx and seriesIdx are output
	public static void  buildSeriesIndex( List<Map<String,?>> seriesInfo, Map<String, Map> eusIdIndex, Map<String, Map> seriesTitleIndex, Map<String, NoterikInfo> noterik ) {
		
		// filter out entries that are not approved
		Iterator<Map<String,?>> listIter = seriesInfo.iterator();
		while( listIter.hasNext() ) {
			Map<String, ?> info = listIter.next();
			String id = (String) info.get("id");
			NoterikInfo ni = noterik.get(id );
			if(( ni == null) || (!ni.isPublic)) listIter.remove();
		}
		
		// build a small index into the records
		for( Map<String, ?> info: seriesInfo ) {
			eusIdIndex.put( (String) info.get( "id" ), info );
		}
		
		for( Map<String, ?> info: seriesInfo ) {
			if( (Boolean) info.get( "isSeries")) {
				seriesTitleIndex.put( (String) info.get("provider") + ":"+ (String) info.get( "seriesTitle"), info);
			}
		}

		
		// clean the index
		// all ids that have no series title or no series title with series record go out
		Iterator<Map.Entry<String, Map>> iter = eusIdIndex.entrySet().iterator();
		while( iter.hasNext() ) {
			Map.Entry<String, Map> entry = iter.next();
			Map<String, Object> info = entry.getValue();
			
			// do nothing with series records
			if( (Boolean) info.get( "isSeries")) continue;
			
			String seriesTitle = (String) info.get( "seriesTitle" );
			String provider = (String) info.get("provider");
			
			if( StringUtils.isEmpty( seriesTitle ) ||
				!seriesTitleIndex.containsKey( provider+":"+seriesTitle )) {
				iter.remove();
				continue;
			}
			
			// series epsiodes are added to the series record
			Map<String, Object> seriesRecord = seriesTitleIndex.get( provider+":"+seriesTitle );
			// should not be empty but check anyway
			if( seriesRecord == null ) {
				log.error( "There really should be a series record for " + seriesTitle );
				continue;
			}
			List episodes = (List) seriesRecord.get("allEpisodes");
			if( episodes == null ) {
			    episodes = new ArrayList<String>(); 
				seriesRecord.put( "allEpisodes",  episodes );
			}
			episodes.add( entry.getKey());
		}
	}
	
	
	
	public static void installSeriesLinks( Dataset edmSet,  
			final Map<String, Map> eusIdIndex, final Map<String,Map> seriesTitleIndex ) throws Exception {
		edmSet.processAllItems(new ApplyI<Item>() {
			public void apply( Item item ) {
				try {
					String id = item.getValue( "//*[local-name()='Aggregation']/@*[local-name()='about']");
					String cho = item.getValue( "//*[local-name()='ProvidedCHO']/@*[local-name()='about'] " );
					String eusId = id.replaceFirst( ".*(EUS_.{32}).*", "$1");

					if( eusIdIndex.containsKey( eusId )) {
						Map info = eusIdIndex.get(  eusId );
						if( (Boolean) info.get( "isSeries")  ) {
							// series record entries
							List<String> episodes  = (List<String>) info.get( "allEpisodes" );
							if( episodes != null ) {
								for( String episodeId: episodes ) {
									Element newElem = item.insertElement( "hasPart","http://purl.org/dc/terms/", linkInsertPos, "//*[local-name()='ProvidedCHO']" );
									Attribute att = new Attribute( "rdf:resource", "http://www.w3.org/1999/02/22-rdf-syntax-ns#", cho.replace(eusId, episodeId));
									newElem.addAttribute(att);
									log.info( "Series link installed in " + eusId + " Item: " + item.getDbID());
								}
							}
						} else {
							String seriesTitle = (String) info.get( "seriesTitle" );
							String provider = (String) info.get("provider");
							Map seriesRecord = (Map) seriesTitleIndex.get(  provider+":"+seriesTitle );
							String seriesId = (String) seriesRecord.get( "id" );

							Element newElem = item.insertElement( "isPartOf","http://purl.org/dc/terms/", linkInsertPos, "//*[local-name()='ProvidedCHO']" );
							Attribute att = new Attribute( "rdf:resource", "http://www.w3.org/1999/02/22-rdf-syntax-ns#", cho.replace(eusId, seriesId));
							newElem.addAttribute(att);
						}
						item.setXml(item.getDocument().toXML());
					}

				} catch( Exception e ) {
					log.error( "Series link install problem", e );
				}
			}
		}, true );
	}

	public static void fixDcSource( final Dataset edmSet ) throws Exception {  
		edmSet.processAllItems(new ApplyI<Item>() {
			public void apply( Item item ) {
				try {
					if( edmSet.getOrganization().getDbID() == 1025l ) {
						item.setValue( "//*[local-name()='source' and starts-with( ., 'EUscreenXL')]", "EUscreen Project 2009-2012" );
						item.setXml(item.getDocument().toXML());
					}
				} catch( Exception e ) {
					log.error( "dc:source not fixed", e );
				}
			}
		}, true );
	}
	
	/**
	 * Maybe this could be an extra transform, not an inplace change ...
	 * @param edmSet Dataset to be opstProcessed in place 
	 * @param xslt An XSLTransform with the XSL already set
	 * @throws Exception - should actually be caught and stuff logged
	 */
	public static void xslProcess( final Dataset edmSet, final gr.ntua.ivml.mint.xml.transform.XSLTransform xslt ) throws Exception {
		edmSet.processAllItems(new ApplyI<Item>() {
			public void apply( Item item ) {
				try {
					String inXml = item.getXml();
					String out = xslt.transform(inXml);
					item.setXml( out );
				} catch( Exception e ) {
					log.error( "Custom Transform not applied", e );
				}
			}
		}, true );
	}
	
	public static Map<String, NoterikInfo> readNoterikInfoUrl( String url ) {
		try {
			InputStream is = new URL( url).openStream();
			return readNoterikInfo( is );
		} catch( Exception e ) {
			log.error( "Couldnt open url '" + url + "' as Stream.", e );
		}
		return Collections.emptyMap();
	}
	
	
	public static Map<String,NoterikInfo> readNoterikInfoResource( String name ) {
		InputStream is = CoreToEuropeana.class.getResourceAsStream( name );
		if( is != null )
			return readNoterikInfo( is );
		else
			log.error( "Couldnt read '" + name + "' as Stream." );
		return Collections.emptyMap();
	}
	
	
	/**
	 * Read given file and return a hashmap of euscreen id and screenshot url
	 * @param filename
	 * @return
	 */
	public static Map<String,NoterikInfo> readNoterikInfo( InputStream is ) {
		HashMap<String, NoterikInfo> result = new HashMap<String, NoterikInfo>();
		ObjectMapper om = Jackson.om();
		
		try {
			List<NoterikInfo> allNoterik = om.readValue(is, new TypeReference<List<NoterikInfo>>(){});
			for( NoterikInfo ni: allNoterik ) 
				result.put( ni.id, ni );
		} catch( Exception e ) {
			log.error( "Stream couldn't be read for screenshots!", e );
		} finally {
			IOUtils.closeQuietly(is);
		}
		return result;
	} 

	public static void installScreenShots( Dataset edmSet, final Map<String, NoterikInfo> screenShotUrls ) {
		try {
			edmSet.processAllItems(new ApplyI<Item>() {
				public void apply( Item item ) {
					try {
						// rdf:about
						String id = item.getValue( "//*[local-name()='Aggregation']/@*[local-name()='about']");
						String eusId = id.replaceFirst( ".*(EUS_.{32}).*", "$1");

						if( screenShotUrls.containsKey( eusId )) {
							NoterikInfo ni = screenShotUrls.get( eusId );
							Element edmObject = item.insertElement( "object", "http://www.europeana.eu/schemas/edm/", 
									screenshotInsertPos, "//*[local-name()='Aggregation']" );
							Attribute att = new Attribute( "rdf:resource", "http://www.w3.org/1999/02/22-rdf-syntax-ns#", ni.screenShot );
							edmObject.addAttribute( att );
							item.setXml( item.getDocument().toXML());
						}
					} catch( Exception e ) {
						log.error( "Screen shot insert problem", e );
					}
				}
			}, true );
		} catch( Exception e ) {
			log.error( "Unexpected Exception setting screenshots on ds #" + edmSet.getDbID());
		}
	}
}
