package gr.ntua.ivml.mint.projects.euscreen;

import static gr.ntua.ivml.mint.api.RequestHandler.errJson;
import static gr.ntua.ivml.mint.api.RequestHandler.okJson;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.xml.sax.XMLReader;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opensymphony.xwork2.util.TextParseUtil;

import gr.ntua.ivml.mint.api.RequestHandlerUtil;
import gr.ntua.ivml.mint.concurrent.GroovyTransform;
import gr.ntua.ivml.mint.concurrent.Queues;
import gr.ntua.ivml.mint.concurrent.Solarizer;
import gr.ntua.ivml.mint.concurrent.Ticker;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.f.ErrorCondition;
import gr.ntua.ivml.mint.f.Interceptor;
import gr.ntua.ivml.mint.f.ThrowingBiConsumer;
import gr.ntua.ivml.mint.f.ThrowingConsumer;
import gr.ntua.ivml.mint.mapping.model.Mappings;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Item;
import gr.ntua.ivml.mint.persistent.Mapping;
import gr.ntua.ivml.mint.persistent.PublicationRecord;
import gr.ntua.ivml.mint.persistent.Transformation;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.persistent.XmlSchema;
import gr.ntua.ivml.mint.projects.euscreen.CoreToEuropeana.NoterikInfo;
import gr.ntua.ivml.mint.util.ApplyI;
import gr.ntua.ivml.mint.util.CachedObject;
import gr.ntua.ivml.mint.util.Config;
import gr.ntua.ivml.mint.util.Counter;
import gr.ntua.ivml.mint.util.Interceptors;
import gr.ntua.ivml.mint.util.Jackson;
import gr.ntua.ivml.mint.util.PublicationHelper;
import gr.ntua.ivml.mint.util.StringUtils;
import gr.ntua.ivml.mint.xml.transform.XMLFormatter;
import gr.ntua.ivml.mint.xml.transform.XSLTGenerator;
import gr.ntua.ivml.mint.xml.transform.XSLTransform;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import nu.xom.Attribute;
import nu.xom.Builder;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Nodes;




public class EuscreenPublish {
	public static final String EUSCREEN_DEFAULT_ID = "EUS_00000000000000000000000000000000";
	
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

	public static CachedObject<AllSeriesInfo> cache ;

	public static class NoterikInfo {
		public String id;
		public boolean isPublic;
		public String screenShot;
		public String provider;
		public String documentUrl;
		public String type;
	}
	
	
	public static class AllNoterikInfo {
		public Map<String, NoterikInfo> idxByEuscreenId = new HashMap<>();
		
		public static AllNoterikInfo read() {
			
			AllNoterikInfo result = new AllNoterikInfo();
			
			try( InputStream is = EuscreenPublish.class.getResourceAsStream("noterikVideos.json.gz");
					GzipCompressorInputStream gzin = new GzipCompressorInputStream(is)) {
				
				ObjectMapper om = Jackson.om();
			
				List<NoterikInfo> allNoterik = om.readValue(gzin, new TypeReference<List<NoterikInfo>>(){});
				for( NoterikInfo ni: allNoterik ) 
					result.idxByEuscreenId.put( ni.id, ni );
			} catch( Exception e ) {
				log.error( "Stream couldn't be read for screenshots!", e );
			} 
			return result;
		}
		
		public NoterikInfo get( String eusId ) {
			return idxByEuscreenId.get( eusId );
		}
		
		public boolean containsKey( String eusId ) {
			return idxByEuscreenId.containsKey(eusId);
		}
	}
	
	public static class SeriesInfo {
		public String seriesTitle, provider, series, eusId;
		public boolean isSeries;
		public List<String> episodeIds = new ArrayList<>();
	}
	
	
	/**
	 * This class contains a mix of what is published in mint and what is published on the EUscreen portal.
	 * @author arne
	 *
	 */
	public static class AllSeriesInfo {
		public List<SeriesInfo> seriesInfo = new ArrayList<>();
		public Map<String, SeriesInfo> titleIdx = new HashMap<>();
		public Map<String, SeriesInfo> idIdx = new HashMap<>();
		
		public SeriesInfo byTitle( String title ) {
			return titleIdx.get( title );
		}
		
		public SeriesInfo byEuscreenId( String id ) {
			return idIdx.get( id );
		}
		
		public static AllSeriesInfo read() {			
			AllSeriesInfo result = new AllSeriesInfo();
			final Counter counter = new Counter();
			Set<String> schemas= TextParseUtil.commaDelimitedStringToSet(Config.get("euscreen.portal.schema"));

			List<Long> prs = DB.getPublicationRecordDAO().listIds( Optional.empty() );
			log.debug( "Started processing AllSeriesInfo\n  Processing " + prs.size() + " publications." );
			for( Long prId: prs  ) {
				PublicationRecord pr = DB.getPublicationRecordDAO().getById(prId, false);
				if( schemas.contains( pr.getPublishedDataset().getSchema().getName())) {
					try {
						pr.getPublishedDataset().processAllValidItems(new ApplyI<Item>() {
							public void apply( Item item ) {
								try {
									SeriesInfo si = new SeriesInfo();
									
									si.seriesTitle= item.getValue( "//*[local-name()='TitleSetInEnglish']/*[local-name()='seriesOrCollectionTitle']");
									si.provider = item.getValue("//*[local-name()='provider']" );
									if( normalizeOrg.containsKey( si.provider ))
										si.provider = normalizeOrg.get( si.provider );
									si.series = item.getValue( "//*[local-name()='recordType']");
									si.eusId = item.getValue( "//*[local-name()='identifier']");
									si.isSeries = "SERIES/COLLECTION".equals( si.series );

									result.seriesInfo.add( si );
									counter.inc();
									if( counter.get() % 1000 == 0 ) log.info( "Processed " + counter.get() +" records.");
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
	
			
			return result;
		}
		
		/**
		 * Remove unpublished records from the series idx. Index by title and by euscreen id.
		 * @param allNoterikInfo
		 */

		public static AllSeriesInfo make( AllNoterikInfo allNoterikInfo ) {
			
			AllSeriesInfo allInfo = AllSeriesInfo.read();
			// remove records which are not published
			// build idx fot title and eusId
			Iterator<SeriesInfo> listIter = allInfo.seriesInfo.iterator();
			while( listIter.hasNext() ) {
				SeriesInfo info = listIter.next();
				NoterikInfo ni = allNoterikInfo.get( info.eusId );
				if(( ni == null) || (!ni.isPublic)) listIter.remove();
			}
			
			
			for(SeriesInfo info: allInfo.seriesInfo ) {
				if( info.isSeries ) {
					allInfo.titleIdx.put( info.provider + ":"+ info.seriesTitle, info);
				}
			}

			for(SeriesInfo info: allInfo.seriesInfo ) {
				allInfo.idIdx.put( info.eusId, info );
			}


			// clean the id index
			// all ids that have no series title or no series title with series record go out
			Iterator<Map.Entry<String, SeriesInfo>> iter = allInfo.idIdx.entrySet().iterator();
			while( iter.hasNext() ) {

				Map.Entry<String, SeriesInfo> entry = iter.next();
				SeriesInfo info = entry.getValue();
				
				
				// do nothing with series records
				if( info.isSeries ) continue;
				
				if( StringUtils.empty( info.seriesTitle ) ||
					!allInfo.titleIdx.containsKey( info.provider+":"+info.seriesTitle )) {
					iter.remove();
					continue;
				}
				
				// series epsiodes are added to the series record
				SeriesInfo seriesRecord = allInfo.titleIdx.get( info.provider+":"+info.seriesTitle );
				// should not be empty but check anyway
				if( seriesRecord == null ) {
					log.error( "There really should be a series record for " + info.seriesTitle );
					continue;
				}
				
				seriesRecord.episodeIds.add( entry.getKey());
			}
			
			return allInfo;
		}
		
		
	}
	
	public static class PortalPublish implements Runnable {
		private Dataset ds;
		private boolean publish;
		public PublicationRecord pr;
		
		private int problemCounter = 3;
		
		public static HashMap<String, String> thesaurusLiteralToUrl = new HashMap<String, String>();
		public static HashMap<String, String> thesaurusAmbiguousToUrl = new HashMap<String, String>();
		public static HashSet<String> thesaurusBadLiteral = new HashSet<String>();
		
		public static boolean forceThesaurusOverwrite = true;

		public static List<String> providerNameChanges =
				Arrays.asList(
						// ERT is already correct
				// "HeNAA", "ERT SA",
				// "ERT", "ERT SA",
				// "DW", "Deutsche Welle",
				"NINA", "Narodowy Instytut Audiowizualny",
				"KB", "Kungliga biblioteket",
				"TVC", "TV3 Televisió de Catalunya (TVC)",
				"SASE", "Screen Archive South East",
				"NISV", "Netherlands Institute for Sound and Vision" );
		
		AllNoterikInfo allNoterikInfo;
		
		public PortalPublish( Dataset ds, boolean publish, PublicationRecord pr,  AllNoterikInfo allNoterikInfo ) {
			this.ds = ds;
			this.publish =  publish;
			this.pr = pr;
			this.allNoterikInfo = allNoterikInfo;
		}

		static {
			readThesaurus();
			readThesaurusExtension();
		}
		
		/**
		 * Make sure the new providername is in the item. 
		 *  if there is an id set in here, leave it
		 *  if there is no id, check if old or new id is known to Noterik. If both, flag a problem and set id from new provider
		 *  if one, use that id. If there is none, use the new provider name derived id.
		 * @param item
		 */
		public void adjustProvider( Item item ) {
			String providername = item.getValue("//*[local-name()='AdministrativeMetadata']/*[local-name()='provider']" );
			if( providerNameChanges.contains( providername )) {
				String originalId = item.getValue("//*[local-name()='AdministrativeMetadata']/*[local-name()='originalIdentifier']");

				String currentId = item.getValue("//*[local-name()='AdministrativeMetadata']/*[local-name()='identifier']");
				String oldProviderName, newProviderName;
				
				int i=0;
				for( ;i<=providerNameChanges.size(); i++ ) {
					if( providerNameChanges.get(i).equals(providername)) break;
				}
				if( i%2 == 0 ) {
					oldProviderName = providerNameChanges.get(i);
					newProviderName = providerNameChanges.get(i+1);
				} else {
					oldProviderName = providerNameChanges.get(i-1);
					newProviderName = providerNameChanges.get(i);					
				}
				
				// these are the possible ids that the item can have
				String oldId = "EUS_"+StringUtils.md5Utf8(StringUtils.join(oldProviderName,":",originalId));
				String newId = "EUS_"+StringUtils.md5Utf8(StringUtils.join(newProviderName,":",originalId));
				
				if( currentId == null ) { // should not happen 
					log.info( "There should be an euscreen id element in the xml. Missing in item #" + item.getDbID());
					return;
				}
				
				// adjust providername 
				if( !providername.equals( newProviderName)) {
					item.setValue("//*[local-name()='AdministrativeMetadata']/*[local-name()='provider']", newProviderName );
					log.info( "Adjusted providername to " + newProviderName  + " in " + currentId );
				}
				
				// bail for the euscreen-ids that are not providername / local-id based 
				if( !( currentId.equals( oldId ) || currentId.equals( newId ))) return;
				
				if( allNoterikInfo.containsKey(oldId) && allNoterikInfo.containsKey( newId)) {
					// flag a problem
					log.info( "Item #"+ item.getDbID() + " has EUS id for old and new provider '"+newProviderName+"' at Noterik ( " + oldId + "/" + newId + ")" );
					log.info( "Published state is " + oldId + "=>'" + allNoterikInfo.get(oldId).isPublic +"' " + newId + "=>'" + allNoterikInfo.get( newId ).isPublic + "'");
					log.info( "Dataset #" + item.getDatasetId());
					// choose new id
					item.setValue( "//*[local-name()='AdministrativeMetadata']/*[local-name()='identifier']", newId );
					if(! currentId.equals( newId )) log.info( "Change from " + currentId + " to new provider " + newId );
				} else if( allNoterikInfo.containsKey(oldId)){
					item.setValue( "//*[local-name()='AdministrativeMetadata']/*[local-name()='identifier']", oldId );						
					if(! currentId.equals( oldId )) log.info( "Change from " + currentId + " to old provider " + oldId );
				} else {
					item.setValue( "//*[local-name()='AdministrativeMetadata']/*[local-name()='identifier']", newId );												
					if(! currentId.equals( newId )) log.info( "Change from " + currentId + " to new provider " + newId );
				}
			} 
		}
				
		/**
		 * Read a file with some typical thesaurus entries that dont exist in the thesaurus,
		 * but we provide a literal to URL translation anyway.
		 */
		public static void readThesaurusExtension() {
			try {
				InputStream thesaurus = EuscreenPublish.class.getResourceAsStream("ExtendThesaurus.json");
				JSONObject thes = (JSONObject) JSONValue.parse(thesaurus);
				for( Entry<String,Object> entry: thes.entrySet() ) {
					String url = (String) entry.getValue();
					String literal = entry.getKey();
					thesaurusLiteralToUrl.put( literal, url );
				}
			} catch( Exception e ) {
				log.error( "Couldnt find the thesaurus Extension.", e );
			}
		}
		
		// some statics to deal with thesaurus
		public static void readThesaurus() {
			// read file from disc
			// tmp hash literal to tuple list
			HashMap<String, List<Pair<String, String>>> thesTmp = new HashMap<String, List<Pair<String,String>>>();
			try {
				InputStream thesaurus = EuscreenPublish.class.getResourceAsStream("ThesaurusInJson.json");
				JSONObject thes = (JSONObject) JSONValue.parse(thesaurus);
				// build list of pairs
				for( Entry<String,Object> conceptObj: thes.entrySet() ) {
					String url = conceptObj.getKey();
					JSONObject thesEntry = (JSONObject) conceptObj.getValue();
					for( Entry<String,Object> langLiteral: thesEntry.entrySet() ) {
						String lang = langLiteral.getKey();
						String literal = langLiteral.getValue().toString();
					
						Pair<String, String> p = Pair.of( lang, url);
						List<Pair<String,String>> litList = thesTmp.get( literal );
						if( litList == null ) {
							litList = new ArrayList<Pair<String,String>>();
							thesTmp.put( literal, litList);
						}
						litList.add( p );
					}
				}
				// create literal lookup
				thesaurusLiteralToUrl.clear();
				thesaurusAmbiguousToUrl.clear();
				thesaurusBadLiteral.clear();
				
				for(Entry<String, List<Pair<String, String>>> literal: thesTmp.entrySet()) {
					if( literal.getValue().size() == 1 ) {
						// easy
						thesaurusLiteralToUrl.put( literal.getKey(), literal.getValue().get(0).getRight() );
					} else {
						// only one URL ?
						List<Pair<String, String>> urls = literal.getValue();
						String firstUrl = urls.get(0).getRight();
						boolean differentUrls = false;
						for( Pair<String,String> urlPair: urls ) {
							if( !urlPair.getRight().equals( firstUrl)) differentUrls = true;
						}
						if( !differentUrls ) {
							// all urls the same, easy again
							thesaurusLiteralToUrl.put( literal.getKey(), firstUrl );
						} else {
							// harder, use the english literal
							String enUrl = null;
							for( Pair<String,String> urlPair: urls ) {
								if( urlPair.getLeft().equals( "en" )) {
									if( enUrl == null ) {
										enUrl = urlPair.getRight();
										thesaurusAmbiguousToUrl.put( literal.getKey(), enUrl );
									} else {
										// multiple equal english literals
										log.warn( "Thesaurus ambiguous on literal '" + literal.getKey() + "'\n" );
									}
								}
							}
							if( enUrl == null ) {
								log.warn( literal.getKey() + " is ambiguous and not english.");
								log.debug( literal.getValue());
								thesaurusBadLiteral.add( literal.getKey() );
							}
						}
					}
					
				}
			} catch( Exception e ) {
				log.error( "Thesaurus not read.", e );
			}
			log.info( "Read Thesaurus with " + thesTmp.size() + " literals. " + thesaurusLiteralToUrl.size() + 
					" easy to map. " + thesaurusAmbiguousToUrl.size() + " map ok with english.");
		}
		
		/**
		 * Insert into the item the thestermCode attributes for given thesaurus literal terms.
		 * The item Document and XML is modified.
		 * @param item
		 */
		public static void amendThesaurus( Item item ) {
			// get doc
			// find all thesaurus literals
			Nodes terms = item.getDocument().query( "//*[local-name()='ThesaurusTerm']");
			for( int i=0; i<terms.size(); i++ ) {
				Element thes = (Element) terms.get(i);
				String literal = thes.getValue().trim();
				if( literal.length() == 0 ) {
					Attribute att = thes.getAttribute("thestermCode");
					if(( att == null) || (att.getValue() == null) || (att.getValue().length() == 0 ))
						log.debug( "Empty literal in Item #" + item.getDbID());
					continue;
				}
				String url = thesaurusLiteralToUrl.get( literal );
				if( url == null ) {
					url = thesaurusAmbiguousToUrl.get( literal );
					if( url == null ) {
						log.warn( "Thesaurus entry '" + literal + "' not found. Item #" + item.getDbID());
						if( thesaurusBadLiteral.contains(literal)) {
							log.warn( "Literal is ambiguous in thesaurus!" );							
						}
					}
				}
				Attribute att = thes.getAttribute("thestermCode");
				if( url != null ) {
					log.debug("Correct Thesaurus entry!");
					if( att != null ) {
						String currentUrl = att.getValue();
						if( !currentUrl.equals( url )) {
							log.info( "Conflicting URL entry for Thesaurus entry '" + literal + "' in item #" + item.getDbID());
							if( forceThesaurusOverwrite) {
								att.setValue(url);
							}
						}
					} else {
						Attribute thestermCode = new Attribute( "thestermCode", url);
						thes.addAttribute(thestermCode);
					}
				} else {
					log.debug("Unmatched Thesaurus entry!");
					// URL == null
					if( att != null ) {
						// we better remove this, its probably crap
						log.info( "Removed funny URL '" + att.getValue() + "'");
						thes.removeAttribute(att);
					}
				}
			}
			item.setXml( item.getDocument().toXML());
		}
		
		public void run() {
			try {
				DB.getSession().beginTransaction();

				ds = DB.getDatasetDAO().getById(ds.getDbID(), false);
				pr = DB.getPublicationRecordDAO().getById(pr.getDbID(), false);
				ds.logEvent((publish?"Publish":"Unpublish") + " started" );

				if( publish ) {
					// iterate over items, 
					// if they don't have eus id, make one
					// if they do, compare it and see if its still ok
					// make a note in log if there has been a change
					DB.commit();
					ApplyI<Item> modifyId = new ApplyI<Item>() {
						@Override
						public void apply(Item item) throws Exception {
							String id = item.getValue("//*[local-name()='AdministrativeMetadata']/*[local-name()='identifier']");
							if( id != null ) {
								// create the id.
								String provider = item.getValue("//*[local-name()='AdministrativeMetadata']/*[local-name()='provider']");
								String originalId = item.getValue("//*[local-name()='AdministrativeMetadata']/*[local-name()='originalIdentifier']");
								if( StringUtils.empty(originalId) || StringUtils.empty( provider)) {
									// bad, will be bad EUS id, need to ignore this and warn here
									// set item to invalid and not publish
									item.setValid(false);
									if( problemCounter > 0 ) {
										problemCounter--;
										ds.logEvent("Provider or originalId not set!", "In Item["+item.getDbID()+"] " + item.getLabel());
									}
									
								} else {
									String eusId = "EUS_"+StringUtils.md5Utf8(StringUtils.join( provider,":",originalId));
									if( !id.equals(eusId )) {
										// for the time being only adjust if the value is initial
										if( id.equals( EUSCREEN_DEFAULT_ID)) {
											// modify the xml
											item.setValue("//*[local-name()='AdministrativeMetadata']/*[local-name()='identifier']", eusId );
										} else {
											log.info( "Item [" + item.getDbID() + "] tried change id from " + id + " to " + eusId + ". NOT DONE!"  );
										}
									}
									amendThesaurus(item);
									adjustProvider(item);
								}
								
								id = item.getValue("//*[local-name()='AdministrativeMetadata']/*[local-name()='identifier']");
								item.setPersistentId( id );
							} else {
								// warn the id field is empty
								if( problemCounter > 0 ) {
									problemCounter--;
									ds.logEvent("ID field not preset during publish!", "In Item["+item.getDbID()+"] " + item.getLabel());
								}
							}
						}
					};

					ds.processAllValidItems( modifyId, true );

					Solarizer si = new Solarizer( ds );
					si.runInThread();
					
				}
				
				// and now for the queueing 
				ApplyI<Item> publishOnQueue = new ApplyI<Item>() {
					public void apply(Item item) throws Exception {
						PublishQueue.queueItem(item, !publish);
					}
				};
		
				ds.processAllValidItems( publishOnQueue, false );
				
				ds.logEvent("Finished " + (publish?"publishing to":"unpublishing from") + " portal", ds.getValidItemCount()+" items queued for process at portal.");
				if( publish ) {
					pr.setEndDate(new Date());
					pr.setPublishedItemCount(ds.getValidItemCount());
					pr.setStatus(Dataset.PUBLICATION_OK);
					DB.commit();
				} else {
					DB.getPublicationRecordDAO().makeTransient(pr);
					DB.commit();
				}
			} catch( Exception e ) {
				ds.logEvent("Error while publishing/unpublishing", e.getMessage());
				if( pr != null ) {
					pr.setEndDate(new Date());
					pr.setStatus(Dataset.PUBLICATION_FAILED);
				}
				DB.commit();
			} finally {
				DB.closeSession();
				DB.closeStatelessSession();
			}
		}
	}
	
	// which id should be scheduled for publish
	public static final Logger log = Logger.getLogger( EuscreenPublish.class );
	
	// from the request handler forwarded
	public static void portalPublishGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			ErrorCondition err = new ErrorCondition();
			Dataset ds = RequestHandlerUtil.accessDataset( request, response, "euscreen", err );
			
			Optional<PublicationRecord> oPr = err
					.grab(() -> DB.getPublicationRecordDAO().findByOriginalDataset(ds.getOrigin()).stream()
							.filter(pr -> pr.getTarget().equals( "euscreen.portal" )).findAny(), "DB access failed");

			err.check(() -> !oPr.isPresent(), "This dataset is already published or publishing.");

			err.check(() -> PublicationHelper.isSchemaAndHasValidItems(ds, Config.get( "euscreen.portal.schema")),
					"Dataset cannot be published. No valid items of required schema found.");

			PublicationRecord pr = err.grab( () -> PublicationRecord.createPublication((User) request.getAttribute("user"), ds,
					"euscreen.portal" ), "Error creating PublicationRecord");

			AllNoterikInfo noterik = err.grab( ()->AllNoterikInfo.read(), "Noterik info not found" );

			err.onValid( () -> {
				PortalPublish pp = new PortalPublish( ds, true, pr, noterik );
				Queues.queue(pp, "db" );
			});
			err.onFailed(() -> errJson(err.get(), response));
			
		} catch( Exception e ) {
			log.error("", e);
			errJson("Something went wrong: '" + e.getMessage() + "'", response);
			return;
		}
	}

	public static void portalUnpublishGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			ErrorCondition err = new ErrorCondition();
			Dataset ds = RequestHandlerUtil.accessDataset( request, response, "euscreen", err );
			
			Optional<PublicationRecord> oPr = err
					.grab(() -> DB.getPublicationRecordDAO().findByOriginalDataset(ds.getOrigin()).stream()
							.filter(pr -> pr.getTarget().equals( "euscreen.portal" )).findAny(), "DB access failed");
		
			PublicationRecord pr = err.grab(()-> oPr.orElse( null ), "Dataset not published on portal" );
			
			AllNoterikInfo noterik = err.grab( ()->AllNoterikInfo.read(), "Noterik info not found" );

			err.onValid( () -> {
				PortalPublish pp = new PortalPublish( ds, false, pr, noterik );
				Queues.queue(pp, "db" );
			});
			err.onFailed(() -> errJson(err.get(), response));
			
		} catch( Exception e ) {
			log.error("", e);
			errJson("Something went wrong: '" + e.getMessage() + "'", response);
			return;
		}
	}
	
	// get a value from doc if its unique
	public static Optional<String> getValue( Document doc, String query ) {
		Nodes nodes = doc.query( query );
		if( nodes.size() != 1 ) return Optional.empty();
		String res = nodes.get(0).getValue();
		if( res == null ) return Optional.empty();
		return Optional.of( res );
	}
	

	// put the screenshots from Noterik server into the edm of the portal records.
	// puts doc and picture links in isShownBy
	public static Interceptor<Document, Document> installScreenShotsInterceptor( final AllNoterikInfo allNoterikInfo ) {

		// this is functional style.
		ThrowingBiConsumer<Document, ThrowingConsumer<Document>> modifier = (Document doc, ThrowingConsumer<Document> sink) -> {
				getValue( doc, "//*[local-name()='Aggregation']/@*[local-name()='about']")
				.flatMap( 
					(String id) -> {
						String eusId = id.replaceFirst( ".*(EUS_.{32}).*", "$1");
						// if the record is not found in the list, its not processed further
						return Optional.ofNullable( allNoterikInfo.get( eusId ));
					})
				.ifPresent (
					( NoterikInfo noterikInfo ) -> {
						// this filters out records that are not published at the portal
						// Maybe better to make invalid then to delete
						if( !noterikInfo.isPublic) return;
						
						if( noterikInfo.type.equals("video") || noterikInfo.type.equals("series")) {
							if( noterikInfo.screenShot != null ) {
								Element edmObject = Item.createElement( doc, "object", "http://www.europeana.eu/schemas/edm/", 
									screenshotInsertPos, "//*[local-name()='Aggregation']" );
								Attribute att = new Attribute( "rdf:resource", "http://www.w3.org/1999/02/22-rdf-syntax-ns#", noterikInfo.screenShot );
								edmObject.addAttribute( att );
							}
						} else {
							if( noterikInfo.documentUrl != null ) {
								Element isShownBy = Item.createElement( doc, "isShownBy", "http://www.europeana.eu/schemas/edm/", 
										screenshotInsertPos, "//*[local-name()='Aggregation']" );
								Attribute att = new Attribute( "rdf:resource", "http://www.w3.org/1999/02/22-rdf-syntax-ns#", noterikInfo.documentUrl );
								isShownBy.addAttribute( att );						
							}
						}
						try {
							sink.accept(doc);
						} catch( Exception e ) {
							log.error( "" , e);
						}
					} );
		};
		
		return Interceptor.mapcatInterceptor(modifier);
	}
	
	

	// add the series links to the episodes and the episode links to the series records
	public static Interceptor<Item, Item> installSeriesLinksInterceptor( AllSeriesInfo allSeriesInfo ) throws Exception {
				 
		 ThrowingConsumer<Item> modifier = (Item item) -> {
			 
			String id = item.getValue( "//*[local-name()='Aggregation']/@*[local-name()='about']");
			String cho = item.getValue( "//*[local-name()='ProvidedCHO']/@*[local-name()='about'] " );
			String eusId = id.replaceFirst( ".*(EUS_.{32}).*", "$1");

			SeriesInfo si = allSeriesInfo.byEuscreenId(eusId);
			if( si != null ) {
				if( si.isSeries ) {
					// series record entries
					for( String episodeId: si.episodeIds ) {
						Element newElem = item.insertElement( "hasPart","http://purl.org/dc/terms/", linkInsertPos, "//*[local-name()='ProvidedCHO']" );
						Attribute att = new Attribute( "rdf:resource", "http://www.w3.org/1999/02/22-rdf-syntax-ns#", cho.replace(eusId, episodeId));
						newElem.addAttribute(att);
						log.info( "Series link installed in " + eusId + " Item: " + item.getDbID());
					}
				} else {
					SeriesInfo seriesRecord = allSeriesInfo.byTitle(  si.provider+":"+si.seriesTitle );
					String seriesId = seriesRecord.eusId;

					Element newElem = item.insertElement( "isPartOf","http://purl.org/dc/terms/", linkInsertPos, "//*[local-name()='ProvidedCHO']" );
					Attribute att = new Attribute( "rdf:resource", "http://www.w3.org/1999/02/22-rdf-syntax-ns#", cho.replace(eusId, seriesId));
					newElem.addAttribute(att);
				}
				item.setXml(item.getDocument().toXML());
			}
		};
			
		return Interceptor.modifyInterceptor( modifier );
	}

	
	/**
	 * create doc interceptor that fixes the dc:source
	 * @param edmSet
	 * @return
	 */
	public static Interceptor<Document, Document> fixDcSource( Dataset edmSet )  {  
		if( edmSet.getOrganization().getDbID() == 1025l ) 
			return Interceptor.modifyInterceptor( (Document doc) -> {
				Item.setDocValue( doc, "//*[local-name()='source' and starts-with( ., 'EUscreenXL')]", "EUscreen Project 2009-2012" );
			} );
		else
			return Interceptor.emptyInterceptor();
	}
	
	/**
	 * The mapping needs the parent dataset to generate the right outer loop and import the correct namespaces.
	 */
	public static Interceptor<Document, Document> interceptorFromMapping( Mapping m, Dataset parentDataset ) {
		try {
			XSLTGenerator xslt = new XSLTGenerator();
		
			xslt.setItemXPath( parentDataset.getItemRootXpath().getXpathWithPrefix(true));
			xslt.setImportNamespaces( parentDataset.getRootHolder().getNamespaces(true));
			xslt.setOption(XSLTGenerator.OPTION_OMIT_XML_DECLARATION, true);
			
			Mappings mappings = m.getMappings();
			String xsl = XMLFormatter.format(xslt.generateFromMappings(mappings));
			XSLTransform transform = new XSLTransform();
			transform.setXSL(xsl);
			// a builder 
			XMLReader parser = org.xml.sax.helpers.XMLReaderFactory.createXMLReader(); 
			parser.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

			Builder builder = new Builder(parser);

			return Interceptor.mapInterceptor( (Document doc) -> {
				String outputXml = transform.transform(doc.toXML());
				return builder.build( outputXml, null );
			});
		} catch( Exception e ) {
			log.error( "Cannot build mapping based transforming interceptor.", e );
		}

		return null;

	}

	public static Interceptor<Item, Item> interceptorFromXsl( String xsl ) {
		try {
			XSLTransform transform = new XSLTransform();
			transform.setXSL(xsl);
			// a builder 

			return Interceptor.modifyInterceptor( (Item item) -> {
				String outputXml = transform.transform(item.getXml());
				item.setXml(outputXml);
			});
		} catch( Exception e ) {
			log.error( "Cannot build xsl based transforming interceptor.", e );
		}

		return null;

	}

	public static Interceptor<Item, Item> createSkosResolveInterceptor() throws Exception  {
		String xsl = FileUtils.readFileToString(Config.getProjectFile( "xsl/edm_enrich_thesaurus.xsl" ), "UTF-8");
		return interceptorFromXsl(xsl);
	}
	
	public static Transformation prepareTransformation( Dataset ds, XmlSchema xmlSchema ) {
		Transformation transformation = new Transformation();
		transformation.init(ds.getCreator());
		transformation.setName("EUscreen Core2Edm prep");
		transformation.setParentDataset(ds);
		transformation.setCreated(new Date());
		transformation.setOrganization(ds.getOrganization());
		transformation.setSchema(xmlSchema);
		
		return transformation;
	}
	
	// use this method via script console to do mass edm generation for 
	// core publication. Submits the Transformation to db queue
	public static void corePrepareEdm( Dataset ds, Optional<Transformation> runningTransformation, AllNoterikInfo allNoterikInfo, AllSeriesInfo allSeriesInfo, Mapping coreToEdmBaseMapping ) {
		try {
			// map to edm
			Interceptor<Document, Document> docInterceptor = interceptorFromMapping(coreToEdmBaseMapping, ds)
					// fix dc
					.into( fixDcSource(ds))
					// install screenshot
					.into( installScreenShotsInterceptor(allNoterikInfo));
			
			// the item doc wrap interceptor makes new items from incoming items, then applies the doc interceptor
			Interceptor<Item, Item> itemInterceptor = new Interceptors.ItemDocWrapInterceptor( docInterceptor, true )
					// install series links
					.into(installSeriesLinksInterceptor(allSeriesInfo))
					// add skos:Concept sections for euscreen thesaurus refs
					.into( createSkosResolveInterceptor());
			
			Transformation tr = runningTransformation
					.orElseGet( ()->prepareTransformation(ds, coreToEdmBaseMapping.getTargetSchema()));
			
			DB.getTransformationDAO().makePersistent(tr);
			DB.commit();

			GroovyTransform gt = new GroovyTransform(tr, itemInterceptor );

			Queues.queue( gt, "db");		

		} catch( Exception e ) {
			log.error( "Failed to prep EDM", e );
		}
	}
	
	public static AllSeriesInfo getOrMakeSeriesInfo( AllNoterikInfo noterik ) {
		if( cache == null ) {
			cache = new CachedObject<AllSeriesInfo>( Ticker.getTimer(), ()->AllSeriesInfo.make(noterik), 3600 );
		}
		return cache.get();
	}
	
	public static void corePrepareEdmGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			ErrorCondition err = new ErrorCondition();
			// probably should respect locks
			Dataset ds = RequestHandlerUtil.accessDataset( request, response, "euscreen", err );
			// what mapping to use for core to edm
			String mappingId = err.grab(  
				() -> RequestHandlerUtil.getUniqueParameter( request, "mappingId").orElse( Config.get("euscreen.core2edm.mapping"))
				, "No mappingId parameter found in request or config");
			
			// TODO: probably should respect locks
			Mapping m = err.grab( ()-> 
				DB.getMappingDAO().getById( Long.parseLong(mappingId), false)
				, "Mapping not in db or invalid");
			
			
			Transformation tr = err.grab( ()-> {
				Transformation t2 = prepareTransformation(ds, m.getTargetSchema());
				t2.setTransformStatus(Transformation.TRANSFORM_RUNNING);
				t2 = DB.getTransformationDAO().makePersistent(t2);
				return t2;
			}, "Starting Transformation failed" );
				
			// set the dataset locked by creating a running Transformation
			
			err.onValid( () -> {
				Queues.queue(()->{					
					try {
						DB.getSession().beginTransaction();
						AllNoterikInfo noterik = AllNoterikInfo.read();
						if( noterik == null ) {
							log.warn( "failed to read Noterik info" );
							return;
						}
						log.info( "Noterik info processed");
						AllSeriesInfo series = getOrMakeSeriesInfo(noterik);
						if( series == null ) {
							log.warn("Series info not created" );
							return;
						}
						log.info( "All portal records read for series info.");
						// put ds and m into hibernate session
						Dataset localDs = DB.getDatasetDAO().getById(ds.getDbID(), false);
						Mapping localMapping = DB.getMappingDAO().getById(m.getDbID(), false);
						
						corePrepareEdm(localDs, Optional.of( tr ), noterik, series, localMapping);
					} finally {
						try {
							DB.closeSession();
							DB.closeStatelessSession();
						} catch( Exception e ) {
							log.error( "", e );
						}
					}
				}, "db");
				okJson( "msg","Transformation to EDM queued!", response );
			});
			err.onFailed(() -> errJson(err.get(), response));

		} catch( Exception e ) {
			log.error("", e);
			errJson("Something went wrong: '" + e.getMessage() + "'", response);
			return;
		}
	}

	// return empty Optional if everything was ok or Some( "errormsg" )
	public static Optional<String> europeanaPublishApi( Dataset ds, User publishingUser ) {
		
		try {
			Optional<PublicationRecord> oPr = DB.getPublicationRecordDAO()
					.findByOriginalDataset(ds.getOrigin())
					.stream()
					.filter(pr -> pr.getTarget().equals( "euscreen.oai" ))
					.findAny();
			if( oPr.isPresent()) return Optional.of( "This dataset is already published or publishing.");
			if( ! PublicationHelper.isSchemaAndHasValidItems(ds, Config.get( "euscreen.oai.schema")))
				return Optional.of( "Dataset cannot be published. No valid items of required schema found.");
	
			String routingKey = EuscreenPublish.isParentPortalPublished(ds) ?
					Config.get( "euscreen.queue.routingKeyCore") :
					Config.get( "euscreen.queue.routingKey");
	
			if( StringUtils.empty(routingKey)) return Optional.of( "Bad routing key setup");
			
			PublicationRecord pr = PublicationRecord.createPublication(publishingUser, ds,
					"euscreen.oai" );
	
			Runnable r = () -> {
				try {
					DB.getSession().beginTransaction();
					Dataset myDs = DB.getDatasetDAO().getById(ds.getDbID(), false);
					User pubUser = DB.getUserDAO().getById(publishingUser.getDbID(), false);
	
					PublicationHelper.oaiPublish(myDs, "euscreen.oai", pubUser, Config.get("euscreen.queue.exchange"),
							routingKey, Config.get("euscreen.oai.server"),
							Integer.parseInt(Config.get("euscreen.oai.port"))
							// optional ItemMessage messing
					, null );
	
					DB.commit();
				} catch (Exception e) {
					RequestHandler.log.error("Problems during publish", e);
				} finally {
					DB.closeSession();
					DB.closeStatelessSession();
				}
			};
	
			Queues.queue(r, "db");
			ds.logEvent("Publication to euscreen OAI queued.");
	
			// all ok, no error msg returned
			return Optional.empty();
		} catch( Exception e ) {
			RequestHandler.log.error( "", e );
			return Optional.of( "Exception: " + e.getMessage() );
		}
	}

	public static boolean isParentPortalPublished( Dataset dataset ) {
		for( Dataset ds: dataset.getParentDatasets()) {
			PublicationRecord pr = ds.getPublicationRecord();
			if( pr != null ) {
				if( pr.getTarget().equals( "euscreen.portal")) return true;
			}
		}
		return false;
	}

	// just the logic to be used from external
	public static Optional<String> europeanaUnpublishApi( Dataset ds ) {
		try {
			String routingKey = isParentPortalPublished(ds) ?
					Config.get( "euscreen.queue.routingKeyCore") :
					Config.get( "euscreen.queue.routingKey");
			if( StringUtils.empty(routingKey)) return Optional.of( "Bad routing key setup");
	
			PublicationHelper.oaiUnpublish(ds, routingKey,
					Config.get("euscreen.oai.server"), Integer.parseInt(Config.get("euscreen.oai.port")));
			
			Optional<PublicationRecord> oPr = DB.getPublicationRecordDAO().findByOriginalDataset(ds.getOrigin())
					.stream().filter(pr -> pr.getTarget().equals("euscreen.oai")).findAny();
			if (oPr.isPresent())
				DB.getPublicationRecordDAO().makeTransient(oPr.get());
			return Optional.empty();
		} catch( Exception e ) {
			RequestHandler.log.error( "", e );
			return Optional.of( "Exception: " + e.getMessage());
		}
	}
}

