package gr.ntua.ivml.mint.projects.fashionxx;

import static gr.ntua.ivml.mint.api.RequestHandler.errJson;
import static gr.ntua.ivml.mint.api.RequestHandler.getPathInt;
import static gr.ntua.ivml.mint.api.RequestHandler.okJson;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;

import gr.ntua.ivml.mint.concurrent.Queues;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.f.ErrorCondition;
import gr.ntua.ivml.mint.f.Interceptor;
import gr.ntua.ivml.mint.f.ThrowingConsumer;
import gr.ntua.ivml.mint.f.ThrowingSupplier;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Project;
import gr.ntua.ivml.mint.persistent.PublicationRecord;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.pi.messages.ItemMessage;
import gr.ntua.ivml.mint.projects.fashion.ManualEnrich;
import gr.ntua.ivml.mint.util.Config;
import gr.ntua.ivml.mint.util.Jackson;
import gr.ntua.ivml.mint.util.PublicationHelper;
import nu.xom.Attribute;
import nu.xom.Builder;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Nodes;

/**
 * Static messages routed here by the router servlet, starting with /api/fashionxx/...
 * @author stabenau
 *
 */
public class RequestHandler {
	
	public static final Logger log=Logger.getLogger(RequestHandler.class);
	
	public static class Option {
		public String project = "fashionxx";
		public String category = "publication";
		public String label;
		public boolean inProgress = false;
		public String url;
		
		// json|htmlNewPanel|htmlReplacePanel
		public String response = "json";
	};

	// get the dataset and check if the user is allowed to do anything 
	// and if the dataset belongs to fashion.
	// if dataset is null, response already contains errcode and mesg
	private static Dataset accessAllowed( HttpServletRequest request, HttpServletResponse response, ErrorCondition err ) throws Exception {
		
		// err does nothing once it has an error condition. grabs return null
		Integer datasetId = err.grab(()->getPathInt( request, 3 ), "Invalid dataset id."); 
		Dataset ds = err.grab( ()->DB.getDatasetDAO().getById((long) datasetId, false ), "Unknown Dataset" ); 
		User u = err.grab(()-> (User) request.getAttribute("user"), "No User logged in");

		Project p = err.grab(()-> DB.getProjectDAO().findByName("fashionxx"), "'fashionxx' project not present in db" );
		err.check(() -> p.hasProject(ds.getOrigin()), "Dataset not in fashionxx" );
		
		err.check( ()-> u.can( "change data", ds.getOrganization() ) || p.hasProject(u), "User has no access");

		return err.grab(()->ds,"");
	}
	
	// TODO: These should be post requests, to be done much later
	
	// this one should be quick enough to not go to Queue
	public static void oaiUnpublishGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			ErrorCondition err = new ErrorCondition();
			Dataset ds = accessAllowed( request, response, err );

			
			// remove the Publication record, we dont care if its successful or not
			// In theory we should check if its running though
			err.onValid(() -> {
				Optional<PublicationRecord> oPr = DB.getPublicationRecordDAO().findByOriginalDataset( ds.getOrigin()) 
						.stream()
						.filter( pr -> pr.getTarget().equals( "fashionxx"))
						.findAny();
				if( oPr.isPresent() ) {
					PublicationHelper.oaiUnpublish( ds
							, Config.get( "fashion.queue.routingKey")
							, Config.get( "fashion.oai.server" )
							, Integer.parseInt( Config.get( "fashion.oai.port" ))
							);
					DB.getPublicationRecordDAO().makeTransient(oPr.get());
					okJson( "msg", "OAI unpublish succeeded.", response );
				} else {
					err.set( "Dataset not published in fashionxx" );
				}
			} );
			
			err.onFailed(()-> errJson( err.get(), response ));
		} catch( Exception e ) {
			log.error( "", e );
			errJson( "Something went wrong: '" + e.getMessage() + "'", response );
			return ;
		}
	}
	
	
	private boolean canPrepareForPublish( Dataset ds ) {
		// needs to be in project
		// needs to have enrichments (??) in organization
		// if in EDM-FP or EDM
		return true;
	}
	
	/*
	 * 
	 */
	public static void prepareForPublish( HttpServletRequest request, HttpServletResponse response) throws IOException {
		
		ThrowingSupplier<String> exec = () -> {
			
			return null;
		};
		
	}
	
	/*
	 * for sure remove if element name is the same (clearly duplicate)
	 * or remove edm:isRelatedTo if there is something else
	 * otherwise leave it
	 */
	public static void removeDuplicateRdfResource( Document doc ) {
		
		// elements with rdf:resource
		Nodes nodes = doc.query("//*[local-name()='ProvidedCHO']/*[@*[local-name()='resource']]");
		Map<String, List<Element>> values = new HashMap<>();
		for (int i = 0; i < nodes.size(); i++) {
			Element elem = (Element) nodes.get(i);
			String resourceValue = elem.getAttributeValue("resource", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");

			List<Element> vec = values.get(resourceValue);
			if (vec == null) {
				vec = new ArrayList<Element>();
				values.put(resourceValue, vec);
			}
			vec.add(elem);
		}

		// now deal with the duplicates
		for (String k : values.keySet()) {
			List<Element> vec = values.get(k);
			// not duplicated
			if (vec.size() < 2)
				continue;

			// group by element name and see if we can remove some there
			Map<String, List<Element>> byElementName = new HashMap<>();
			
			Iterator<Element> it = vec.iterator();
			while (it.hasNext()) {
				Element e = it.next();

				String elementName = e.getLocalName();
				
				List<Element> vec2 = byElementName.get(elementName);
				if (vec2 == null) {
					vec2 = new ArrayList<Element>();
					byElementName.put(elementName, vec2);
				}
				vec2.add(e);
			}
			
			boolean specificAnnotationExists = false;
			for( String elementName: byElementName.keySet()) {
				if( elementName.equals("isRelatedTo" )) continue;
				specificAnnotationExists = true;
				
				// if we have the same element we should keep the computer annotation?
				// If that sounds odd, you are not alone :-(
				List<Element> vec2 = byElementName.get(elementName);
				if( vec2.size() < 2) continue;

				vec2.sort((Element e1, Element e2) -> {
					// if we have a wasGeneratedBy put it at the end of the list
					int e1b = (e1.getAttribute("wasGeneratedBy", "http://www.europeana.eu/schemas/edm/" ) == null)?0:1;
					int e2b = (e2.getAttribute("wasGeneratedBy", "http://www.europeana.eu/schemas/edm/" ) == null)?0:1;
					return Integer.compare(e1b, e2b);
				} );
				it = vec2.iterator();
				// we need to leave at least one
				int limit = vec2.size();
				while (it.hasNext()) {
					Element e = it.next();
					if( limit > 1 ) {
						e.getParent().removeChild(e);
						log.info( "Removed duplicated " + elementName + "'" + k + "'");
						limit--;
					}
				}
			}
			// remove isRelatedTo if the same URL is elsewhere as well, surely thats redundant
			List<Element> vec2 = byElementName.get( "isRelatedTo" );
			// nothing to do here
			if( vec2 == null ) continue;
			
			if( !specificAnnotationExists ) {
				// need to keep one
				vec2.remove(0);
			} // else all can go
			for( Element e: vec2 ) {
				e.getParent().removeChild( e );
				log.info( "Removed duplicated edm:isRelatedTo '" + k + "'");
			}			
		} // end of loop over equal rdf:resources
	}
	
	private static void insertDcTerms( Document doc ) {
		
		Element elem = new Element("dcterms:isPartOf", "http://purl.org/dc/terms/");
		elem.appendChild( "Europeana XX: Century of Change" );

		Attribute att = new Attribute("xml:lang", "http://www.w3.org/XML/1998/namespace","en");
		elem.addAttribute( att );

		Nodes nodes = doc.query( "//*[local-name()='ProvidedCHO']" );
		if( nodes.size() > 0 ) {
			Element parent = (Element) nodes.get( 0 );
			parent.insertChild(elem, 0 );
		}
	}
	
	private static Interceptor<ItemMessage, ItemMessage> modifyItem( Dataset ds ) {
		// Build the dom
		// add the dcterms entry if there are any SoftwareAgent annotations
		// remove duplicate rdf:resources
		// if the dataset is uploaded for xx add the CoC regardless
		
		final Builder xmlDocBuilder = new Builder();
		
		
		ThrowingConsumer<Document> modifyConsumer = 
			doc -> {
				ManualEnrich.manualEnrich(doc, true );
				// then remove duplicated rdf resources
				removeDuplicateRdfResource(doc);

				Nodes annotated = doc.query( "//@*[local-name()='wasGeneratedBy' and .='SoftwareAgent']");
				if(( annotated.size() != 0 )) { 
					// do this only if we have annotations ..
					insertDcTerms(doc);					
				}
			};

		return Interceptor.mapInterceptor( im -> {
			Document doc = xmlDocBuilder.build( im.xml, null );
			modifyConsumer.accept(doc);
			im.xml = doc.toXML();
			return im;
		});
	}
	
	
	// maybe the same as unpublish
	public static void oaiCleanGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		oaiUnpublishGet(request, response);
	}
	
	
	public static void oaiPublishGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			ErrorCondition err = new ErrorCondition();
			Dataset ds = accessAllowed( request, response, err );
			
			Optional<PublicationRecord> oPr = err.grab( ()-> DB.getPublicationRecordDAO().findByOriginalDataset( ds.getOrigin()) 
					.stream()
					.filter( pr -> pr.getTarget().equals( "fashionxx"))
					.findAny(), "DB access failed");
			
			err.check( ()-> !oPr.isPresent(), "This dataset is already published or publishing.");

			err.check(()-> PublicationHelper.isSchemaAndHasValidItems(ds, Config.get( "fashionxx.oai.schema")),
					"Dataset cannot be published. No valid items of required schema found.");
			final User publisher = err.grab(()-> (User) request.getAttribute("user"), "No User logged in");

			err.onValid( ()-> {
				// Which dataset to publish
				// No magic on this one. Publish this if it is allowed, magic somewhere else (which dataset to publish)
				
				// Publish takes some time, so we need to queue it
				// but we should mark it as started
				PublicationRecord pr = new PublicationRecord();
	
				pr.setStartDate(new Date());
				pr.setPublisher((User)request.getAttribute( "user" ));
				pr.setOriginalDataset(ds.getOrigin());
				pr.setPublishedDataset(ds );
				pr.setStatus(Dataset.PUBLICATION_RUNNING);
				pr.setOrganization(ds.getOrganization());
				pr.setTarget("fashionxx");
				DB.getPublicationRecordDAO().makePersistent(pr);
				DB.commit();
	
				Runnable r = () -> {
					try {
						DB.getSession().beginTransaction();
						Dataset myDs = DB.getDatasetDAO().getById(ds.getDbID(), false);
						User pubUser = DB.getUserDAO().getById(publisher.getDbID(), false);
						
						PublicationHelper.oaiPublish(myDs, "fashionxx", pubUser
							, Config.get( "fashion.queue.exchange")
							, Config.get( "fashion.queue.routingKey")
							, Config.get( "fashion.oai.server" )
							, Integer.parseInt( Config.get( "fashion.oai.port" ))
							, modifyItem( myDs.getOrigin()) /* interceptor to add europeanaXX century ... and remove duplicate rdf:resources */
						);
						
						DB.commit();
					} catch( Exception e ) {
						log.error( "Problems during publish", e );
					} finally {
						DB.closeSession();
						DB.closeStatelessSession();
					}
				};
				
				Queues.queue( r, "db");
				ds.logEvent( "Publication to fashion OAI queued.");
				okJson( "msg", "Publication to OAI queued.", response );
			} );

			err.onFailed(()-> errJson( err.get(), response ));

		} catch( Exception e ) {
			log.error( "", e );
			errJson( "Something went wrong: '" + e.getMessage() + "'", response );
			return ;
		}
	}
	
	public static ArrayNode datasetPublishOptions( Dataset ds, User user ) {
		ArrayNode result = Jackson.om().createArrayNode();
		
		List<String> projects = Arrays.asList( ds.getOrigin().getProjectNames());

		// maybe nothing to do here, although we only get here if there is?
		if( !projects.contains("fashionxx")) return result;
		
		
		List<PublicationRecord> pubs = DB.getPublicationRecordDAO().findByOriginalDataset( ds.getOrigin());
		Optional<PublicationRecord> relevantPub = pubs.stream().filter( pr -> pr.getTarget().equals( "fashionxx")).findAny();
		Option option = new Option();
	
		if( relevantPub.isPresent()) {
			// show is running, needs cleaning or unpublish
			String status = relevantPub.get().getStatus();
			switch( status ) {
			case PublicationRecord.PUBLICATION_OK:
				option.label = "Unpublish from FashionXX OAI";
				option.url = "api/fashionxx/oaiUnpublish/"+relevantPub.get().getPublishedDataset().getDbID();
				break;
			case PublicationRecord.PUBLICATION_RUNNING:
				option.label = "FashionXX OAI publication running";
				option.inProgress = true;
				option.url = null;
				break;
			case PublicationRecord.PUBLICATION_FAILED:
				option.label = "Clean failed FashionXX OAI publication";
				option.url = "api/fashionxx/oaiClean/"+relevantPub.get().getPublishedDataset().getDbID();
				break;
			}
			result.add( Jackson.om().valueToTree(option));
		}
		return result;
	}
}
