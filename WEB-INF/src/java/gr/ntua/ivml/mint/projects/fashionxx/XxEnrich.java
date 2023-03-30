package gr.ntua.ivml.mint.projects.fashionxx;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.log4j.Logger;

import gr.ntua.ivml.mint.concurrent.GroovyTransform;
import gr.ntua.ivml.mint.concurrent.Queues;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.db.DB.SessionRunnable;
import gr.ntua.ivml.mint.f.Interceptor;
import gr.ntua.ivml.mint.f.ThrowingConsumer;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Enrichment;
import gr.ntua.ivml.mint.persistent.Enrichment.EnrichBuilderTrie;
import gr.ntua.ivml.mint.persistent.Item;
import gr.ntua.ivml.mint.persistent.Organization;
import gr.ntua.ivml.mint.persistent.Transformation;
import gr.ntua.ivml.mint.persistent.XmlSchema;
import gr.ntua.ivml.mint.projects.fashion.ManualEnrich;
import gr.ntua.ivml.mint.util.EdmEnrichBuilder;
import gr.ntua.ivml.mint.util.Interceptors;
import nu.xom.Document;
import nu.xom.Nodes;

public class XxEnrich {
	/**
	 * Create the modify(doc) method that enriches for xx
	 * Assumes there are only certain named enrichments in the org
	 * 
	 */
	public static final Logger log = Logger.getLogger( XxEnrich.class );
		
	static interface Enricher {
		// in theory the key can be extracted from the doc, but we have many enrichers running, why bother many times.
		public void enrich( String key, Document doc );
	}
	
	public static List<Enricher> createEnrichers( Organization org ) {
		List<Enrichment> allOrgEnrich = DB.getEnrichmentDAO().simpleList( "organization="+org.getDbID());
		String[] enricherTypes = { "creator", "subject", "title", "description", "isrelatedto", "contributor", "spatial", "type" };
		
		List<Enricher> enrichers = new ArrayList<>();
		
		for( Enrichment enrich: allOrgEnrich) {
			String name = enrich.getName();
			for( String enricherType: enricherTypes) {
				if( name.toLowerCase().contains( enricherType) && 
						!name.toLowerCase().contains("values")) {
					final String finalType = enricherType;
					
					Enricher e = new Enricher() {
						EnrichBuilderTrie idx = new EnrichBuilderTrie( enrich, 0 );
						public void enrich( String key, Document doc ) {
							switch( finalType) {
							case "creator": idx.enrich(key, (String[] row) -> EdmEnrichBuilder.enrichCreator( doc, row[1], row[3] ));
								break;
							case "subject": idx.enrich(key, (String[] row) -> EdmEnrichBuilder.enrichSubject( doc, row[1], row[3] ));
								break;
							case "title": idx.enrich(key, (String[] row) -> EdmEnrichBuilder.enrichTitle(  doc, "en", row[1], row[2] ));
								break;
							case "description": idx.enrich(key, (String[] row) -> EdmEnrichBuilder.enrichDescription( doc, "en", row[1], row[2] ));
								break;
							case "isrelatedto": idx.enrich(key, (String[] row) -> EdmEnrichBuilder.enrichRelatedTo( doc, row[1], row[3] ));
								break;
							case "contributor": idx.enrich(key, (String[] row) -> EdmEnrichBuilder.enrichContributor( doc, row[1], row[3] ));
								break;
							case "spatial": idx.enrich(key, (String[] row) -> EdmEnrichBuilder.enrichSpatial( doc, row[1], row[3] ));
								break;
							case "type": idx.enrich(key, (String[] row) -> EdmEnrichBuilder.enrichDcType( doc, row[1], row[3] ));
								break;
							}
						}
					};
					enrichers.add( e );
				}
			}	
		}
		
		return enrichers;
	}
	
	public static Interceptor<Document,Document> getInterceptor( Organization org ) {
		final List<Enricher> enrichers = createEnrichers( org );
		
		ThrowingConsumer<Document> mod = doc -> {
			Nodes nodes = doc.query( "//*[local-name()='ProvidedCHO']/@*[local-name()='about']");
			if( nodes.size() != 1 ) return;
			String key = nodes.get(0).getValue();
			key = key.replaceAll( "[^0-9a-zA-Z]", "_");
			for( Enricher e: enrichers  ) {
				e.enrich( key, doc );
			}
			EdmEnrichBuilder.removeDuplicateRdfResource( doc );
			EdmEnrichBuilder.tagXX( doc, true );
		};
		
		return Interceptor.modifyInterceptor(mod);
	}
	
	
	// enrich with all the enirchments in the org
	// do the manual enrich from fashion
	// TODO: This function is useful without the manual enrich 
	public static void xxEnricAsync( Dataset ds ) {
		Transformation tr= Transformation.fromDataset(ds);
		tr.setName(  "Apply enrichments, tag with XX" );
		tr.setTransformStatus( Transformation.TRANSFORM_RUNNING );

		List<XmlSchema> lxs = XmlSchema.getSchemasFromConfig("fashionxx.oai.schema");
		// should be only one
		XmlSchema annotatedTargetSchema = lxs.get(0);
		tr.setSchema(annotatedTargetSchema);
		
		DB.getTransformationDAO().makePersistent(tr);
		DB.commit();
		
		SessionRunnable r = () -> {
			try {
				Dataset localDs = DB.getDatasetDAO().getById(ds.getDbID(), false);
				
				Interceptor<Document,Document> enrichInterceptor = getInterceptor(localDs.getOrganization());
				Interceptor<Document, Document> manualEnrichInterceptor = Interceptor.modifyInterceptor( 
						doc -> ManualEnrich.manualEnrich(doc, true)
					);
				Interceptor<Item, Item> interceptor = new Interceptors.ItemDocWrapInterceptor( 
						manualEnrichInterceptor.into( enrichInterceptor ), true );
				
				GroovyTransform gt = new GroovyTransform(tr, interceptor );
				
				
				gt.runInThread();
			}catch( Exception e ) {
				log.error( "", e );
				ds.logEvent(e.getMessage());
			}
		};
		
		Queues.queue(r, "db");
	}
}
