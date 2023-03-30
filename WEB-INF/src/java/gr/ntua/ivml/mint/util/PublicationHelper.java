package gr.ntua.ivml.mint.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.opensymphony.xwork2.util.TextParseUtil;

import gr.ntua.ivml.mint.OAIServiceClient;
import gr.ntua.ivml.mint.Publication;
import gr.ntua.ivml.mint.RecordMessageProducer;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.f.Interceptor;
import gr.ntua.ivml.mint.f.ThrowingConsumer;
import gr.ntua.ivml.mint.persistent.Crosswalk;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Item;
import gr.ntua.ivml.mint.persistent.PublicationRecord;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.persistent.XmlSchema;
import gr.ntua.ivml.mint.persistent.XpathHolder;
import gr.ntua.ivml.mint.pi.messages.ExtendedParameter;
import gr.ntua.ivml.mint.pi.messages.ItemMessage;
import gr.ntua.ivml.mint.pi.messages.Namespace;

/**
 * Lots of helpers for publications, independent of project.
 * 
 * 
 * @author stabenau
 *
 */
public class PublicationHelper {
	
	public static Logger log = Logger.getLogger(Publication.class);

	/**
	 * 1: Create the publisher.
	 * 2: setDataset
	 * 3: send Items
	 * 4: goto 2 or
	 * 5: 
	 * @author arne
	 *
	 */
	public static class OaiItemPublisher {
		
		Dataset currentDataset;
		Dataset originalDataset;

		int originalDatasetId;
		
		Namespace ns;
		int schemaId;
		int orgId;
		
		RecordMessageProducer rmp;
		Set<String> routingKeySet;
		OAIServiceClient osc;
		
		ArrayList<ExtendedParameter> extendedParameterList;
		String reportId;
		// public ThrowingConsumer<Item> sendItem;
		Counter itemCounter = new Counter(0);
		Counter edmCounter = new Counter(0);
		
		public OaiItemPublisher( String exchange, String routingKeys, String oaiHost, int oaiPort ) throws Exception {
			this.rmp = new RecordMessageProducer(Config.get("queue.host"),exchange );
			routingKeySet =  TextParseUtil.commaDelimitedStringToSet( routingKeys );
			
			osc = new OAIServiceClient(oaiHost, oaiPort);
		}
		
		public void setDataset( Dataset ds, String schemaPrefix, String schemaUri ) {
			this.ns = new Namespace();
			this.ns.setPrefix(schemaPrefix);
			this.ns.setUri(schemaUri);
			
			originalDataset = ds.getOrigin();
			currentDataset = ds;
			if( ds.getSchema() != null ) 
				this.schemaId = ds.getSchema().getDbID().intValue();

			// first Dataset creates report
			if( extendedParameterList == null ) {
				orgId = (int) ds.getOrganization().getDbID();
				originalDatasetId = originalDataset.getDbID().intValue();

				ArrayList<Integer> datasetIds = new ArrayList<Integer>();
				datasetIds.add( originalDatasetId );
				
				String projectName = "";
				for( String s: routingKeySet ) {
					if( s.contains("oai")) projectName= s;
				}
				
				reportId = osc.createReport(projectName, originalDataset.getCreator().getDbID().intValue(), 
						(int) originalDataset.getOrganization().getDbID(), 
						datasetIds );
				
				originalDataset.logEvent( "OAI report id is " + reportId);
				extendedParameterList = new ArrayList<ExtendedParameter>();
				ExtendedParameter ep = new ExtendedParameter();
				ep.setParameterName("reportId" );
				ep.setParameterValue(reportId);
				extendedParameterList.add( ep );
			}
			originalDataset.logEvent( "Sending #" +ds.getDbID() + " Prefix: " + schemaPrefix + " to OAI");
			ds.logEvent( "Publishing with prefix: " + schemaPrefix + " to OAI");
		}
		
		public void sendItem( Item item ) throws Exception {
			try {
				ItemMessage im = new ItemMessage();
				if( item.getSourceItem() != null )
					im.setSourceItem_id(item.getSourceItem().getDbID().intValue());
					
				im.setDataset_id(currentDataset.getDbID().intValue());
				im.setDatestamp(System.currentTimeMillis());
				im.setItem_id(item.getDatasetId().intValue());
				im.setOrg_id( orgId );
				im.setPrefix(ns);
				im.setProject("");
				im.setSchema_id(schemaId);
				im.setSourceDataset_id( originalDatasetId );
				im.setUser_id(1);
				im.setXml( item.getXml());
				
				//just the report id
				im.setParams(extendedParameterList);

				for (String routingKey : routingKeySet)
					rmp.send(im, routingKey);

				itemCounter.inc();
				// edm get inserted twice, once as OAI_DC
				if( ns.getPrefix().equals( "rdf" )) {
					itemCounter.inc();
					edmCounter.inc();
				}
				
			} catch (Exception e) {
				log.error( "Sending item to OAI failed for #" + item.getDbID());
				throw e;
			}
		}
		
		// writes result in pr
		public void finishPublication( PublicationRecord pr) {
			try {
				long lastChange = System.currentTimeMillis();
				int lastTotal=0;
				while( true ) {
					int currentTotal = osc.getProgress(reportId).getTotalRecords();
					int inserted  = osc.getProgress(reportId).getInsertedRecords();
					int conflicts = osc.getProgress(reportId).getConflictedRecords();
	
					if(  (inserted+conflicts) == itemCounter.get() ) {
						log.info( "All items processed.");
						break;
					}
					if( currentTotal != lastTotal ) {
						lastChange = System.currentTimeMillis();
						lastTotal = currentTotal;
					}
					else {
						if(( System.currentTimeMillis() - lastChange ) > 1000l*60l*10l ) {
							log.warn( "Timeout occured in Publication.");
							originalDataset.logEvent("Timeout occured in Publication", "Expected " + itemCounter.get() + " records");
							break;
						}
					}
					Thread.sleep( 10000l );
				}
				
				pr.setReport(osc.getProgress(reportId).toString());
				osc.closeReport(reportId);
				osc.close();
				
				rmp.close();
	
				originalDataset.logEvent( "Finished publishing. " + edmCounter.get() + " items published.",pr.getReport());
				pr.setPublishedItemCount(edmCounter.get());
				pr.setEndDate(new Date());
				pr.setStatus(Dataset.PUBLICATION_OK);
				DB.commit();
	
			} catch( Exception e ) {
				log.error( "Problem closing OAI publication", e );

				pr.setStatus(Dataset.PUBLICATION_FAILED);
				pr.setEndDate(new Date());
				pr.setReport(e.getMessage());
				pr.setPublishedItemCount(-1);
				DB.commit();
			}
		}
	}
	
	
	public static List<XmlSchema> schemasFromConfigNames( String schemaNames ) {
		
		if(StringUtils.empty(schemaNames)) {
			return Collections.emptyList();
		}
		
		Set<String> schemaSet = TextParseUtil.commaDelimitedStringToSet(schemaNames);

		ArrayList<XmlSchema> result = new ArrayList<XmlSchema>();
		for( String name:schemaSet ) {
			XmlSchema xs = DB.getXmlSchemaDAO().getByName(name);
			if( xs != null ) result.add( xs );
			else {
			 	log.error("Configured target schema  ["+name+"] not in DB");
			 	// throw new Error( "Target schema ["+name+"] not in DB" );
			}
		}
		return result;
	}
	
	public static Optional<Dataset> uniqueSuitableDataset( Dataset ds, List<XmlSchema> schemas ) {
		ArrayList<Dataset> all = new ArrayList<>();
		all.addAll( ds.getDerived());
		all.add( ds );
		List<Dataset> lds = all.stream()
				.filter( myds -> isSchemaAndHasValidItems(myds, schemas))
				.collect( Collectors.toList());
		if( lds.size() == 1 ) return Optional.of( lds.get(0));
		else return Optional.empty();
	}
	
	// is there exactly one suitable dataset in this tree with valid items? 
	public static Optional<Dataset> uniqueSuitableDataset( Dataset ds, String schemaConfig ) {
		List<XmlSchema> schemas = schemasFromConfigNames(schemaConfig);
		return uniqueSuitableDataset(ds, schemas);
	}
	
	public static boolean isSchemaAndHasValidItems( Dataset ds, List<XmlSchema> schemas ) {
		if( ds.getValidItemCount() == 0 )  return false;
		if( ds.getSchema() == null ) return false;
		
		for( XmlSchema schema: schemas) {
			if( ds.getSchema().getDbID() == schema.getDbID()) return true;
		}
		return false;
	}
	
	
	// does Dataset has valid items in any of the given schema names? Then return true, else false.
	public static boolean isSchemaAndHasValidItems( Dataset ds, String schemaConfig ) {
		if( ds.getValidItemCount() == 0 )  return false;
		if( ds.getSchema() == null ) return false;
		
		List<XmlSchema> schemas = schemasFromConfigNames( schemaConfig );
		return isSchemaAndHasValidItems(ds, schemas);
	}
	
	// only direct conversions are useful, multi step wont be done automated
	public Optional<Crosswalk> directConversion( XmlSchema start, XmlSchema finish ) {
		List<Crosswalk> lc = DB.getCrosswalkDAO().findBySourceAndTarget(start.getName(), finish.getName());
		if( lc.size() == 1 ) return Optional.of(lc.get(0));
		else return Optional.empty();
	}
	
	
	
	
	
	/* Will make it if it doesnt exist, stores in db */
	public static PublicationRecord setPublicationRunning( Dataset ds, String target, Optional<User> publisherOpt ) {
		Dataset originalDataset = ds.getOrigin();
		User publisher = publisherOpt.orElse(originalDataset.getCreator());
		// in case this was started somewhere else, we dont do much
		
		PublicationRecord pr = DB.getPublicationRecordDAO().getByPublishedDatasetTarget(ds,target).orElseGet(
				()-> {
					PublicationRecord pubRec = new PublicationRecord();
	
					pubRec.setStartDate(new Date());
					pubRec.setPublisher(publisher);
					pubRec.setOriginalDataset(originalDataset);
					pubRec.setPublishedDataset( ds );
					pubRec.setOrganization( ds.getOrganization());
					return pubRec;
				});
		pr.setStatus(Dataset.PUBLICATION_RUNNING);
		DB.getPublicationRecordDAO().makePersistent(pr);
		DB.commit();

		return pr;
	}

	
	// sends given ds to OAI
	/**
	 * Give the user and which Message Queue exchange you are going to use.
	 * This need to run in a worker thread, it sleeps a lot, waiting for the 
	 * Message queue consumers to finish inserting.
	 * @param ds The dataset to send to the queue
	 * @param publisher which user is doing it
	 * @param exchange the Queue exchange
	 * @param routingKeys what keys to add to the message 
	 */
	public static void oaiPublish( final Dataset ds, String target, User publisher, String queueExchange, String routingKeys,
			String oaiHost, int oaiPort, final Interceptor<ItemMessage, ItemMessage> itemMessageInterceptor ) {
		final Dataset originalDataset = ds.getOrigin();
		
		log.debug( "External publish " + originalDataset.getName() );
		originalDataset.logEvent("Sending " + ds.getSchema().getName() + " to OAI." );
		
		// get or make a publication record
		
		
		PublicationRecord pr = setPublicationRunning(ds, target, Optional.of(publisher) );
		
		final Counter itemCounter = new Counter();
		try {
			final RecordMessageProducer rmp = new RecordMessageProducer(Config.get("queue.host"), queueExchange );

			final Namespace ns = new Namespace();
			final int schemaId = ds.getSchema().getDbID().intValue();
						
			final Set<String> routingKeysSet =  TextParseUtil.commaDelimitedStringToSet( routingKeys );

			XpathHolder xmlRoot = ds.getRootHolder().getChildren().get(0);
			
			ns.setPrefix( xmlRoot.getUriPrefix());
			ns.setUri(xmlRoot.getUri());

			OAIServiceClient osc = new OAIServiceClient(oaiHost, oaiPort);
			
			String projectName = "";
			for( String s: routingKeysSet ) {
				if( s.contains("oai")) projectName= s;
			}
			
			ArrayList<Integer> datasetIds = new ArrayList<Integer>();
			datasetIds.add( originalDataset.getDbID().intValue());
			
			final String reportId = osc.createReport(projectName, originalDataset.getCreator().getDbID().intValue(), 
					(int) originalDataset.getOrganization().getDbID(), 
					datasetIds );
			ds.logEvent( "OAI report id is " + reportId);
			ExtendedParameter ep = new ExtendedParameter();
			ep.setParameterName("reportId" );
			ep.setParameterValue(reportId);
			final ArrayList<ExtendedParameter> params = new ArrayList<ExtendedParameter>();
			params.add( ep );
			
			itemCounter.set(0);
			
			ApplyI<Item> itemSender = new ApplyI<Item>() {
				@Override
				public void apply(Item item) throws Exception {
						ItemMessage im = new ItemMessage();
						im.setDataset_id(item.getDataset().getDbID().intValue());
						im.setDatestamp(System.currentTimeMillis());
						im.setItem_id(item.getDbID());
						im.setOrg_id((int) item.getDataset().getOrganization().getDbID());
						im.setPrefix(ns);
						im.setProject("");
						im.setSchema_id(schemaId);
						im.setSourceDataset_id(originalDataset.getDbID().intValue());
						if(item.getSourceItem() != null)
							im.setSourceItem_id(item.getSourceItem().getDbID());
						else
							im.setSourceItem_id(item.getImportItem().getDbID());
						im.setUser_id(originalDataset.getCreator().getDbID().intValue());
						im.setXml(item.getXml());
						im.setParams(params);
						
						ThrowingConsumer<ItemMessage> normalMessageProcess = (newItemMessage)->{
							for( String routingKey: routingKeysSet ) 
								rmp.send(newItemMessage, routingKey );
							itemCounter.inc();															
						};
						
						if( itemMessageInterceptor != null ) 
							normalMessageProcess = itemMessageInterceptor.intercept( normalMessageProcess );
					
						normalMessageProcess.accept( im );
				}
			};
			
			ds.processAllValidItems(itemSender, false);
			
			// all items send off, now check the reports
			
			long lastChange = System.currentTimeMillis();
			int lastTotal=0;
			while( true ) {
				int currentTotal = osc.getProgress(reportId).getTotalRecords();
				int inserted  = osc.getProgress(reportId).getInsertedRecords();
				int conflicts = osc.getProgress(reportId).getConflictedRecords();

				// likely inserted/2+conflicts needs to be itemCounter
				// inserted dc and edm namespace and rejected items only once counted 
				if(  (inserted/2+conflicts) == itemCounter.get() ) {
					log.info( "All items processed.");
					break;
				}
				if( currentTotal != lastTotal ) {
					lastChange = System.currentTimeMillis();
					lastTotal = currentTotal;
				}
				else {
					if(( System.currentTimeMillis() - lastChange ) > 1000l*60l*10l ) {
						log.warn( "Timeout occured in Publication.");
						break;
					}
				}
				Thread.sleep( 10000l );
			}
			
			pr.setReport(osc.getProgress(reportId).toString());
			osc.closeReport(reportId);
			osc.close();
			
			rmp.close();

			ds.logEvent( "Finished publishing. " + itemCounter.get() + " items send.");
			pr.setPublishedItemCount(itemCounter.get());
			pr.setEndDate(new Date());
			pr.setStatus(Dataset.PUBLICATION_OK);
			DB.commit();
		} catch( Exception e ) {
			log.warn( "Item publication went wrong", e );
			ds.logEvent( "Publication went wrong. " + e.getMessage());
			pr.setStatus(Dataset.PUBLICATION_FAILED);
			pr.setEndDate(new Date());
			pr.setReport(e.getMessage());
			pr.setPublishedItemCount(itemCounter.get());
			DB.commit();
		}		
	}
   
	public static void oaiUnpublish( Dataset ds, String routingKeys, String oaiHost, int  oaiPort ) {
		log.debug( "OAI unpublish " + ds.getName() );
		OAIServiceClient osc = new OAIServiceClient(oaiHost, oaiPort);
		
		Set<String> routingKeysSet =  TextParseUtil.commaDelimitedStringToSet( routingKeys );
		String projectName = "";
		for( String s: routingKeysSet ) {
			if( s.contains("oai")) projectName= s;
		}

		if( StringUtils.empty( projectName )) {
			log.error( "Routing key empty, external unpublish failed");
			ds.getOrigin().logEvent("External removal failed", "Bad routing key on Dataset #"+ds.getDbID());
		} else 	{
			osc.unpublishRecordsByDatasetId((int)ds.getOrganization().getDbID(), 
					ds.getCreator().getDbID().intValue(),
					projectName, 
					ds.getOrigin().getDbID().intValue());
			ds.logEvent("Removed from External.");
		}
	}

}
