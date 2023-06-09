package gr.ntua.ivml.mint;

import gr.ntua.ivml.mint.concurrent.Queues;
import gr.ntua.ivml.mint.concurrent.XSLTransform;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.Crosswalk;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Item;
import gr.ntua.ivml.mint.persistent.Organization;
import gr.ntua.ivml.mint.persistent.PublicationRecord;
import gr.ntua.ivml.mint.persistent.Transformation;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.persistent.XmlSchema;
import gr.ntua.ivml.mint.persistent.XpathHolder;
import gr.ntua.ivml.mint.pi.messages.ExtendedParameter;
import gr.ntua.ivml.mint.pi.messages.ItemMessage;
import gr.ntua.ivml.mint.pi.messages.Namespace;
import gr.ntua.ivml.mint.util.ApplyI;
import gr.ntua.ivml.mint.util.Config;
import gr.ntua.ivml.mint.util.Counter;
import gr.ntua.ivml.mint.util.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.opensymphony.xwork2.util.TextParseUtil;

public class BasePublication extends Publication {
	
	public class Publish implements Runnable {
		private Dataset ds;
		private User publisher;
		
		public Publish( Dataset ds, User publisher ) {
			this.ds = ds;
			this.publisher = publisher;
		}
		
		public void run() {
			try {
				DB.getSession().beginTransaction();
				ds = DB.getDatasetDAO().getById(ds.getDbID(), false);
				publisher = DB.getUserDAO().getById(publisher.getDbID(), false);
				
				externalPublish( ds, publisher );
				DB.commit();
			} catch( Exception e ) {
				log.error( "Publish has exception.", e );
			} finally {
				DB.closeSession();
				DB.closeStatelessSession();
			}
		}
	}
	
	public class UnPublish implements Runnable {
		public UnPublish() {
			
		}
		
		public void run() {
			
		}
	}
	
	public class Prepare implements Runnable {
		private Dataset ds;
		private Map<String, String> params;
		private User user;
		
		public Prepare( Dataset ds, Map<String, String> params, User user) {
			this.ds = ds;
			this.params = params;
			this.user = user;
		}
		
		public void run() {
			try {
				DB.getSession().beginTransaction();
				
				ds = DB.getDatasetDAO().getById(ds.getDbID(), false);
				user = DB.getUserDAO().getById(user.getDbID(), false);
				
				for( XmlSchema target: requiredTargetSchemas()) {
					runTransforms(ds, params, target, user);
				}
				DB.commit();
			} catch( Exception e ) {
				log.error("Prepare went wrong.", e );
				ds.logEvent( "Prepare for publish failed. " + e.getMessage());
				DB.commit();
			} finally {
				DB.closeSession();
				DB.closeStatelessSession();
			}
		}
	}
	
	
	public BasePublication(Organization org) {
		super(org);
	}

	@Override
	public boolean isPublishable(Dataset ds) {
		return (isDirectlyPublishable(ds) || canCreateRequired(ds));
	}

	/**
	 * Compute missing Datasets for given ds. Supply parameters for this in a Map.
	 * @param ds
	 * @throws Exception
	 */
	
	public void prepareForPublication( Dataset ds, Map<String, String> params, User user) throws Exception {
		if( ! isPublishable(ds) ) throw new Exception( "Is not publishable!");
		Prepare prep = new Prepare( ds, params, user );
		Queues.queue( prep, "db" );
	}
	
	
	@Override
	public boolean publish(Dataset ds, User publisher)  {
		if( ! isDirectlyPublishable(ds)) return false;
		Publish pbl = new Publish( ds, publisher );
		Queues.queue( pbl, "net" );
		return true;
	}

	@Override
	public boolean isDirectlyPublishable(Dataset ds) {
		return hasRequiredSchemas(ds);
	}


	@Override
	public boolean unpublish(Dataset ds) {
		// no dataset is removed for this
		// send the unpublish info to the export target
		boolean result = externalUnpublish( ds );
		
		// get the oai schema dataset
		
		Dataset publishedDs = ds.getBySchemaName(Config.get( "oai.schema"));
		if( publishedDs == null ) return false;
		
		PublicationRecord pr = DB.getPublicationRecordDAO().getByPublishedDataset(publishedDs).orElse(null);		
		
		if( pr != null  ) {
			DB.getPublicationRecordDAO().makeTransient(pr);
			DB.commit();
			return true;
		} else {
			log.info( "Unpublish " + ds.getDbID() + " failed.");
			return false;
		}
	}


	/**
	 * A dataset can be published when it has derived datasets with schema names listed in config. 
	 * @return
	 */
	public List<XmlSchema> getSchemasFromConfig( String configKey) {
		String schemaNames= Config.get(  configKey );
		if(StringUtils.empty(schemaNames)) {
			log.warn( "No Schema configured for '" + configKey +"'");
			return Collections.emptyList();
		}
		
		Set<String> schemaSet = TextParseUtil.commaDelimitedStringToSet(schemaNames);
		ArrayList<XmlSchema> result = new ArrayList<XmlSchema>();
		for( String name:schemaSet ) {
			XmlSchema xs = DB.getXmlSchemaDAO().getByName(name);
			if( xs != null ) result.add( xs );
			else {
			 	log.error("Configured schema  ["+name+"] not in DB");
			 	throw new Error( "Configured schema ["+name+"] not in DB" );
			}
		}
		return result;
	}
	
	public List<XmlSchema> requiredTargetSchemas() {
		return getSchemasFromConfig( "publication.requiredSchemaNames");
	}
	
	
	/**
	 * Compares existing transformed datasets with required schemas from config
	 * @param ds
	 * @return
	 */
	public boolean hasRequiredSchemas( Dataset ds ) {
		for( XmlSchema schema: requiredTargetSchemas()) 
			if( ds.getBySchemaName(schema.getName()) == null ) return false;
		return true;
	}
	
	
	/**
	 * Compares existing transformed datasets with required schemas from config
	 * @param ds
	 * @return
	 */
	public boolean hasRequiredSchemasAndValidItems( Dataset ds ) {
		for( XmlSchema schema: requiredTargetSchemas()) {
			Dataset dsDerived = ds.getBySchemaName(schema.getName());
			if( dsDerived.getValidItemCount() == 0 ) return false; 
		}
		return true;
	}

	/**
	 * Can I reach all required target schemas in 2 steps ?
	 * @param ds
	 * @return
	 */
	public boolean canCreateRequired( Dataset ds  ) {
		for( XmlSchema target: requiredTargetSchemas()) {
			if( pathlenSchema(2, ds, target) < 0 ) return false;
		}
		return true;
	}
	
	public boolean canCrosswalkTo( Dataset ds, List<XmlSchema> schemas ) {
		for( XmlSchema target: schemas ) {
			if( pathlenSchema(2, ds, target) < 0 ) return false;
		}
		return true;		
	}
	
	/**
	 * How many crosswalks to reach target schema, check until max depth
	 * depth 1 - direct conversion possible
	 * depth >1 -  depth-1 conversion possible
	 * @param depth
	 * @param ds
	 * @param target
	 * @return
	 */
	private int pathlenSchema( int max, Dataset ds, XmlSchema... targets ) {
		// check if I have target
		for( XmlSchema target: targets ) {
			if( ds.getBySchemaName(target.getName()) != null ) return 0;
		}
		if( max == 0 ) return -1;
		
		HashMap<Long, XmlSchema> newTargets = new HashMap<Long, XmlSchema>();
		
		for( XmlSchema target: targets ) {
			List<Crosswalk> potentialCrosswalks = DB.getCrosswalkDAO().findByTargetName(target.getName());
			for( Crosswalk cw: potentialCrosswalks) {
				newTargets.put( cw.getSourceSchema().getDbID(), cw.getSourceSchema());
			}
		}
		
		targets = newTargets.values().toArray(new XmlSchema[0]);
		int result = pathlenSchema( max-1, ds, targets );
		if( result == -1 ) return -1;
		else return result+1;
	}
	
	
	
	public Dataset runTransforms( Dataset ds, Map<String, String> params, XmlSchema target, User user ) throws Exception {
		if( ds.getBySchemaName(target.getName()) != null ) return ds.getBySchemaName(target.getName()); 
		List<Crosswalk> potentialCrosswalks = DB.getCrosswalkDAO().findByTargetName(target.getName());
		for( Crosswalk cw: potentialCrosswalks ) {
			String sourceName = cw.getSourceSchema().getName();
			Dataset source = ds.getBySchemaName(sourceName);
			if( source != null ) {
				return directTransformation(source, params, cw, user );
			}
		}

		// indirect 
		for( Crosswalk cw: potentialCrosswalks ) {
			if( pathlenSchema( 1, ds, cw.getSourceSchema()) == 1 ) {
				Dataset intermediate = runTransforms( ds, params, cw.getSourceSchema(), user);
				return directTransformation(intermediate, params, cw, user );
			}
		}
		
		// should not get here
		return null;
	}

	/**
	 * Execute the crosswalk.
	 * @param ds
	 * @param params
	 * @param cw
	 * @param user
	 * @throws Exception
	 */
	public Dataset directTransformation( Dataset ds, Map<String, String> params, Crosswalk cw, User user ) throws Exception  {
		Transformation tr = new Transformation();
		if( user != null )
			tr.init(user);
		else
			tr.init( ds.getCreator());
		
		tr.setOrganization(ds.getOrganization());
		tr.setName("Auto Crosswalk to " + cw.getTargetSchema().getName());
		tr.setParentDataset(ds);
		tr.setCrosswalk(cw);
		tr.setSchema(cw.getTargetSchema());
		DB.getTransformationDAO().makePersistent(tr);
		tr.logEvent( "Auto created for publication." );
		DB.commit();
		XSLTransform xt = new XSLTransform(tr);
		xt.setXslTransformationParams(params);
		xt.runInThread();
		return tr;
	}
	
	/**
	 * Send the data away. If you want all the Transformation magic, you can just overwrite this one.
	 * @param ds
	 * @return
	 */
	public void externalPublish( Dataset ds, User publisher ) {
		String oaiSchema = Config.get( "oai.schema" );
		final Dataset originalDataset = ds.getOrigin();
		
		log.debug( "External publish " + originalDataset.getName() );
		originalDataset.logEvent("Sending to External.", "Use schema " + oaiSchema + " to publish to OAI." );
		final Dataset derivedItemsDataset = ds.getBySchemaName(oaiSchema);
		
		// get or make a publication record
		
		PublicationRecord pr;
		
		pr = DB.getPublicationRecordDAO().getByPublishedDataset(derivedItemsDataset).orElseGet(
				()-> {
					PublicationRecord pubRec = new PublicationRecord();
	
					pubRec.setStartDate(new Date());
					pubRec.setPublisher(publisher);
					pubRec.setOriginalDataset(originalDataset);
					pubRec.setPublishedDataset(derivedItemsDataset );
					pubRec.setOrganization(derivedItemsDataset.getOrganization());
					return pubRec;
				});
		pr.setStatus(Dataset.PUBLICATION_RUNNING);
		DB.getPublicationRecordDAO().makePersistent(pr);
		DB.commit();

		final Counter itemCounter = new Counter();
		try {
			final RecordMessageProducer rmp = new RecordMessageProducer(Config.get("queue.host"), "mint" );
			// this should be the derived Dataset with the right schema
			final Namespace ns = new Namespace();
			final int schemaId = derivedItemsDataset.getSchema().getDbID().intValue();
			pr.setPublishedDataset(derivedItemsDataset);
			
			String routingKeysConfig = Config.get("queue.routingKey");
			if(StringUtils.empty(routingKeysConfig)) {
				log.warn( "No routing Key for Publication.");
				ds.logEvent("No routing Key for Publication." );
				throw new Exception( "No routing key");
			}
			
			final Set<String> routingKeys =  TextParseUtil.commaDelimitedStringToSet(routingKeysConfig);

			XpathHolder xmlRoot = derivedItemsDataset.getRootHolder().getChildren().get(0);
			
			ns.setPrefix( xmlRoot.getUriPrefix());
			ns.setUri(xmlRoot.getUri());

			OAIServiceClient osc = new OAIServiceClient(Config.get( "OAI.server.host"), Integer.parseInt(Config.get("OAI.server.port")));
			
			String projectName = "";
			for( String s: routingKeys ) {
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
						
						for( String routingKey: routingKeys ) 
							rmp.send(im, routingKey );
						itemCounter.inc();
				}
			};
			
			derivedItemsDataset.processAllValidItems(itemSender, false);
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

	public boolean externalUnpublish( Dataset ds ) {
		log.debug( "External unpublish " + ds.getName() );
		OAIServiceClient osc = new OAIServiceClient(Config.get( "OAI.server.host"), Integer.parseInt(Config.get("OAI.server.port")));
		
		String routingKeysConfig = Config.get("queue.routingKey");
		if(StringUtils.empty(routingKeysConfig)) {
			log.warn( "No routing Key for Publication.");
			return false;
		}
		Set<String> routingKeys =  TextParseUtil.commaDelimitedStringToSet(routingKeysConfig);
		String projectName = "";
		for( String s: routingKeys ) {
			if( s.contains("oai")) projectName= s;
		}

		osc.unpublishRecordsByDatasetId((int)ds.getOrganization().getDbID(), 
				ds.getCreator().getDbID().intValue(),
				projectName, 
				ds.getOrigin().getDbID().intValue());
		ds.logEvent("Removed from External.");
		return true;
	}
}
