package gr.ntua.ivml.mint.concurrent;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.log4j.Logger;

import gr.ntua.ivml.mint.Custom;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.db.LockManager;
import gr.ntua.ivml.mint.f.Interceptor;
import gr.ntua.ivml.mint.f.ThrowingConsumer;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Item;
import gr.ntua.ivml.mint.persistent.Lock;
import gr.ntua.ivml.mint.persistent.Transformation;
import gr.ntua.ivml.mint.persistent.XpathHolder;
import gr.ntua.ivml.mint.util.Interceptors.EndEstimateInterceptor;
import gr.ntua.ivml.mint.util.Interceptors.ProgressInterceptor;
import gr.ntua.ivml.mint.util.StringUtils;
import groovy.lang.GroovyShell;
import groovy.lang.Script;

public class GroovyTransform implements Runnable {
	public final Logger log = Logger.getLogger(GroovyTransform.class );

	public Transformation transformation;
	private Ticker ticker;
	
	private List<Lock> aquiredLocks = Collections.emptyList();
	private String script;
	private Interceptor<Item, Item> scriptInterceptor;
	public Optional<Consumer<Transformation>> postProcessor = Optional.empty();
	
	public GroovyTransform( Transformation tr, String groovyScript ) throws Exception {
		this.transformation = tr;
		
		// syntax check
		script = groovyScript;
		scriptInterceptor = getScriptInterceptor(script);
	}

	// make a GroovyTransform ready to queue for generating from regular groovy console
	public GroovyTransform( Transformation transformation, Interceptor<Item, Item> processInterceptor ) {
		this.transformation = transformation;
		scriptInterceptor = processInterceptor;
	}
	
	
	/**
	 * Make a default Interceptor based Transformation, modify transformation in passed callback.
	 * (eg change name, schema, creator etc)
	 * @param parent
	 * @param processInterceptor
	 * @param modifyTransformation
	 * @return
	 */
	public static GroovyTransform defaultFromDataset( Dataset parent, Interceptor<Item, Item> processInterceptor,
			Optional<Consumer<Transformation>> modifyTransformationOptional ) throws Exception {
		Transformation transformation = new Transformation();
		transformation.init(parent.getCreator());
		transformation.setName("Process: "+parent.getName());
		transformation.setParentDataset(parent);
		transformation.setCreated(new Date());
		transformation.setOrganization(parent.getOrganization());
		transformation.setSchema( parent.getSchema());
		transformation.setTransformStatus( Transformation.TRANSFORM_RUNNING);
		
		modifyTransformationOptional.ifPresent( mod-> mod.accept(transformation));
		Transformation storedTransformation = DB.getTransformationDAO().makePersistent(transformation);
		DB.commit();
		return new GroovyTransform( storedTransformation, processInterceptor );
	}
	
	
	static class StoringConsumer implements ThrowingConsumer<Item> {
		public long total = 0;
		public long valid = 0;
		public long invalid = 0;

		Dataset ds;

		public StoringConsumer( Dataset ds ) {
			this.ds = ds;
		}
		@Override
		public void accept(Item item) throws Exception {
			// TODO Auto-generated method stub
			item.setDataset(ds);

			if( item.isValid())
				valid++;
			else
				invalid++;
			total++;

			DB.getSession().save(item);
			DB.getSession().flush();
			DB.getSession().evict(item);	
		}		
	}	
	/**
	 * Precondition, to aquire the right locks !
	 */
	public void runInThread() {
		
		try {
			ticker = new Ticker(60);
			
			transformation = DB.getTransformationDAO().getById(transformation.getDbID(), false);
			// new version of the transformation for this session
			if( transformation == null ) {
				log.error( "Total desaster, Transformation unavailable, no reporting to UI!!!");
				return;
			}

			process();

			// try to build stats ... need to circumvent this if we don't expect XML
			// TODO: Only run when we expect XML
			SchemaStatsBuilder ssb = new SchemaStatsBuilder(transformation);
			ssb.runInThread();
			
			// stats clear session !!!
			transformation = DB.getTransformationDAO().getById(transformation.getDbID(), false);

			// Validator only makes sense on XML.
			// ALL ITEMS have to be valid xml though
			if( Dataset.STATS_OK.equals( transformation.getStatisticStatus())) {
				Validator validator = new Validator( transformation );
				validator.runInThread();

				if( validator.getValidItemOutputFile() != null ) {
					transformation.uploadFile(validator.getValidItemOutputFile());				
					transformation.setLoadingStatus(Dataset.LOADING_OK);
				}
				if( validator.getInvalidItemOutputFile() != null ) {
					transformation.uploadInvalid(validator.getInvalidItemOutputFile());				
				}
				DB.commit();
				validator.clean();
			}
			
			// need to set item root if possible. Either from schema or to the root node
			if( transformation.getStatisticStatus().equals( Dataset.STATS_OK)) {
				transformation.updateItemPathsFromSchema();
				
				// if not set item root, make one up
				if( transformation.getItemRootXpath() == null ) {
					XpathHolder xp = transformation.getRootHolder();
					if( xp != null ) {
						xp = xp.getChildren().get(0);
						transformation.setItemRootXpath(xp);
					}
				}
				DB.commit();
			}
			
			// aa hook to do some work after all is pushed through
			postProcessor.ifPresent(process -> process.accept(transformation));
			
			// support transformation hotdir
			TransformHotdir hotdir = DB.getHotdir();
			if( hotdir != null ) hotdir.addDs( transformation.getDbID().intValue());
			
			// fire of solarizer for the result.
			if( Solarizer.isEnabled()) {
				if( Custom.allowSolarize( transformation ))
				Solarizer.queuedIndex(transformation);
			}
			
			
		} catch( Exception e ) {
			log.error( "Transformation failed, should be already noted.", e );
		} catch( Throwable t ) {
			log.error( "uhh", t );
		} finally {
				ticker.cancel();
		}
	}

	public void run() {
		log.info( "Offline transform started");
		// this might be a used session, the thread is reused
		DB.getSession().beginTransaction();
		// should not throw anything
		runInThread();
		
		// need to release locks here 
		releaseLocks();
		try {
		DB.closeStatelessSession();
		DB.closeSession();
		} catch( Exception e ) {
			log.error( "Problem closing sessions.", e );
		}
	}
	
	private void releaseLocks() {
		LockManager lm = DB.getLockManager();
		for( Lock l: aquiredLocks)
			lm.releaseLock(l);
	}

	
	public Interceptor<Item, Item> getScriptInterceptor( String script ) throws Exception {
		GroovyShell gshell = new GroovyShell();
		Script gscript = gshell.parse(script);

		// create either an XML Processor or an ItemProcessor
		Object processor = gscript.run();
		
		if( ! (processor instanceof Interceptor<?, ?> ))
			throw new Exception( "No Interceptor returned from script!" );
		return (Interceptor<Item, Item>) processor;
	}

	
	private void process() throws Exception {
		try {
			transformation.logEvent("Processing Transformation started.", transformation.getName());
			transformation.setTransformStatus(Transformation.TRANSFORM_RUNNING);
			DB.getTransformationDAO().makePersistent(transformation);
			DB.commit();
			
			final long totalInputItems = transformation.getParentDataset().getItemCount();
			final StoringConsumer store = new StoringConsumer(transformation);

			try (ProgressInterceptor progressInterceptor = new ProgressInterceptor(transformation, totalInputItems)) {

				// Set it up: endestimate->progress->script ( storingConsumer)
				final ThrowingConsumer<Item> sourceConsumer = new EndEstimateInterceptor(totalInputItems,
						transformation)
						.into(progressInterceptor)
						.into(scriptInterceptor)
						.intercept(store);

				DB.getItemDAO().applyForDataset(transformation.getParentDataset(),
						item -> sourceConsumer.accept(item), true);
			}
			transformation.setItemCount((int) store.total);
			transformation.setValidItemCount((int) store.valid);

			transformation.logEvent("Dataset processing finished.", transformation.getName());
			transformation.setTransformStatus(Transformation.TRANSFORM_OK);
			DB.commit();

		} catch (Exception e) {
			transformation.setTransformStatus(Transformation.TRANSFORM_FAILED);
			transformation.logEvent("Process failed. " + e.getMessage(), StringUtils.stackTrace(e, null));
			DB.commit();
			throw e;
		} finally {
			releaseLocks();
		}
	}
	
	
	
	public void setAquiredLocks( List<Lock> locks ) {
		this.aquiredLocks = locks;
	}
}
