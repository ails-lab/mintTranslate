package gr.ntua.ivml.mint.util;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.ocpsoft.prettytime.PrettyTime;
import org.xml.sax.XMLReader;

import gr.ntua.ivml.mint.concurrent.Ticker;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.f.Interceptor;
import gr.ntua.ivml.mint.f.ThrowingConsumer;
import gr.ntua.ivml.mint.mapping.model.Mappings;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Item;
import gr.ntua.ivml.mint.persistent.Mapping;
import gr.ntua.ivml.mint.xml.transform.XMLFormatter;
import gr.ntua.ivml.mint.xml.transform.XSLTGenerator;
import gr.ntua.ivml.mint.xml.transform.XSLTransform;
import gr.ntua.ivml.mint.xsd.ReportErrorHandler;
import gr.ntua.ivml.mint.xsd.SchemaValidator;
import nu.xom.Builder;
import nu.xom.Document;

/**
 * 
 * @author arne
 *
 * Some useful Interceptors for Mint
 */
public class Interceptors {

	public static Logger log = Logger.getLogger( Interceptors.class);
	
	// this one logs to the dataset an end estimate when there are more than 2000 items to process
	public static class EndEstimateInterceptor implements Interceptor<Item, Item> {
		long startTime;
		long totalCount;
		long currentCount = 0;
		Dataset ds;
		
		public EndEstimateInterceptor( long totalCount, Dataset ds ) {
			this.totalCount = totalCount;
			this.startTime = System.currentTimeMillis();
			this.ds = ds ;
		}

		@Override
		public ThrowingConsumer<Item> intercept(ThrowingConsumer<Item> nestedConsumer) {
			if( totalCount <= 2000) return nestedConsumer;
			return item -> {
				currentCount++;
				if( currentCount == (totalCount/50)) {
					long usedTime = System.currentTimeMillis()-startTime;
					PrettyTime pt = new PrettyTime();
						
					String expected = pt.format( new Date( System.currentTimeMillis() + 49*usedTime )); 
					ds.logEvent( "Expect finished " + expected );
					DB.commit();
				}
				nestedConsumer.accept(item);
			};
		}
	}
	
	/**
	 * An Item Interceptor that logs progress every minute to the log file.
	 * It needs to be closed to get removed from a timer.
	 * @author arne
	 *
	 */
	public static class ProgressInterceptor implements Interceptor<Item, Item>, Closeable {
		// this one log once per minute the process to the log file

		long totalCount;
		long currentCount = 0;
		Ticker ticker = new Ticker(60);
		String formatString;
		/**
		 * Report progress every minute on the given count of item for the given Dataset.
		 * @param ds
		 * @param totalCount
		 */
		public ProgressInterceptor( Dataset ds, long totalCount ) {
			this.formatString = "Processed %d of %d in Dataset #" + ds.getDbID();
			this.totalCount = totalCount;
		}
		
		public ProgressInterceptor( String formatCountTotal, long totalCount ) {
			this.formatString = formatCountTotal;
			this.totalCount = totalCount;
		}
		
		@Override
		public ThrowingConsumer<Item> intercept(ThrowingConsumer<Item> nestedConsumer) {
			return item -> {
				currentCount++;
				if( ticker.isSet()) {
					ticker.reset();
					DB.commit();
					Dataset.log.info(String.format( formatString, currentCount,  totalCount ));
				}
				nestedConsumer.accept(item);
			};
		}

		@Override
		public void close() throws IOException {
			ticker.cancel();
		}
	}
	
	// a default unmodified child derived Item is created
	public static class DefaultItemInterceptor implements Interceptor<Item, Item> {
		public ThrowingConsumer<Item> intercept(ThrowingConsumer<Item> nestedConsumer) {
			return item -> {
				Item newItem = new Item();
				newItem.setXml(item.getXml());
				newItem.setLabel( item.getLabel());
				newItem.setPersistentId(item.getPersistentId());
				newItem.setSourceItem(item);
			};
		}
	}
	
	/**
	 * This Interceptor wraps a simple document interceptor into an Item Interceptor.
	 * doc gets extracted, processed and reinserted into an Item to be passed on.
	 * @author arne
	 *
	 */			
	public static class ItemDocWrapInterceptor implements Interceptor<Item, Item> {
		Item currentSourceItem; 
		Interceptor<Document, Document> wrappedInterceptor;
		boolean createNewItem = true;
		
		/**
		 * Wrap the given Document->Document Interceptor into an Item->Item Interceptor
		 * @param createNewItem If the Item passed on is new and has as parent the incoming Item.
		 * @param wrappedInterceptor
		 */
		public ItemDocWrapInterceptor( Interceptor<Document, Document> wrappedInterceptor, boolean createNewItem ) {
			this.wrappedInterceptor = wrappedInterceptor;
			this.createNewItem = createNewItem;
		}
		
		@Override
		public ThrowingConsumer<Item> intercept(ThrowingConsumer<Item> nestedConsumer) {

			final ThrowingConsumer<Document> docSinkModifyItem = doc -> {
				currentSourceItem.setXml(doc.toXML());
				nestedConsumer.accept(currentSourceItem);
			};

			final ThrowingConsumer<Document> docSinkNewItem = doc -> {
				Item newItem = new Item();
				
				newItem.setXml(doc.toXML());
				newItem.setSourceItem( currentSourceItem );
				newItem.setLabel( currentSourceItem.getLabel());
				newItem.setPersistentId( currentSourceItem.getPersistentId());
				newItem.setValid( currentSourceItem.isValid());
				
				nestedConsumer.accept(newItem);
			};

			final ThrowingConsumer<Document> modifiedConsumer = createNewItem?
					wrappedInterceptor.intercept(docSinkNewItem) :
					wrappedInterceptor.intercept(docSinkModifyItem);
			
			return (Item item) -> {
				currentSourceItem = item;
				modifiedConsumer.accept(item.getDocument());
			};
		}					

	}
	
	/**
	 * Validate against a schema in interceptor format, so it can be chained in.
	 * Logs the first three invalid to the dataset log and every invalid to the normal programm log.
	 * 
	 * @author arne
	 *
	 */
	public static class ValidateInterceptor implements Interceptor<Item, Item> {
		final ReportErrorHandler rh = new ReportErrorHandler();
		final Dataset dataset;
		final static Logger log = Logger.getLogger(ValidateInterceptor.class );
		
		int logLimit = 3;
		
		/**
		 * The Interceptor needs to know the dataset for logging and to find the schema
		 * @param ds
		 */
		public ValidateInterceptor( Dataset ds ) {
			this.dataset = ds;
		}
		
		@Override
		public ThrowingConsumer<Item> intercept(ThrowingConsumer<Item> nestedConsumer) {
			
			if( dataset.getSchema() == null ) return nestedConsumer;
			
			return item -> {
				String itemXml = item.getXml();
				if(!itemXml.startsWith("<?xml")) itemXml = "<?xml version=\"1.0\"  standalone=\"yes\"?>" + item.getXml();
				
				rh.reset();
				SchemaValidator.validate(itemXml, dataset.getSchema(), rh );
				
				item.setValid( rh.isValid() );

				if( !rh.isValid() && (logLimit>0 )) {
					dataset.logEvent( "Invalid item " + item.getLabel(), rh.getReportMessage() );
					logLimit--;
				}
				log.debug( "Item: " + item.getLabel() + "\n" + rh.getReportMessage() );
				nestedConsumer.accept(item);
			};
		}
	}
	
	public static Interceptor<Item, Item> createXslInterceptor( String xsl ) throws Exception {
			XSLTransform transform = new XSLTransform();
			transform.setXSL(xsl);

			return Interceptor.modifyInterceptor( (Item item) -> {
				String outputXml = transform.transform(item.getXml());
				item.setXml(outputXml);
			});
	}
	
	public static Interceptor<Item, Item> createXslInterceptor( String projectPath, Map<String, String> parameters ) throws Exception {
		XSLTransform transform = new XSLTransform();
		String fileContent = IOUtils.resourceToString(projectPath, Charset.forName("UTF-8"));
		transform.setXSL(fileContent);
		transform.setParameters(parameters);

		return Interceptor.modifyInterceptor( (Item item) -> {
			String outputXml = transform.transform(item.getXml());
			item.setXml(outputXml);
		});
	}
	
	public static Interceptor<Item, Item> thesaurusEdmSkosInterceptor( String prefix, String thesaurus ) throws Exception {
		Map<String, String> params = new HashMap<>();
		params.put( "thesaurusRdf", thesaurus);
		params.put( "thesaurusPattern", prefix );
		return createXslInterceptor( "xsl/edm_enrich_thesuarus.xsl", params );
	}

	/*
	 * This interceptor is Dataset specific, however, if all the datasets you want to use the mapping on
	 * share namespaces and prefixes, it MIGHT work on many.
	 */
	public static Interceptor<Document, Document> interceptorFromMapping( Mapping m, Dataset parentDataset ) throws Exception  {
			XSLTGenerator xslt = new XSLTGenerator();
		
			xslt.setItemXPath( parentDataset.getItemRootXpath().getXpathWithPrefix(true));
			xslt.setImportNamespaces( parentDataset.getRootHolder().getNamespaces(true));
			xslt.setOption(XSLTGenerator.OPTION_OMIT_XML_DECLARATION, true);
			
			Mappings mappings = m.getMappings();
			String xsl = (mappings == null)?
					m.getXsl():
					XMLFormatter.format(xslt.generateFromMappings(mappings));
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
	}

}
