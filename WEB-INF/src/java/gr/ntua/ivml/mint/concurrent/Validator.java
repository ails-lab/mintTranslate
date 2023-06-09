package gr.ntua.ivml.mint.concurrent;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

import javax.xml.transform.stream.StreamSource;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.ocpsoft.prettytime.PrettyTime;

import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Item;
import gr.ntua.ivml.mint.persistent.XmlSchema;
import gr.ntua.ivml.mint.util.ApplyI;
import gr.ntua.ivml.mint.util.StringUtils;
import gr.ntua.ivml.mint.xsd.ReportErrorHandler;
import gr.ntua.ivml.mint.xsd.SchemaValidator;


public class Validator implements Runnable {
	private Dataset dataset;
	private int validCounter=0, invalidCounter=0, itemCounter = 0;
	private TarArchiveOutputStream validOutput = null;
	private TarArchiveOutputStream invalidOutput = null;
	private File validOutputFile = null;
	private File invalidOutputFile = null;
	private boolean stopOnFirstInvalid = false;
	private XmlSchema schema = null;
	
	public static final Logger log = Logger.getLogger( Validator.class );
	
	/**
	 * Validates against dataset's target schema
	 * @param ds
	 */
	public Validator( Dataset ds ) {
		this.dataset = ds;
		this.schema = this.dataset.getSchema();
	}
	
	/**
	 * Validates dataset against a schema
	 * @param ds
	 * @param schema
	 */
	public Validator( Dataset ds, XmlSchema schema ) {
		this.dataset = ds;
		this.schema = schema;
	}
	
	public void runInThread() {
		
		try {
			if( this.schema == null ) {
				dataset.logEvent( "No schema set to validate." );
				return;
			}
			dataset.setSchemaStatus(Dataset.SCHEMA_RUNNING);
			dataset.logEvent("Validation started." );
			DB.commit();
			
			// if there are items, validate items
			if( dataset.getItemizerStatus().equals(Dataset.ITEMS_OK)) 
				checkItemSchema(); 
			// else validate files
			else if(dataset.getLoadingStatus().equals(Dataset.LOADING_OK)) {
				checkEntrySchema();
			}

			if( dataset.getSchemaStatus().equals(Dataset.SCHEMA_RUNNING)) {
				dataset.setSchemaStatus( Dataset.SCHEMA_OK );			
			}
			
			
			// collect the results
			DB.commit();
		} catch( Exception e ) {
			dataset.setSchemaStatus(Dataset.SCHEMA_FAILED); 
			dataset.logEvent("Validation failed. " + e.getMessage(), StringUtils.stackTrace(e, null));
			log.error( "Validation failed.", e );
			DB.commit();
		} 
	}
	
	
	public void run() {
		try {
			DB.getSession().beginTransaction();
			dataset = DB.getDatasetDAO().getById(dataset.getDbID(), false);
			schema = DB.getXmlSchemaDAO().getById( schema.getDbID(), false);
			runInThread();
		} finally {
			DB.closeSession();
			DB.closeStatelessSession();
		}
	}

	
	private void checkItemSchema() throws Exception {
		final ReportErrorHandler rh = new ReportErrorHandler();
		final int totalInputItems = dataset.getItemCount();
		final long startTime = System.currentTimeMillis();

		ApplyI<Item> itemProcessor = new ApplyI<Item>() {
			@Override
			public void apply(Item item) throws Exception {
				Thread.sleep(0);
				String itemXml = item.getXml();
				if(!itemXml.startsWith("<?xml")) itemXml = "<?xml version=\"1.0\"  standalone=\"yes\"?>" + item.getXml();
				
				// here we should use the SchemaValidator
				rh.reset();
				SchemaValidator.validate(itemXml, schema, rh );
				if( rh.isValid()) {
					// this needs a state for the items so the changed item is
					// committed back
					item.setValid( true );
					collectValid(item);
					validCounter++;
				} else {
					item.setValid(false);
					if(isStopOnFirstInvalid()) {
						throw new Exception("Invalid item. Schema validation failed");
					}
					invalidCounter++;
					collectInvalid(item, rh.getReportMessage() );
					if( invalidCounter < 4 ) {
						dataset.logEvent( "Invalid item " + item.getLabel(), rh.getReportMessage() );
					}
					log.debug( "Item: " + item.getLabel() + "\n" + rh.getReportMessage() );
				}

				itemCounter++;
				if( totalInputItems > 2000 ) {
					if( itemCounter == (totalInputItems/50)) {
						long usedTime = System.currentTimeMillis()-startTime;
						PrettyTime pt = new PrettyTime();
						
						String expected = pt.format( new Date( System.currentTimeMillis() + 49*usedTime )); 
						dataset.logEvent( "Expect validation finished " + expected );
						DB.commit();
					}
				}

			}
		};
		dataset.processAllItems(itemProcessor, true );
		if( invalidOutput != null ) invalidOutput.close();
		if( validOutput != null ) validOutput.close();
		dataset.setValidItemCount(validCounter);
		if( validCounter == 0 ) {
			throw new Exception( "No item was validated." );
		}
		dataset.logEvent("Validation finished. ", validCounter + " Items valid, " + invalidCounter + " Items invalid." );
	}
	
	private void checkEntrySchema() throws Exception {
		final ReportErrorHandler rh = new ReportErrorHandler();
		EntryProcessorI ep = new EntryProcessorI( ) {
			public void  processEntry(String entryName, InputStream is) throws Exception {
				if( !entryName.endsWith(".xml") &&  !entryName.endsWith(".XML")) return;
				// makes this process interruptible
				Thread.sleep(0);
				rh.reset();
				// need to copy into a mem to run two courses of schema checks
				byte[] buf = IOUtils.toByteArray(is);
				ByteArrayInputStream bis = new ByteArrayInputStream(buf);
				StreamSource ins = new StreamSource();
				ins.setInputStream(bis);
				SchemaValidator.validateSchematron(ins, schema, rh);
				
				bis.reset();
				ins = new StreamSource();
				ins.setInputStream(bis);
				SchemaValidator.validateXSD(ins, schema, rh);
				
				if( rh.isValid() ) {
					validCounter++;
				} else {
					if(isStopOnFirstInvalid()) {
						throw new Exception("Invalid item. Schema validation failed");
					}
					invalidCounter++;
					if( invalidCounter < 10 ) {
						dataset.logEvent("Entry " + entryName + " didn't validate.",rh.getReportMessage());
					}
				}
			}
		};
		dataset.processAllEntries(ep);
		if( invalidCounter > 0 ) {
			throw new Exception( invalidCounter + " entries failed validation." );
		}
		dataset.logEvent("Validation finished. ", validCounter + " entries valid, " + invalidCounter + " entries invalid." );
	}
	
	
	
	/**
	 * Return the Stream, with the entry already put.
	 * @param item
	 * @return
	 */
	private TarArchiveOutputStream getValidTgz(  ) {
		if( validOutput == null ) {
			try {
				validOutputFile = File.createTempFile("ValidItems", ".tgz" );
				log.info( "Logging valid to " + validOutputFile.getAbsolutePath());
				GzipCompressorOutputStream gz = new GzipCompressorOutputStream(
						new FileOutputStream(validOutputFile));
				validOutput = new TarArchiveOutputStream(gz);
				validOutput.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
				validOutput.putArchiveEntry(new TarArchiveEntry(datasetSubdir(dataset)+"/"));
				validOutput.closeArchiveEntry();
			} catch( Exception e ) {
				log.error( "Cannot create valid output File.", e );
			}
		}
		return validOutput;
	}
	

	private TarArchiveOutputStream getInvalidTgz(  ) {
		if( invalidOutput == null ) {
			try {
				invalidOutputFile = File.createTempFile("InvalidItems", ".tgz" );
				log.info( "Logging invalid to " + invalidOutputFile.getAbsolutePath());
				GzipCompressorOutputStream gz = new GzipCompressorOutputStream(
						new FileOutputStream(invalidOutputFile));
				invalidOutput = new TarArchiveOutputStream(gz);
				invalidOutput.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
				invalidOutput.putArchiveEntry(new TarArchiveEntry(datasetSubdir(dataset)+"/"));
				invalidOutput.closeArchiveEntry();
			} catch( Exception e ) {
				log.error( "Cannot create invalid output File.", e );
			}
		}
		return invalidOutput;
	}

	
	
	/**
	 * Write into the tgz archive for valid items.
	 * @param item
	 */
	private void collectValid( Item item ) {
		TarArchiveOutputStream tos = getValidTgz();
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			String itemXml = item.getXml();
			if(!itemXml.startsWith("<?xml")) itemXml = "<?xml version=\"1.0\"  encoding=\"UTF-8\" ?>\n" + item.getXml();
			IOUtils.write(itemXml, baos, "UTF-8" );
			baos.close();
			
			write( baos.toByteArray(), datasetSubdir(dataset)+"/Item_" + item.getDbID() + ".xml", tos );
		} catch( Exception e ) {
			log.error( "Failed to collect valid item ", e );
		}
	}
	
	/**
	 * Write into the tgz archive for valid items.
	 * @param item
	 */
	private void collectInvalid( Item item, String msg ) {
		TarArchiveOutputStream tos = getInvalidTgz();
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			String itemXml = item.getXml();
			if(!itemXml.startsWith("<?xml")) itemXml = "<?xml version=\"1.0\"  encoding=\"UTF-8\"?>\n" + item.getXml();
			IOUtils.write(itemXml, baos, "UTF-8" );
			baos.close();
			
			write( baos.toByteArray(),  datasetSubdir(dataset)+"/Item_" + item.getDbID() + ".xml", tos );
			baos.reset();

			IOUtils.write(msg, baos, "UTF-8" );
			baos.close();
			write( baos.toByteArray(),  datasetSubdir(dataset)+"/Item_" + item.getDbID() + ".err", tos );
			
		} catch( Exception e ) {
			log.error( "Failed to collect invalid item ", e );
		}	
	}

	private String datasetSubdir( Dataset ds ) {
		return String.format("%1$ty-%1$tm-%1$td_%1$tH:%1$tM:%1$tS", ds.getCreated());
	}
	
	private void write( byte[] data, String name, TarArchiveOutputStream tos ) throws IOException  {
		TarArchiveEntry entry = new TarArchiveEntry(name);
		entry.setSize((long) data.length );
		tos.putArchiveEntry(entry);
		IOUtils.write(data, tos);
		tos.closeArchiveEntry();
	}
	
	public void clean() {
		if( validOutputFile != null) validOutputFile.delete();
		if( invalidOutputFile != null ) invalidOutputFile.delete();
	}
	
	public File getValidItemOutputFile() {
		return validOutputFile;
	}
	
	public File getInvalidItemOutputFile() {
		return invalidOutputFile;
	}

	public void setStopOnFirstInvalid(boolean stopOnFirstInvalid) {
		this.stopOnFirstInvalid = stopOnFirstInvalid;
	}

	public boolean isStopOnFirstInvalid() {
		return stopOnFirstInvalid;
	}
}
