package gr.ntua.ivml.mint.api;

import java.io.File;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.TypeFactory;

import gr.ntua.ivml.mint.concurrent.Itemizer;
import gr.ntua.ivml.mint.concurrent.Queues;
import gr.ntua.ivml.mint.concurrent.SchemaStatsBuilder;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.DataUpload;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Organization;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.persistent.XmlSchema;
import gr.ntua.ivml.mint.persistent.XpathHolder;
import gr.ntua.ivml.mint.util.ApplyI;
import gr.ntua.ivml.mint.xml.util.XPathUtils;

/**
 * 
 * @author Arne Stabenau
 * This class imports a Dataset that comes from old Mint instances in a very specific way.
 * It wont need to do everything the Uploadindexer does, so hopefully its easier to write.
 * 
 */
public class DatasetImportHandler implements Runnable, ApplyI<InputStream> {

	Logger log = Logger.getLogger( DatasetImportHandler.class );
	
	// paths, schema, org, original created date, in here
	public ObjectNode datasetMetadata;
	
	// where the imported data goes first
	public File uploadedTgz;
	
	public long userId;
	
	
	public DatasetImportHandler( ObjectNode meta, long userId ) {
		this.datasetMetadata = meta;
		this.userId = userId; 
	}
	
	// called when the data is transferred 
	public void apply( InputStream is ) {
		// store to a file and initiate (maybe via queue) the processing of said data
		try {
			uploadedTgz = File.createTempFile("OldMintTransfer", "tgz");
			FileUtils.copyInputStreamToFile( is, uploadedTgz);
			
			Queues.queue( this, "db");
		} catch( Exception e ) {
			log.error( "", e );
		}
	}
	
	// started from Queue
	public void run() {
		// process the uploaded file
		DB.getSession().beginTransaction();

		try {
			User u = DB.getUserDAO().getById(userId, false );
			if( !u.can( "change data", u.getOrganization() )) { 
				log.error("User #" + u.getDbID() + " attempted illegal data upload.");
				throw new Exception("User cannot upload");
			}
			

			DataUpload du = new DataUpload();
			du.init( u );

			// check and add org
			if( datasetMetadata.has("organizationId")) {
				Organization o = DB.getOrganizationDAO()
						.getById(datasetMetadata.get( "organizationId").asLong(), false);
				if( u.can( "change data", o ))
					du.setOrganization(o);
				else 
					throw new Exception( "Illegal upload organization" );
			}
			
			if( datasetMetadata.has("schemaId")) {
				XmlSchema schema = DB.getXmlSchemaDAO()
						.getById( datasetMetadata.get( "schemaId").asLong(), false);
				if( schema != null ) du.setSchema(schema);
			}
			
			if( datasetMetadata.has( "project")) {
				String project = datasetMetadata.get("project").asText();
				// TODO: need to check if folder is acceptable for organization
				// and add the correct color code to the end
				du.addFolder(project + "_#a00000");
			}
			
	    	du.setLoadingStatus(Dataset.LOADING_HARVEST);

			Date created = new Date( datasetMetadata.get( "created" ).asLong());			
			du.setCreated(created);
			if( datasetMetadata.has("name"))
				du.setName( datasetMetadata.get( "name" ).asText());
			du.setStructuralFormat(DataUpload.FORMAT_TGZ_XML);
			du.setUploadMethod(DataUpload.METHOD_SERVER);
			
			DB.getDataUploadDAO().makePersistent(du);
			
			du.uploadFile(uploadedTgz);
			du.setLoadingStatus(Dataset.LOADING_OK);
			SchemaStatsBuilder ssb = new SchemaStatsBuilder(du);
			ssb.runInThread();

			// now we need to set the xpathholders
			// find the xpath holders ...
			Map<String, String> externalPrefixes = getPrefixes();
			
			du.setItemLabelXpath(
					byPath(XPathUtils.expandNamespaces( 
							datasetMetadata.get( "labelPath" ).asText(), 
							externalPrefixes ), du ));
			du.setItemNativeIdXpath(
					byPath(XPathUtils.expandNamespaces( 
							datasetMetadata.get( "idPath" ).asText(), 
							externalPrefixes ), du ));
			du.setItemRootXpath(
					byPath(XPathUtils.expandNamespaces( 
							datasetMetadata.get( "rootPath" ).asText(), 
							externalPrefixes ), du ));
			
			DB.getDataUploadDAO().makePersistent(du);

			// and now the itemizer, solarizer etc.
			Itemizer itemizer = new Itemizer( du );
			itemizer.runInThread();

			// Only missing, if there is a schema set, we need to validate the items.
			log.info( "Not implemented the rest yet. Upload id #" + du.getDbID());
		} catch( Exception e ) {
			log.error( "", e );
		} finally {
			if( uploadedTgz != null ) uploadedTgz.delete();
			DB.closeSession();
			DB.closeStatelessSession();
		}
	}
	
	private XpathHolder byPath( String completePath, Dataset ds ) {
		String localPath = completePath.replaceAll("\\{[^}]+\\}:", "" );
		
		// get a list of XpathHolders with same localname path		
		List<XpathHolder> holders = DB.getXpathHolderDAO().getByPath(ds, localPath );
		if( holders.size() == 0 ) return null;
		if( holders.size() == 1 ) return holders.get( 0 );
		// otherwise expand every path and compare with input 
		for( XpathHolder path: holders ) {
			if( path.getFullPath().equals( completePath))
				return path;
		}
		return null;
	}
	
	private Map<String,String> getPrefixes() {
		Map<String, String> result = new ObjectMapper().convertValue(
				 datasetMetadata.get( "namespaces"), 
				 TypeFactory.defaultInstance().constructMapType( HashMap.class, String.class, String.class));
		return result;
	}
	
}
