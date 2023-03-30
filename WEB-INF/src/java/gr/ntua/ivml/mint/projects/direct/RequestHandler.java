package gr.ntua.ivml.mint.projects.direct;

import static gr.ntua.ivml.mint.api.RequestHandler.errJson;
import static gr.ntua.ivml.mint.api.RequestHandler.getPathInt;
import static gr.ntua.ivml.mint.api.RequestHandler.okJson;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;

import gr.ntua.ivml.mint.api.RequestHandlerUtil;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.f.ErrorCondition;
import gr.ntua.ivml.mint.f.ThrowingConsumer;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.PublicationRecord;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.util.Jackson;

/**
 * Static messages routed here by the router servlet, starting with /api/direct/...
 * @author stabenau
 *
 */
public class RequestHandler {
	
	public static final Logger log=Logger.getLogger(RequestHandler.class);
	
	public static class Option {
		public String project = "direct";
		public String category = "publication";
		public String label;
		public boolean inProgress = false;
		public String url;
		
		// json|htmlNewPanel|htmlReplacePanel
		public String response = "json";
	};

	public static class DirectEntry {
		public static final SimpleDateFormat dateFormat = new SimpleDateFormat( "dd/MM/yy HH:mm:ss" );
		public DirectEntry( PublicationRecord pr, String urlFormat ) {
			Dataset ds = pr.getPublishedDataset();
			originalDatasetId = ds.getOrigin().getDbID();
			organizationId = ds.getOrganization().getDbID();
			datasetId = ds.getDbID();
			itemCount = ds.getItemCount();
			name = ds.getName();
			originalName = ds.getOrigin().getName();
			organizationName = ds.getOrganization().getEnglishName();
			if( ds.getSchema() != null ) {
				schemaName = ds.getSchema().getName();
			}
			url = String.format( urlFormat, ds.getDbID());
			availableSince = dateFormat.format( pr.getStartDate());
			
		}
		
		public long originalDatasetId;
		public long organizationId;
		public long datasetId;
		public int itemCount;
		public String name;
		public String originalName;
		public String organizationName;
		public String schemaName;
		public String url;
		public String availableSince;
	}
	
	
	// TODO: These should be post requests, to be done much later
	
	// this one should be quick enough to not go to Queue
	public static void unpublishGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			ErrorCondition err = new ErrorCondition();
			Dataset ds = RequestHandlerUtil.accessDataset( request, response, "direct", err );

			// remove the Publication record, we dont care if its successful or not
			// In theory we should check if its running though
			err.onValid(() -> {
				Optional<PublicationRecord> oPr = DB.getPublicationRecordDAO().getByPublishedDatasetTarget( ds, "direct" ); 
				if( oPr.isPresent() )
					DB.getPublicationRecordDAO().makeTransient(oPr.get());
				okJson( "msg", "Dataset removed from direct access", response );
			} );
			
			err.onFailed(()-> errJson( err.get(), response ));
		} catch( Exception e ) {
			log.error( "", e );
			errJson( "Something went wrong: '" + e.getMessage() + "'", response );
			return ;
		}
	}

	
	public static void publishGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			ErrorCondition err = new ErrorCondition();
			Dataset ds = RequestHandlerUtil.accessDataset( request, response, "direct", err );
			
			Optional<PublicationRecord> oPr = err.grab( ()-> 
				DB.getPublicationRecordDAO().getByPublishedDatasetTarget( ds, "direct" )
				, "DB access failed");
			
			err.check( ()-> !oPr.isPresent(), "This dataset is already directly accessible.");

			err.onValid( ()-> {
				// Which dataset to publish
				// No magic on this one. Publish this if it is allowed, magic somewhere else (which dataset to publish)
				
				// Publish takes some time, so we need to queue it
				// but we should mark it as started
				PublicationRecord pr = new PublicationRecord();
	
				pr.setStartDate(new Date());
				pr.setEndDate( new Date());
				pr.setPublisher((User)request.getAttribute( "user" ));
				pr.setOriginalDataset(ds.getOrigin());
				pr.setPublishedDataset(ds );
				pr.setStatus(Dataset.PUBLICATION_OK);
				pr.setOrganization(ds.getOrganization());
				pr.setTarget("direct");
				DB.getPublicationRecordDAO().makePersistent(pr);
				DB.commit();
				ds.logEvent( "Dataset directly accessible");
				okJson( "msg", "Dataset accessible via api/direct/download/"+ds.getDbID(), response );
			} );

			err.onFailed(()-> errJson( err.get(), response ));

		} catch( Exception e ) {
			log.error( "", e );
			errJson( "Something went wrong: '" + e.getMessage() + "'", response );
			return ;
		}
	}
	
	public static void downloadGet(HttpServletRequest request, HttpServletResponse response) throws IOException {

		try {
			ErrorCondition err = new ErrorCondition();

			Integer datasetId = err.grab(()->getPathInt( request, 3 ), "Invalid dataset id."); 
			Dataset ds = err.grab( ()->DB.getDatasetDAO().getById((long) datasetId, false ), "Unknown Dataset" ); 
			err.check( ()-> 
				DB.getPublicationRecordDAO().getByPublishedDatasetTarget(ds, "direct").isPresent()
				, "No direct access to this dataset" );
			
			
			ThrowingConsumer<OutputStream> dataProducer = err.grab( () -> ( os  -> {
				try (
						GzipCompressorOutputStream gzos = new org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream( 
								os );

						TarArchiveOutputStream tos = new TarArchiveOutputStream( gzos );

						)
				{
					
					tos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
					tos.putArchiveEntry(new TarArchiveEntry(ds.getDbID()+"/"));
					tos.closeArchiveEntry();
	
					ds.processAllItems( item ->
					{ 
						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						String itemXml = item.getUtf8Xml();
						IOUtils.write(itemXml, baos, "UTF-8");
						baos.close();
	
						TarArchiveEntry entry = new TarArchiveEntry(ds.getDbID()+"/Item_" + item.getDbID() + ".xml");
						byte[] data = baos.toByteArray();
						entry.setSize( data.length );
						tos.putArchiveEntry(entry);
						tos.write(data, 0, data.length);
						tos.closeArchiveEntry();
					}, false);
					tos.finish();
				} catch( Exception e ) {
					log.error( "",e );
				}
			} ), "Cannot create downloader (Shouldn't happen)" );
			err.onValid( ()-> {
				RequestHandlerUtil.streamDownload("application/x-gtar ", ds.getDbID()+".tgz", dataProducer, response);
			} );
			
			err.onFailed(()-> errJson( err.get(), response ));

		} catch( Exception e ) {
			log.error( "", e );
			errJson( "Something went wrong: '" + e.getMessage() + "'", response );
			return ;
		}

	}
	
	/**
	 * parameters should be start and count for sorted by date desc. 
	 * provide a datasetId for just one response
	 * @param request
	 * @param response
	 * @throws IOException
	 */
	public static void listGet(HttpServletRequest request, HttpServletResponse response) throws IOException {

		try {
			int start = Integer.parseInt( 
					gr.ntua.ivml.mint.api.RequestHandlerUtil.getUniqueParameter(request, "start").orElse("0"));
			int count = Integer.parseInt(
					gr.ntua.ivml.mint.api.RequestHandlerUtil.getUniqueParameter(request, "count").orElse("10"));
			int datasetId = Integer.parseInt(
					gr.ntua.ivml.mint.api.RequestHandlerUtil.getUniqueParameter(request, "datasetId").orElse("-1"));
			
			List<PublicationRecord> directPubs;
			if( datasetId > 0 ) {
				Dataset ds = DB.getDatasetDAO().findById((long)datasetId , false);
				
				Optional<PublicationRecord> prOpt = DB.getPublicationRecordDAO().getByPublishedDataset(ds);
				directPubs = prOpt.map( pr -> Arrays.asList( pr )).orElse(Collections.emptyList());
			} else {
				directPubs = DB.getPublicationRecordDAO().pageByTargetDateDesc("direct", start, count );
			}
			
			// this should be a good guess for the URL to use to download a dataset
			String baseUrl = request.getRequestURL().toString().replaceAll("/list[?&]?$", "/download/");
			
			List<DirectEntry> res = directPubs.stream()
				.map( pr-> new DirectEntry( pr, baseUrl+"%d" ))
				.collect( Collectors.toList());
			
			okJson( Jackson.om().valueToTree( res ), response );
		} catch( Exception e ) {
			errJson( "Something went wrong: '" + e.getMessage() + "'", response );
			log.error( "",e );
		}
	}

	public static ArrayNode datasetPublishOptions( Dataset ds, User user ) {
		ArrayNode result = Jackson.om().createArrayNode();
		
		List<String> projects = Arrays.asList( ds.getOrigin().getProjectNames());

		// maybe nothing to do here, although we only get here if there is?
		if( !projects.contains("direct")) return result;
		
		
		Optional<PublicationRecord> oPr = DB.getPublicationRecordDAO().getByPublishedDatasetTarget( ds, "direct" ); 
		Option option = new Option();
	
		if( oPr.isPresent()) {
			// show is running, needs cleaning or unpublish
			option.label = "Remove from direct Access";
			option.url = "api/direct/unpublish/"+ds.getDbID();
			result.add( Jackson.om().valueToTree(option));
		} else {
			if(( Dataset.ITEMS_OK.equals( ds.getItemizerStatus())  ) && 
					(ds.getItemCount() > 0 )) {
				option.label = "Enable direct Access";
				option.url = "api/direct/publish/"+ds.getDbID();
				result.add( Jackson.om().valueToTree(option));
			}
		}
		return result;
	}
}
