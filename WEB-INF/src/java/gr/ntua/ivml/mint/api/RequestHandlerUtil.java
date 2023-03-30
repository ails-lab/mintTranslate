package gr.ntua.ivml.mint.api;

import static gr.ntua.ivml.mint.api.RequestHandler.errJson;
import static gr.ntua.ivml.mint.api.RequestHandler.getPathInt;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.log4j.Logger;

import gr.ntua.ivml.mint.api.RouterServlet.Handler;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.f.ErrorCondition;
import gr.ntua.ivml.mint.f.ThrowingConsumer;
import gr.ntua.ivml.mint.f.ThrowingSupplier;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Project;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.persistent.XpathHolder;
import gr.ntua.ivml.mint.persistent.XpathStatsValues.ValueStat;
import gr.ntua.ivml.mint.util.StringUtils;
 
/**
 * Static methods for use in Request processing operation
 * @author arne
 *
 */
public class RequestHandlerUtil {

	public static Logger log = Logger.getLogger(RequestHandlerUtil.class);

	// execute the supply method and throw if it returns null or throws itself
	public static <T> T grab( ThrowingSupplier<T> supply, String failureMsg ) throws ParameterException {
		try {
			T tmp = supply.get();
			if( tmp != null ) 
				return tmp;
		} catch( ParameterException pe) {
			// dont extra wrap parameterexception
			throw pe;
		} catch( Throwable t ) {
			throw new ParameterException( failureMsg, t );
		}
		throw new ParameterException( failureMsg );
	}
	
	// execute the supply method and throw if it returns null or throws itself
	public static void check( ThrowingSupplier<Boolean> tester, String failureMsg ) throws ParameterException {
		try {
			if( tester.get()) return;
		} catch( ParameterException pe) {
			throw pe;
		} catch( Throwable t ) {
			throw new ParameterException( failureMsg, t );
		}
		throw new ParameterException( failureMsg );
	}
	
	// standard json response for throwing handlers wrapped around the handler
	public static Handler errorWrap( Handler inputHandler ) {
		return (request, response ) -> {
			try {
				inputHandler.handle(request, response);
			} catch( ParameterException pe ) {
				errJson( pe.getMessage(), response );
				if( pe.getCause() != null ) {
					log.error( "Exception in parameter processing ", pe.getCause());
				}
				return;
			} catch( Throwable th ) {
				errJson( StringUtils.filteredStackTrace(th, "gr.ntua.ivml.mint").toString(), response );
				log.error( "Exception in parameter processing ", th );
			}
		};
	}
	
	
	
	// get the dataset with id in path
	// Problems will be in ParameterException (Login, exists, rights, malformed url )
	public static Dataset accessDataset( HttpServletRequest request, HttpServletResponse response, String project, int pathPositionOfDatasetNumber ) throws ParameterException {
		
		// err does nothing once it has an error condition. grabs return null
		Integer datasetId = grab(()->getPathInt( request, pathPositionOfDatasetNumber ), "Invalid dataset id."); 
		Dataset ds = grab( ()->DB.getDatasetDAO().getById((long) datasetId, false ), "Unknown Dataset" ); 
		User u = grab(()-> (User) request.getAttribute("user"), "No User logged in");

		Project p = grab(()-> DB.getProjectDAO().findByName( project ), "'" + project + "' project not present in db" );
		if( ! p.hasProject(ds.getOrigin()))
			throw new ParameterException( "Dataset not in " + project );
		
		if( !(u.can( "change data", ds.getOrganization() ) || p.hasProject(u)))
			throw new ParameterException( "User has no access");

		return ds;
	}

	// get the dataset in position 3 of request path,
	// if dataset is null, response already contains errcode and mesg
	public static Dataset accessDataset( HttpServletRequest request, HttpServletResponse response, String project,  ErrorCondition err ) throws Exception {
		
		// err does nothing once it has an error condition. grabs return null
		Integer datasetId = err.grab(()->getPathInt( request, 3 ), "Invalid dataset id."); 
		Dataset ds = err.grab( ()->DB.getDatasetDAO().getById((long) datasetId, false ), "Unknown Dataset" ); 
		User u = err.grab(()-> (User) request.getAttribute("user"), "No User logged in");

		Project p = err.grab(()-> DB.getProjectDAO().findByName( project ), "'" + project + "' project not present in db" );
		err.check(() -> p.hasProject(ds.getOrigin()), "Dataset not in " + project );
		
		err.check( ()-> u.can( "change data", ds.getOrganization() ) || p.hasProject(u), "User has no access");

		return err.grab(()->ds,"");
	}
		
	
	
	/**
	 * Copy output stream into tmp file and then copy to response with given content type and filename.
	 * Optionally compress the stream. Data producer needs to close the stream!
	 * @param contentType
	 * @param fileName
	 * @param dataProducer
	 * @param compressOutput
	 * @param response
	 */
	public static void download( String contentType, String fileName, 
			ThrowingConsumer<OutputStream> dataProducer, boolean compressOutput,
			HttpServletResponse response ) {
		try {
			File tmpFile = File.createTempFile( "forDownload","" );
			FileOutputStream fos = new FileOutputStream( tmpFile );

			FilterOutputStream myos = new FilterOutputStream( fos ) {
				public void close() throws IOException {
					out.flush();
					out.close();
					// copy the file to client
					if( tmpFile.length() > 0 ) {
						response.setStatus(HttpServletResponse.SC_OK);
						response.setContentType( contentType );
						response.setHeader("Content-Disposition", "attachment; filename=" + fileName);
						
						// copy the file to the response
						if( compressOutput ) {
							OutputStream os = new org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream( response.getOutputStream());
							IOUtils.copy( new FileInputStream( tmpFile ), os);
							os.flush();
							os.close();
						} else {
							IOUtils.copy( new FileInputStream( tmpFile ), response.getOutputStream());
							response.getOutputStream().flush();
							response.getOutputStream().close();
						}
						// still no error
						tmpFile.delete();
					} else {
						errJson( "Output problem", response );
					}
				}
			};
			
			dataProducer.accept(myos);
		} catch( Exception e ) {
			log.error( "Download data problem", e );
			errJson( "Problem downloading data", response );
		}
	}
	
	// STream output stream into resonse with given fileanme and content type
	public static void streamDownload( String contentType, String fileName, 
			ThrowingConsumer<OutputStream> dataProducer,
			HttpServletResponse response ) {
		try( OutputStream os = response.getOutputStream()) {
			response.setStatus(HttpServletResponse.SC_OK);
			response.setContentType( contentType );
			response.setHeader("Content-Disposition", "attachment; filename=" + fileName);

			dataProducer.accept( os );
			os.flush();
		} catch( Exception e ) {
			log.error( "", e );
		}
 	}

	// download given holders values in csv format
	public static void downloadValues( XpathHolder holder, HttpServletResponse response ) {
		String name = holder
				.getFullPath()
				.replaceAll("/text()", "")
				.replaceAll( "[^0-9a-zA-Z]", "_")
				// remove trailing underscores
				.replaceAll( "_*$","");
		
		if( name.length()>20) name = name.substring( name.length()-20);
		final XpathHolder holderFinal = (holder.getTextNode() != null)?holder.getTextNode():holder;

		String fileName = "dataset_#"+holder.getDataset().getDbID()+ "_"+name+".csv.gz";
		ThrowingConsumer<OutputStream> csvSupply = outputStream -> {
			int start =0;
			while( true ) {
				List<ValueStat> values = holderFinal.getValues(start, 100);
				Writer w = new OutputStreamWriter( outputStream, "UTF-8");
				if( start==0 && values.size()>0 ) {
					w.write( CSVFormat.DEFAULT.format( "Value", "Count") + "\n" );
				}
				for( ValueStat v: values ) {
					w.write( CSVFormat.DEFAULT.format(v.value, v.count ) +"\n");
				}
				log.debug( "Start = " + start + "ValueSize: " + values.size());
				start += 100;
				w.flush();
				if( values.size() == 0 ) {
					w.close();
					break;
				}
			}
		};
		download( "application/gzip", fileName, csvSupply, true, response );
	}

	// returns value if unique otherwise empty
	public static Optional<String> getUniqueParameter( HttpServletRequest request, String name) {
		String[] values = request.getParameterValues(name);
		if( values == null ) return Optional.empty();
		if( values.length > 1 ) return Optional.empty();
		return Optional.of( values[0]);
	}
	
	public static Optional<Long> getUniqueNumberParameter(  HttpServletRequest request, String name) {
		try {
			return getUniqueParameter(request, name).map( s-> (long) Integer.parseInt(s));
		} catch( NumberFormatException e ) {
			throw new ParameterException( name + " is not a number.");
		}
	}
}
