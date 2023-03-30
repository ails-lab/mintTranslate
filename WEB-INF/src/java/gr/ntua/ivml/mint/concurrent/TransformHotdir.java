package gr.ntua.ivml.mint.concurrent;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Item;
import gr.ntua.ivml.mint.persistent.Transformation;
import gr.ntua.ivml.mint.util.ApplyI;

/**
 * In this class, hotdir is managed. A hotdir is the directory that mirrors tarballs from
 * all transformations that are in the system. All exports are done (and synchronized) through
 * this class. 
 * 
 * Hotdir will create subdirs with orgname-[id] and tarballs with
 * uploadname-[id].tgz
 * 
 * Inside the tarball, there is a valid subdir with items that were found valid during
 * transformation and invalid with items that were invalid. The item filename inside the
 * tarball is id.xml.
 * 
 * @author Arne Stabenau
 *
 */
public class TransformHotdir implements Runnable {
	
	public static final Logger log = Logger.getLogger( TransformHotdir.class );
	private File basedir;
	private File inProgress;
	
	public String working = null;
	public static class Job {
		public Job( int id, String deletePath ) {
			this.dsId = id;
			this.deletePath = deletePath;
		}
		
		public int dsId;
		public String deletePath;
	}
	
	public Deque<Job> queue = new LinkedList<Job>();
	
	
	/**
	 * Create an instance that exports to given dir. 
	 * If dir doesn't exists it will be created, if it cant be created, the instance should 
	 * crash.
	 * @param dir
	 */
	public TransformHotdir( String dir ) throws Exception {
		basedir = new File( dir );
		FileUtils.forceMkdir(basedir);
		inProgress = new File( basedir, "inProgress");
	}
	
	/**
	 * Remove export with given id. The Dataset is probably already removed from the DB.
	 * The path is optional, but if its known at call time, you can safe time with searching the file to delete.
	 * @param datasetId
	 */
	public synchronized void removeDs( int datasetId, String path ) {
		if( path == null ) path = findPath( datasetId );
		if( path != null ) {
			Job j = new Job( datasetId, path );
			queue.add(j);
			// maybe start the worker
			newWorker();
		}
	}
	/**
	 * Adds the given datasetId to the list of Datasets that have to be processed / exported.
	 * @param datasetId
	 */
	public synchronized void addDs( int datasetId ) {
		Job j = new Job( datasetId, null );
		queue.add(j);
		// maybe start the worker
		newWorker();		
	}
	
	/**
	 * Work on the queue while there is stuff in there.
	 */
	public void run() {
		try {
			while( true ) {
				Job j = getJob();
				if( j == null ) break;
				// do stuff
				if( j.deletePath != null ) {
					File f = new File( j.deletePath);
					f.delete();
				} else {
					Transformation tr= DB.getTransformationDAO().getById((long) j.dsId, false);
					// require the validation has run
					if( tr.getSchemaStatus().equals( Dataset.SCHEMA_OK ))
						exportDs( tr );
					else
						log.info( "Dataset " + tr.getDbID() + " has not successfully validated.");
				}
			} 
		} catch( Exception e ) {
			log.error( "Hotdir Worker died with Exception", e );
		}
	}
	
	
	private void newWorker() {
		if( working == null ) {
			working = "working";
			Queues.queue(this, "now");
		}
	}
	
	/**
	 * Internally called to check the queue for more jobs and schedule the next one if there is.
	 * Deletes the ".inProgress" if there is nothing left to do. 
	 */
	private synchronized Job getJob() {
		if( queue.size() > 0  ) {
			return queue.pop();
		} else {
			inProgress.delete();
			working = null;
			return null;
		}
	}
	
	/**
	 * Given dataset is tarballed and written to correct dir  
	 * @param ds
	 */
	private void exportDs( Dataset ds ) {
		// touch .inProgress
		// create the tmpFile
		// start filling the tarball
		// rename to final name
		FileOutputStream fos = null;
		GzipCompressorOutputStream gz = null;
		File tmpFile = null;
		
		try {
			FileUtils.touch(inProgress);
			tmpFile = tmpFile( ds );
			fos = new FileOutputStream( tmpFile );
			gz = new GzipCompressorOutputStream( fos );

			final TarArchiveOutputStream tos = new TarArchiveOutputStream(gz);
			
			ds.processAllItems(new ApplyI<Item>() {
				public void apply(Item item) throws Exception {
					writeItem( item, tos );
				}
			}, false );
			
			renameFinish(tmpFile);
			tos.close();
		} catch( Exception e ) {
			log.error( "Export in hotdir problem",e );
			// 
		} finally {
			if( fos != null ) IOUtils.closeQuietly(fos);
			if( gz != null ) IOUtils.closeQuietly(gz);
			
		}
	}
	
	/**
	 * Generate the temporary export file for this Dataset.
	 * @param ds
	 * @return
	 */
	private File tmpFile( Dataset ds ) {
		String orgname = ds.getOrganization().getEnglishName();
		orgname += "_["+ds.getOrganization().getDbID()+"]";
		orgname = saneFilename( orgname );
		File orgDir = new File( basedir, orgname );
		if(!orgDir.exists() ) {
			// make it
			orgDir.mkdir();
		}
		if( ! orgDir.isDirectory()) {
			// uhh
			// crash ??
			log.error( "Orgdir exists but is not a dir ???");
			throw new RuntimeException("hotdir corrupt!");
		}
		
		String filename = ds.getOrigin().getName()+"_["+ds.getDbID()+"].part.tgz";
		filename = saneFilename( filename );
		File result = new File( orgDir, filename );
		return result;
	}
	
	
	private String saneFilename( String insaneFilename ) {
		return insaneFilename.replaceAll( "[\u0001-\u001f<>:\"/\\\\|?*\u007f]", "_" ).trim();
	}
	
	
	
	/**
	 * Dump the item into the given tarball stream.
	 * @param item
	 * @param tos
	 */
	private static void writeItem( Item item, TarArchiveOutputStream tos  ) {
		String name = item.getDbID()+".xml";
		try {
			if( item.isValid()) name = "valid/"+name;
			else name= "invalid/"+name;

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			String itemXml = item.getXml();
			if(!itemXml.startsWith("<?xml"))
				itemXml = "<?xml version=\"1.0\"  encoding=\"UTF-8\" ?>\n" + itemXml;
			else 
				itemXml = itemXml.replaceFirst("<\\?xml.*\\?>", 
							"<?xml version=\"1.0\"  encoding=\"UTF-8\" ?>\n");
			IOUtils.write(itemXml, baos, "UTF-8");
			baos.close();
			byte[] data = baos.toByteArray();

			TarArchiveEntry entry = new TarArchiveEntry(name);
			entry.setSize((long) data.length );
			tos.putArchiveEntry(entry);
			tos.write(data, 0, data.length);
			tos.closeArchiveEntry();
		} catch( Exception e ) {
			log.error( "Dump item into tarball failed.", e );
		}
	}
	
	/**
	 * When export is finished, remove the ".part" in the filename.
	 * @param tmpFile
	 */
	private void renameFinish( File tmpFile ) {
		String orgName = tmpFile.getAbsolutePath();
		try {
			String newName = orgName.replaceAll(".part.tgz$", ".tgz");
			File newFile = new File( newName );
			tmpFile.renameTo( newFile );
		} catch( Exception e ) {
			log.error( "Finalizing file with rename failed for '" + orgName +"'" );
		}
	}

	// generate delete and create jobs to get the hotdir in sync with the 
	// db
	public void sync() {
		// remove partial files,
		// remove inProgress
		// find sets for exported and existing Transformations
		removePartialExports();
		HashSet<Integer> toCreate = collectDbTransformedDatasets();
		HashMap<Integer,String> toDelete = collectExportedDatasets();

		// remove common elements from both sets
		Iterator<Integer> i = toCreate.iterator();
		while( i.hasNext()) {
			Integer in = i.next();
			if( toDelete.containsKey(in)) {
				toDelete.remove(in);
				i.remove();
			}
		}
		// issue deletes and creates
		for( Map.Entry<Integer,String> i1: toDelete.entrySet() ) removeDs( i1.getKey(), i1.getValue() );
		for( Integer i2: toCreate ) addDs( i2 );
		log.info( "Issued " + toDelete.size() + " deletes and " + toCreate.size() + " creates in hotdir sync");
	}
	
	/**
	 * Find all ids of datasets alreadyt exported.
	 * @return
	 */
	private HashMap<Integer,String> collectExportedDatasets() {
		HashMap<Integer,String> result = new HashMap<Integer,String>();
		Iterator<File> itf = FileUtils.iterateFiles(basedir, new String[]{"tgz"}, true);
		while( itf.hasNext()) {
			File f = itf.next();
			try {
				String num = f.getName().replaceAll("^.*\\[(\\d+)\\].tgz", "$1");
				int numI = Integer.parseInt(num);
				result.put( numI,f.getAbsolutePath() );
			} catch( Exception e ) {
				log.warn( "File '" + f.getName() + "' didn't have dataset Id." );
			}
		}
		return result;
	}

	/**
	 * Given only the datasetId, find the file and return its path (or nothing if its not there)
	 * @param datasetId
	 * @return
	 */
	private String findPath( int datasetId  ) {
		Iterator<File> itf = FileUtils.iterateFiles(basedir, new String[]{"tgz"}, true);
		while( itf.hasNext()) {
			File f = itf.next();
			String num = f.getName().replaceAll("^.*\\[(\\d+)\\].tgz", "$1");
			int numI = Integer.parseInt(num);
			if( numI == datasetId ) return f.getAbsolutePath();
		}
		return null;
	}

	private void removePartialExports() {
		Iterator<File> itf = FileUtils.iterateFiles(basedir, new String[]{"tgz"}, true);
		while( itf.hasNext()) {
			File f = itf.next();
			if( f.getName().matches( ".*\\.part\\.tgz$"))
				f.delete();
		}
	}

	/**
	 * Dump all ids of Transformation Datasets
	 * @return
	 */
	private HashSet<Integer> collectDbTransformedDatasets() {
		HashSet<Integer> result = new HashSet<Integer>();
		for(Long l: DB.getTransformationDAO().listIds( Optional.empty())) result.add(  l.intValue() );
		return result;		
	}
}
