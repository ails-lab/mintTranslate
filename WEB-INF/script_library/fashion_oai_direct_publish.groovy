import gr.ntua.ivml.mint.concurrent.Queues
import gr.ntua.ivml.mint.util.*

import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

import org.apache.log4j.Logger

// make some fashionUtils into a deque

rnbl = {
	def utils = new LinkedBlockingQueue();
	// def res = [] as List
	def res = DB.publicationRecordDAO.findAll().
		findAll{ pr -> pr.status == "OK" }.
		collect{ pr -> [ pr.originalDataset.dbID, pr.publishedItemCount ] }.
		sort{ a,b -> b[1] <=> a[1] }.
		collect{ it[0] }
		
	for( int i=0; i<10; i++ ) {
		def f = new FashionUtils();
		f.inintRMP();
		utils.put(f )
	}
	
	// create runnables whenever there is a free fashionUtil
	while( !res.empty ) {
		def r2 = new MyR();
		r2.datasetId = res.remove(0) as long;
		// block here
		r2.f = utils.take();
		r2.q = utils;
		Queues.queue(r2, "db" )
	}
}

Queues.queue( rnbl, "now" )

class MyR implements Runnable {
	public BlockingQueue q;
	public FashionUtils f;
	Logger log = Logger.getLogger( "gr.ntua.ivml.mint.LongScript")
	
	public long datasetId;
	
	public void run() {
		// do stuff
		try {
			def ds = DB.datasetDAO.getById( datasetId, false )
			ds = ds.getBySchemaName( "EDM FP" );
			if( ds != null ) {
				log.info( "Publishing #$ds.dbID with $ds.itemCount items.")
				f.oaiPublishDatasetDirectly( ds )
			} else {
				log.info( "Didnt find EDM FP for #"+ datasetId )
			}
		} catch( Throwable th ) {
			log.error( "", e )
		} finally {
		// return f for next run
			q.put( f )
		}
	}
}