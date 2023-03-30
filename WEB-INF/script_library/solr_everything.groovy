import gr.ntua.ivml.mint.concurrent.*
import gr.ntua.ivml.mint.persistent.*
import org.apache.log4j.Logger;
import gr.ntua.ivml.mint.db.DB

// Solr everything again

// delete everything
Solarizer.delete( "*:*")
Solarizer.commit()

// build a solarizer for every dataset and queue

List ids = DB.datasetDAO.listIds()
// List someIds = ids[0..2]
// someIds.each{ id ->
ids.each { id ->
	def rb = {
		try {
			DB.getSession().beginTransaction();

			Long idLong = id as Long
			Dataset ds = DB.datasetDAO.getById( idLong, false)
			Solarizer solr = new Solarizer( ds)
			solr.runInThread()
		} finally {
			DB.closeSession();
			DB.closeStatelessSession();
		}
	}
	Queues.queue( rb, "db")
}
