// Transfrom many Datasets with same mapping and ignore locks

import gr.ntua.ivml.mint.persistent.*
import gr.ntua.ivml.mint.util.*
import gr.ntua.ivml.mint.concurrent.Queues;
import gr.ntua.ivml.mint.concurrent.XSLTransform;


def mappingName = "edmfp2edm"
def mapping = DB.mappingDAO.simpleGet( "name='$mappingName'")

if( mapping == null ) {
    println( "Mapping $mappingName not found")
    return
}

def dsIds = [ 1,2,3,4 ]

def toDo = []

for( dsId in dsIds ) {
    long x = dsId 
    ds = DB.datasetDAO.getById( x, false )
    if( ds == null ) {
        println( "Invalid id #$x")
        return
    }
    toDo.add( ds )
}

for( ds in toDo ) {
//    println( "Mapping $mapping.name to $mapping.targetSchema.name" )
     println( "$ds.origin.name in $ds.organization.englishName")
     Transformation tr = Transformation.fromDataset( ds, mapping);
     tr.setName( "Mapping $mapping.name to $mapping.targetSchema.name" )
     DB.getTransformationDAO().makePersistent( tr );
     DB.commit();
     log.info( "Created Transformation " + tr.getDbID());
     XSLTransform trans = new XSLTransform( tr );
     Queues.queue( trans, "db");
     log.info( "Queued " + ds.getName()+ " with " + ds.getItemCount() + " items" );
}
