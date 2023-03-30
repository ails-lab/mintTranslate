import gr.ntua.ivml.mint.projects.euscreen.*
import gr.ntua.ivml.mint.concurrent.Queues

// convert all portal published core sets into edm

// or make a list of those you want to do

def listds = [
5124, 8919, 5066, 4746, 8053, 7487, 6789, 6240, 7538, 7301, 8524, 6233, 9784, 9106, 4671, 4790, 7878, 8913, 4859, 8918, 5031, 9748, 
4879, 8910, 7951, 4869, 9396, 5047, 7950, 8909, 7963, 4858, 4952, 5027, 4693, 4572, 7830, 8934, 4660, 5038, 9092, 5125, 4929, 9433, 
4408, 4630, 4992, 4628, 4758, 4616, 5045, 4422, 8223, 8224, 4963, 5037, 8132, 9144, 8201, 6156, 9998, 4867, 9994, 9764, 4803, 7388, 
4696, 5035, 6805, 9143, 5046, 5128, 4637, 10000, 5587, 5582, 5121, 9265, 9781, 9058, 9484, 4818, 4550, 5161, 4822, 9398, 6067, 4764, 
6612, 4462, 5168, 8226, 9202, 6801, 9754, 8912, 7637, 9093, 10001, 9266, 9996, 9949, 9942, 9081, 9432, 5147, 8081, 4584, 9149, 4947, 
5026, 9798, 8606, 9198, 9145, 9980, 7636, 6624, 8921, 4897, 10577, 8033, 9766, 6622, 5157, 9801, 4565, 7460, 5041, 5017, 9999, 6172, 
8129, 9962, 9800, 9132, 4335, 6647, 9971, 4631, 4706, 6410, 4740, 9933, 9434, 4519, 4889, 4810, 4815, 5137, 4459, 4970, 9926, 4611, 
4531, 6158, 4982, 9908, 7019, 5692, 6618, 6074, 6087, 4805, 4785, 4807, 4802, 6072, 7973, 4708, 6057, 6070, 4801, 4842, 6319, 4863, 
8769, 4672, 7892, 8137, 5048, 8526, 6918, 8528, 4689, 4553, 5149, 8784, 5235, 9963, 4821, 8812, 9965, 7149, 9997, 4712, 4756, 
5745, 9142, 5164, 7998, 4658, 7017, 10598, 7995, 7985, 4547, 4775, 6811, 8353, 8956, 8837, 4381, 8001, 4479, 4665, 9108, 7483 
]

// def listds = DB.publicationRecordDAO.simpleList( "target='euscreen.portal'").collect{ it.publishedDataset.dbID }


Queues.queue( {
    DB.getSession().beginTransaction();
    def mapping = DB.mappingDAO.getById( 999l, false )
    def noterik = EuscreenPublish.AllNoterikInfo.read()
    def series = EuscreenPublish.getOrMakeSeriesInfo( noterik ) 

  for( long dsid: listds ) {
      def ds = DB.datasetDAO.getById( dsid, false )
    EuscreenPublish.corePrepareEdm( ds, noterik, series, mapping ) 
  }
  DB.closeSession()

}, "now" )

