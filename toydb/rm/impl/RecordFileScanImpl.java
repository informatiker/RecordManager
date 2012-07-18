package toydb.rm.impl;


import toydb.rm.RID;
import toydb.rm.Record;
import toydb.rm.impl.RecordImpl;
import toydb.rm.RecordFile;
import toydb.rm.RecordFileScan;
import toydb.util.AttrInfo;
import toydb.util.DBException;

/**
 * @author Christian Müller
 */
public class RecordFileScanImpl implements RecordFileScan {

	//describing the state of a search: the current pagenumber, and record-slot
	private RID CurrentRID;
	private AttrInfo[] AttribList;
	private int AttribSize = 0;
	//Beware - this one is implementation-specific
	private RecordFileImpl rFile;
	private int Offsets[];
	
	/** Prepare a scan to iterate over all records in relation
	 * 
	 * @param rFile the recordFile to scan over
	 * @param attribs a list of AttrInfos describing the structure of a record and
	 * 					constraints every result record has to fullfill
	 */

	//TODO What Exception?
	public void openScan(RecordFile rFile, AttrInfo[] attribs) throws DBException {
		//save the attributes and the reference to the RecordFile for further usage
		AttribList = attribs;
		this.rFile = (RecordFileImpl)rFile;
		
		if(this.rFile == null) {
			throw new DBException("Not a valid RecordFile instance");
		}
		
		//allocate table for offsets of attributes in records to avoid calcing offsets
		//multiple times
		Offsets = new int[AttribList.length];
		
		AttrInfo[] TupleAttr = rFile.getAttrInfo();
		
		int OffsetCounter = 0;
		
		//No Attributes to filter are given - assume user wants "all"
		if(TupleAttr == null) {
			TupleAttr = rFile.getAttrInfo();
		}
			
		//walk the Array of relation attribs
		for(int i = 0; i<TupleAttr.length; i++) {
			
			//for every attrib in relation check if there is the same attribute in our
			//projection-list. If so, save the offsets counted so far in 'offsets'
			for(int y = 0; y<AttribList.length; y++) {
				
				//is the attrib in relation scheme one of the projected ones?
				if(TupleAttr[i].getAttrName().equals(AttribList[y].getAttrName())) {
					
					//calculate the offsets of the desired attribs within the record
					Offsets[y] = OffsetCounter;
					//OffsetCounter += AttribList[y].getByteLength();
					AttribSize += AttribList[y].getByteLength();
				}
			}
			OffsetCounter += TupleAttr[i].getByteLength();
		}
		
		//initialize the position within the records
		CurrentRID = new RID();

	}
	
	/** Iterate through the records and return the next record
	 * 
	 * @return Record object representing the next found tuple,
	 * 					null if there are no more records matching attribs constraints
	 */
	public Record getNextRecord() throws DBException {
		Record tmprecord;
		
		//TODO don't create new RID just to check for defaults -.-
		if(CurrentRID.equals(new RID())) {
			//We just started the scan - so get the first record in relation
			tmprecord = rFile.getFirstRecord();
		} else {
			tmprecord = rFile.getNextRecord(CurrentRID);
		}
		
		if(tmprecord == null) {
			// error: no successor found
			return null;
		} else {
			CurrentRID = tmprecord.getRID();
			
			//Everything's done - we got the record: filter for wanted attributes
			
			byte valbuff[] = new byte[AttribSize];
			int poscount = 0;
			
			for(int i = 0; i<AttribList.length; i++) {
				System.arraycopy(tmprecord.getData(), Offsets[i], valbuff, poscount, AttribList[i].getByteLength());
				poscount += AttribList[i].getByteLength();
			}
			
			return new RecordImpl(CurrentRID, valbuff);
		}
	}

	/** Close the scan
	 */
	public void closeScan() throws DBException {
		//TODO what da hell is closeScan() supposed to mean?
		//TODO No exception to throw!
		//NOP
	}
}
