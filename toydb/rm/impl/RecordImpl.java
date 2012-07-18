package toydb.rm.impl;


import toydb.rm.RID;
import toydb.rm.Record;

/** Class representing the record within a PagedFile page. On init, data is copied
 * from the memory page, so changes must be written back.
 * 
 * @author Christian Müller
 */
public class RecordImpl implements Record {

	private byte[] data;
	private RID rid;
	
	/** Constructor for RecordImpl
	 */
	public RecordImpl(RID rid, byte[] data) {
		
		this.rid = rid; 
		this.data = data;
	}
	
	/** Return the copy of the content of the actual record
	 * 
	 * @return a copy of the record content as byte array
	 */
	public byte[] getData() {
		
		return data;
	}
	
	/** Return the RID uniquely identifying the record within the memory
	 * 
	 * @return the record Identifier as RID-Object
	 */
	public RID getRID() {
		
		return rid;
	}
}

