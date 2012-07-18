package toydb.rm.impl;

import java.util.BitSet;

import toydb.pf.Page;
import toydb.pf.impl.PagedFileImpl;
import toydb.rm.RID;
import toydb.rm.Record;
import toydb.rm.RecordFile;
import toydb.util.AttrInfo;
import toydb.util.DBException;

/**
 * @author Daniel Oelschlegel
 * @version $Revision: 1.1 $
 */

public class RecordFileImpl implements RecordFile {
	// array that describes the relation scheme
	private AttrInfo[] attribs;
	// raw byte array of a record
	private int recordRawSize = 0;
	// count current used pages
	private int usedPages = 0;
	// file handler for accessing a page
	private PagedFileImpl pFile;
	
	// Helper
	// saves list of pages which have a free slot
	private BitSet bs = new BitSet();
	// max. number of elements that fit in a mini-page
	private int mpSize = 0;
	// size of slot allocation table in bytes
	private int headerSize = 0;
	// last character in header when all slots are free
	private byte lastByte = 0x00;
	// distance between two value of an attribute
	private int stepRange = 0;
	// positions where a mini-page begin
	private int[] mpBegin;
	// for faster access 
	private int[] attLen;
	private static final byte[] calc = {-128, 64, 32, 16, 8, 4, 2, 1};
	
	/**
	 * Creates necessary data structure for better handling. 
	 * @param attribs Describes the attribute scheme of the relation.
	 * @param pFile Represents the the page file.
	 * @exception invalid input data
	 */
	public RecordFileImpl(AttrInfo[] attribs, PagedFileImpl pFile) 
	throws DBException {
		
		if(pFile == null)
			throw new DBException("no valid page file reference");
		if(attribs.length < 1)
			throw new DBException("no columns in the relation");
		
		this.attribs = attribs;
		// save unnecessary class member accesses
		attLen = new int[attribs.length];
		for(int i = 0; i< attribs.length; i++)
			attLen[i] = attribs[i].getByteLength();
		// biggest attribute size
		for(int i = 0; i < attribs.length; i++)
			if(stepRange < attLen[i])
				stepRange = attLen[i];
		// number of elements / mini-pages
		int amount = (Page.PAGE_SIZE / stepRange) / attribs.length;
		headerSize = (int) Math.ceil(amount / 8.0);
		mpSize = (Page.PAGE_SIZE - headerSize) / attribs.length;
		// important for the header on a record page
		if(mpSize % 8 != 0) {
			for(int i = 0; i < mpSize % 8; i++)
				lastByte |= 128 >> (i-1); // why not calc[i]?
			lastByte = (byte) ~lastByte;
		}
		// save unnecessary computations
		mpBegin = new int[attribs.length];
		for(int i = 0; i < attribs.length; i++) 
			mpBegin[i] = i * mpSize + headerSize;
		
		for(int i = 0; i < attribs.length; i++)
			recordRawSize += attLen[i];
		this.pFile = pFile;
		// traverse through all pages
		// INFO: PagedFile should not grow without noticing bit set
		// first page is already occupied
		Page page = pFile.firstPage();
		while((page=pFile.nextPage(page)) != null) 
			if(!fullSlots(page.getData())) 
				bs.set(page.getPageNum());
		usedPages = getNumOfPages();
	}
	
	/**
	 * checks in the header of a page if all slots are full 
	 * @param raw byte stream from a record page
	 * @return true is it empty otherwise false
	 */
	private boolean fullSlots(byte[] data) {
		
		for(int i = 0; i < headerSize; i++) 
			if(data[i] != -1) 
				return false;
			
		return true;
	}
	
	/**
	 * Checks in the header of a (record) page, if all slots are empty.
	 * @param Raw byte stream from a record page.
	 * @return True is it empty, otherwise false.
	 */
	private boolean emptySlots(byte[] data) {
		
		for(int i = 0; i < headerSize - 1; i++) 
			if(data[i] != 0)
				return false;
		if(data[headerSize-1] != lastByte)
			return false;
		
		return true;
	}
	
	/**
	 * Finds the first record in the relation.
	 * @return Returns null, if the record is not fund, otherwise a reference
	 * to a record instance.
	 * @exception
	 */
	public Record getFirstRecord() throws DBException{
		int slotNum = -1;
		Page page = pFile.firstPage();
		
		while(slotNum == -1) {
			page = pFile.nextPage(page);
			if(page == null)
				break;
			byte[] data = page.getData();
			for(int i = 0; i < headerSize; i++)
				if(data[i] != 0) {
					int k = 0;
					for(; (data[i] & (calc[k])) == 0; k++);
					slotNum = (i << 3) + k;
					break;		
				}
		}
		
		return getRecord(new RID(page.getPageNum(), slotNum));
	}
	
	/**
	 * Returns the next record from RID.
	 * @param RID object specifies page and slot number
	 * @return The return value is null, if the end is reached, 
	 * otherwise it returns a reference to a record instance.
	 * @exception invalid input data
	 */
	public Record getNextRecord(RID rid) throws DBException{
		int slotNum = rid.getSlotNum();
		Page page = pFile.getPage(rid.getPageNum());
		boolean finish = false;
		
		// pining useful
		do {
			byte[] data = page.getData();
			for(int i = 0; i < headerSize; i++)
				if(data[i] != 0) {
					int k = 0;
					for(; (data[i] & (calc[k])) == 0; k++);
					slotNum = (i << 3) + k;
					// nested loop
					finish = true;
					break;		
				}
			if(!finish) {
				page = pFile.nextPage(page);
				if(page == null)
					return null;
			}
		} while(!finish);
		
		return getRecord(new RID(page.getPageNum(), slotNum));
	}
	
	/**
	 * Close the opened page file .
	 * @exception
	 */
	public void close() throws DBException {
		
		pFile.close();
	}

	/**
	 * deleting and closing paged files doesn't work -.-
	 * @exception not needed
	 */	
	public void destroy() throws DBException {
		//do nothing
	}
	
	/**
	 * Checks, if the slot is occupied and returns the specified record.
	 * @param rid The specified unique record identifier.
	 * @return Returns null, when slot is marked as empty or slot number doesn't 
	 * exit, otherwise a valid record object is returned.
	 * @exception invalid input data 
	 */
	public Record getRecord(RID rid) throws DBException {
		byte[] data = pFile.getPage(rid.getPageNum()).getData();
		byte[] result = new byte[recordRawSize];
		int pos = 0;
		int slotNum = rid.getSlotNum();
		int offset = stepRange * slotNum;
		byte check = data[slotNum >> 3];
		
		// is slot occupied?
		if(((check & (calc[slotNum % 8])) == 0) || slotNum >= mpSize) 
			return null;
		for(int i = 0; i < attribs.length; i++) {
			System.arraycopy(data, mpBegin[i] + offset, result,	pos, attLen[i]);
			pos += attLen[i];
		}
		
		return new RecordImpl(rid, result);
	}
	
	/**
	 * Copies the raw byte stream on the right position on the page.
	 * @param pageNum Specifies the needed page number.
	 * @param slotNum Specifies the needed slot number within a (record) 
	 * page.
	 * @exception invalid input data
	 */
	private void writeRecord(int pageNum, int slotNum, byte[] raw) 
	throws DBException {
		Page page = pFile.getPage(pageNum);
		byte[] data = page.getData();
		int pos = 0;
		int offset = stepRange * slotNum;
		
		for(int i = 0; i < attribs.length; i++) {
			System.arraycopy(raw, pos, data, mpBegin[i] + offset, 
					attLen[i]);
			pos += attLen[i];
		}
		page.markDirty();
	}
	
	/**
	 * Inserts a record, if necessary add a new page.
	 * @param data Specifies the raw byte stream that contains the 
	 * raw content.
	 * @return Returns a RID object with the page and slot number.
	 * @exception An exception is thrown when allocation page 
	 * index is full.
	 */
	public RID insertRecord(byte[] data) throws DBException {
		int pageNum = bs.nextSetBit(0); 
		int slotNum = -1;
		byte[] header;
		
		if(pageNum == -1) {
			// no free page found, allocate a new page
			Page page = pFile.allocatePage();
			pageNum = page.getPageNum();
			slotNum = 0;
			// write header
			header = page.getData();
			for(int i = 0; i < headerSize - 1; i++)
				header[i] = 0;
			header[headerSize - 1] = lastByte;
			// special case: <= 8 slots per page
			header[0] |= 128;
			bs.set(pageNum);
			++usedPages;
		} else
			header = pFile.getPage(pageNum).getData();
		
		// find free slot on the page
		// use not only the header :-|
		if(slotNum == -1) {
			for(int i = 0; i < headerSize; i++)
				if(header[i] != -1) {
					int k = 0;
					for(; (header[i] & calc[k]) != 0; k++);
					slotNum = (i << 3) + k;
					header[i] |= calc[k];
					break;		
				}
			if(fullSlots(header))
				bs.clear(pageNum); 
		} 
		writeRecord(pageNum, slotNum, data);
		
		return new RID(pageNum, slotNum);
	}
	
	/**
	 * Deletes a record by setting a bit in the allocation index on the 
	 * right (record) page.
	 * @param rid Specifies the chosen the page and slot number. There 
	 * are no checks if the RID is within their bounds.
	 * @exception invalid input data
	 */
	public void deleteRecord(RID rid) throws DBException {
		int pageNum = rid.getPageNum();
		int slotNum = rid.getSlotNum();
		Page page = pFile.getPage(pageNum);
		byte[] data = page.getData();
		
		// exists this record on page?
		if((data[slotNum >> 3] & (calc[slotNum % 8])) == 0) 
			return;
		// clear bit in header
		data[slotNum >> 3] &= ~(calc[slotNum % 8]);
		page.markDirty();
		
		// remove page if is empty
		if(emptySlots(data)) {
			bs.clear(pageNum);
			pFile.dispose(page); 
			--usedPages;
		} else 
			bs.set(pageNum); 
		
	}
	
	/**
	 * Updates an existing record in the relation.
	 * @param rec Stores the Record object including data and RID. Slots, 
	 * which are not occupied will be ignored. There are no checks
	 * if the RID is within their bounds.
	 * @exception invalid input data
	 */
	public void updateRecord(Record rec) throws DBException {
		int pageNum = rec.getRID().getPageNum();
		int slotNum = rec.getRID().getSlotNum();
		byte[] data = pFile.getPage(pageNum).getData();
		
		// check if slot is occupied
		if((data[slotNum >> 3] & (calc[slotNum % 8])) != 0) 
			writeRecord(pageNum, slotNum, rec.getData());
	}
	
	/**
	 * getting the attribute scheme of the relation
	 * @return AttrInfo[]  a reference
	 */ 
	public AttrInfo[] getAttrInfo() {
		 
		return attribs;
	}

	/**
	 * Returns the number of used pages.
	 * @return The size is 0, when an error is occurred, otherwise
	 * at least 1.
	 * @exception
	 */ 
	public int getNumOfPages() {
		 
		if(usedPages == 0) {
			try {
				Page page = pFile.firstPage();
				usedPages = 1;
				while((page = pFile.nextPage(page)) != null) 
					++usedPages;
			} catch(DBException e) {/* dirty */} 
		 }
		 return usedPages;
	 }

	/**
	 * Forces that all pages in the buffer have to be written on disk.
	 * @exception
	 */
	public void forceAll() throws DBException {
		
		pFile.forceAllPages();
	}
}
