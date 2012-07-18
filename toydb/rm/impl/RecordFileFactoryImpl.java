package toydb.rm.impl;

import java.io.File;
import java.io.IOException;

import toydb.rm.RecordFile;
import toydb.rm.RecordFileFactory;
import toydb.rm.RecordFileScan;
import toydb.util.AttrInfo;
import toydb.util.DBException;
import toydb.pf.Page;
import toydb.pf.impl.PagedFileFactoryImpl;
import toydb.pf.impl.PagedFileImpl;
import toydb.util.ByteInputStream;
import toydb.util.ByteOutputStream;

/**
 * @author Daniel Oelschlegel
 * @version $Revision: 0.8 $
 */

public class RecordFileFactoryImpl implements RecordFileFactory {

	private static final RecordFileFactoryImpl SHARED_INSTANCE = new RecordFileFactoryImpl();
	
	public static RecordFileFactoryImpl sharedInstance() {
		return SHARED_INSTANCE;
	}
	
	public RecordFile createFile(String fileName, AttrInfo[] attribs) 
	throws DBException {
		
		//if(new File(fileName).exists())
			//throw new DBException("file already exists");
		//	return openFile(fileName);
		if(attribs.length < 1)
			throw new DBException("no columns in the database");
		else {	
			PagedFileFactoryImpl pfMgr = PagedFileFactoryImpl.sharedInstance();
			PagedFileImpl pFile = (PagedFileImpl) pfMgr.createFile(fileName);
			
			Page page = pFile.allocatePage();
		    byte[] data = page.getData();
			ByteOutputStream bOS = new ByteOutputStream(data);
			
			try {
				// number of AttrInfo objects
				bOS.writeInt(attribs.length);
				// write AttrInfo[] Block
				for(int n=0; n<attribs.length; n++) {
					bOS.writeInt(attribs[n].getRelName().length());
					bOS.writeChars(attribs[n].getRelName());
					bOS.writeInt(attribs[n].getAttrName().length());
					bOS.writeChars(attribs[n].getAttrName());
					bOS.writeInt(attribs[n].getType());
					bOS.writeInt(attribs[n].getLength());
				}
				// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
				// limitation all data must fit to one page
				// enhanced version: use the first pages and save 
				// a marker of the first record page
				// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
				bOS.close();
			} catch(IOException e) {
				throw new DBException(e.getStackTrace().toString());
			}
			page.unPin();
			page.markDirty();
			
			return new RecordFileImpl(attribs, pFile);
		}
	}

	
	public RecordFile openFile(String fileName) throws DBException {
		AttrInfo[] attribs;
		
		if(!new File(fileName).exists())
			throw new DBException("file doesn't exist");
		
		
		PagedFileFactoryImpl pfMgr = PagedFileFactoryImpl.sharedInstance();
		PagedFileImpl pFile = (PagedFileImpl) pfMgr.openFile(fileName);
		
		Page page = pFile.getPage(0);
		byte[] mem = page.getData();
		ByteInputStream bIS = new ByteInputStream(mem);
		try {
			// read number of AttrInfo objects
			attribs = new AttrInfo[bIS.readInt()];
			// read AttrInfo objects
			for(int n = 0; n < attribs.length; n++) {
				int len = bIS.readInt();
				String relName = bIS.readChars(len);
				len = bIS.readInt();
				String attrName = bIS.readChars(len);
				attribs[n] = new AttrInfo(relName, attrName, bIS.readInt(), bIS.readInt());
			}
			bIS.close();
		} catch(IOException e) {
			throw new DBException(e.getStackTrace().toString());
		}
		page.unPin();
		if(attribs.length < 1)
			throw new DBException("no columns in the database");
		
		return new RecordFileImpl(attribs, pFile);
	}		

	
	public void destroyFile(String fileName) throws DBException {
		
		if(new File(fileName).exists())
			try {
				if(!new File(fileName).delete())
					throw new DBException("can't delete file");
			} catch(SecurityException e) { 
				throw new DBException(e.getStackTrace().toString());
			}
	}

	
	public RecordFileScan createScan() throws DBException {
		
		return new RecordFileScanImpl();
	}
}
