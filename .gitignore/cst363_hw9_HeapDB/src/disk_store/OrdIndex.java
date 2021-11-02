package disk_store;

import java.util.ArrayList;
import java.util.List;

/**
 * An ordered index.  Duplicate search key values are allowed,
 * but not duplicate index table entries.  In DB terminology, a
 * search key is not a superkey.
 * 
 * A limitation of this class is that only single integer search
 * keys are supported.
 *
 */


public class OrdIndex implements DBIndex {
	
	private class Entry {
		int key;
		ArrayList<BlockCount> blocks;
	}
	
	private class BlockCount {
		int blockNo;
		int count;
	}
	
	ArrayList<Entry> entries;
	int size = 0;
	
	/**
	 * Create an new ordered index.
	 */
	public OrdIndex() {
		entries = new ArrayList<>();
	}
	
	@Override
	public List<Integer> lookup(int key) {
		// binary search of entries arraylist
		// return list of block numbers (no duplicates). 
		// if key not found, return empty list
		//throw new UnsupportedOperationException();
		List<Integer>blockNums= new ArrayList<>();
		int min=0;
		int max=entries.size()-1;

		while(min<=max) {
			int mid = (max + min) / 2;
			if (entries.get(mid).key == key) {
				for (BlockCount blockCount : entries.get(mid).blocks) {
					blockNums.add(blockCount.blockNo);
				}
				return blockNums;

			}
			 else if (entries.get(max).key == key) {
				for (BlockCount blockCount : entries.get(max).blocks) {
					blockNums.add(blockCount.blockNo);

				}
				return blockNums;
			}else if (entries.get(min).key == key) {
					for (BlockCount blockCount : entries.get(min).blocks) {
						blockNums.add(blockCount.blockNo);

					}
				return blockNums;
				} else if (entries.get(mid).key < mid) {
					max = mid - 1;
				} else {
					min = mid + 1;
				}
			}

		return blockNums;
	}
	
	@Override
	public void insert(int key, int blockNum) {
		//throw new UnsupportedOperationException();
		List<Integer>blocklist=lookup(key);
		if(blocklist.size()!=0){
			for(Entry entry:entries){
				if(entry.key==key){
					for (BlockCount blockCount:entry.blocks){
						if(blockCount.blockNo==blockNum){
							blockCount.count++;
							size++;
							return;
						}
					}

					BlockCount newBlock=new BlockCount();
					newBlock.count=1;
					newBlock.blockNo=blockNum;
					entry.blocks.add(newBlock);
					size++;
					return;

				}
			}

		}else {


			int index=0;
			int min = 0;
			int max = entries.size() - 1;
			while (max - min > 1) {
				int mid = (max + min) / 2;
				if (entries.get(mid).key == key) {

					index = mid;
				} else if (entries.get(mid).key < mid) {
					max = mid;
				} else {
					min = mid;
				}
			}

			Entry newEntry=new Entry();
			newEntry.key=key;
			newEntry.blocks=new ArrayList<>();
			BlockCount newBlock=new BlockCount();
			newBlock.count=1;
			newBlock.blockNo=blockNum;
			newEntry.blocks.add(newBlock);
			entries.add(index,newEntry);
			size++;


		}

	}

	@Override
	public void delete(int key, int blockNum) {
		// lookup key 
		//  if key not found, should not occur.  Ignore it.
		//  decrement count for blockNum.
		//  if count is now 0, remove the blockNum.
		//  if there are no block number for this key, remove the key entry.
		//throw new UnsupportedOperationException();
		List<Integer> blocklist = lookup(key);
		if (blocklist.size() == 0) {
			return;
		}
		for (Entry entry : entries) {
			if (entry.key == key) {
				for (BlockCount blockCount : entry.blocks) {
					if (blockCount.blockNo == blockNum) {
						blockCount.count--;
						size--;

						if (blockCount.count == 0) {
							entry.blocks.remove(blockCount);
						}
						if (entry.blocks.size() == 0) {
							entries.remove(entry);
						}
						return;
					}
				}
				entries.remove(entry);
				return;
			}
		}

	}
	
	/**
	 * Return the number of entries in the index
	 * @return
	 */
	public int size() {
		return size;
		// you may find it useful to implement this
		
	}
	
	@Override
	public String toString() {
		throw new UnsupportedOperationException();
	}
}