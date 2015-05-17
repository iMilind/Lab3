package edu.sjsu.cmpe.cache.repository;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import edu.sjsu.cmpe.cache.domain.Entry;
import net.openhft.chronicle.map.ChronicleMap;

public class ChronicleMapCache implements CacheInterface {

	private final ChronicleMap<Long,Entry>cMap;
	
	public ChronicleMapCache(ChronicleMap<Long,Entry>entries){
		cMap=entries;
	}
	
	public Entry save(Entry newEntry) {
		// TODO Auto-generated method stub
		 checkNotNull(newEntry, "newEntry instance must not be null");
		 cMap.putIfAbsent(newEntry.getKey(), newEntry);
		return null;
	}

	public Entry get(Long key) {
		// TODO Auto-generated method stub
		checkArgument(key > 0,
                "Key was %s but expected greater than zero value", key);
		return cMap.get(key);
	}

	public List<Entry> getAll() {
		// TODO Auto-generated method stub
		return new ArrayList<Entry>(cMap.values());
	}
	
	

}

