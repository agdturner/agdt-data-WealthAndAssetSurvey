/*
 * Copyright 2018 geoagdt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.leeds.ccg.andyt.generic.data.waas.data;

import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W4ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W1ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_CollectionID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W5ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W2ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W3ID;
import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Object;

/**
 *
 * @author geoagdt
 */
public class WaAS_Data extends WaAS_Object {

    public final HashMap<Short, WaAS_CollectionID> cIDs;
    
    /**
     * For storing the number of Collections.
     */
    public int nOC;
    
    /**
     * The main WaAS data stored in collections. Keys are Collection IDs.
     */
    public final HashMap<WaAS_CollectionID, WaAS_Collection> collections;

    /**
     * The main WaAS data store in simple collections. Keys are Collection IDs.
     */
    public final HashMap<WaAS_CollectionID, WaAS_CollectionSimple> collectionsSimple;

    /**
     * Looks up from a CASEW1 to the Collection ID.
     */
    public final HashMap<WaAS_W1ID, WaAS_CollectionID> w1_To_c;

    /**
     * Looks up from a CASEW1 to the ID.
     */
    public final HashMap<Short, WaAS_W1ID> CASEW1_To_w1;

    /**
     * Looks up from a CASEW2 to the ID.
     */
    public final HashMap<Short, WaAS_W2ID> CASEW2_To_w2;

    /**
     * Looks up from a CASEW3 to the ID.
     */
    public final HashMap<Short, WaAS_W3ID> CASEW3_To_w3;

    /**
     * Looks up from a CASEW4 to the ID.
     */
    public final HashMap<Short, WaAS_W4ID> CASEW4_To_w4;
    
    /**
     * Looks up from a CASEW5 to the ID.
     */
    public final HashMap<Short, WaAS_W5ID> CASEW5_To_w5;


    public WaAS_Data(WaAS_Environment e) {
        super(e);
        collections = new HashMap<>();
        collectionsSimple = new HashMap<>();
        w1_To_c = new HashMap<>();
        CASEW1_To_w1 = new HashMap<>();
        CASEW2_To_w2 = new HashMap<>();
        CASEW3_To_w3 = new HashMap<>();
        CASEW4_To_w4 = new HashMap<>();
        CASEW5_To_w5 = new HashMap<>();
        cIDs = new HashMap<>();
    }

    /**
     *
     * @param cID collectionID
     * @return
     */
    public WaAS_Collection getCollection(WaAS_CollectionID cID) {
        WaAS_Collection r = collections.get(cID);
        if (r == null) {
            r = (WaAS_Collection) loadSubsetCollection(cID);
            collections.put(cID, r);
        }
        return r;
    }

    /**
     *
     * @param cID collectionID
     * @return
     */
    public WaAS_CollectionSimple getCollectionSimple(WaAS_CollectionID cID) {
        WaAS_CollectionSimple r = collectionsSimple.get(cID);
        if (r == null) {
            r = (WaAS_CollectionSimple) loadSubsetCollectionSimple(cID);
            collectionsSimple.put(cID, r);
        }
        return r;
    }

    /**
     * Sets the value for cID in {@link #collections} to {@code null}.
     *
     * @param cID the ID of the subset collection to be set to {@code null}.
     */
    public void clearCollection(WaAS_CollectionID cID) {
        String m = "clearCollection " + cID;
        env.logStartTag(m);
        env.log("TotalFreeMemory " + env.getTotalFreeMemory());
        collections.put(cID, null);
        env.log("TotalFreeMemory " + env.getTotalFreeMemory());
        env.logEndTag(m);
    }

    /**
     * Sets the value for cID in {@link #collectionsSimple} to {@code null}.
     *
     * @param cID the ID of the subset collection to be set to {@code null}.
     */
    public void clearCollectionSimple(WaAS_CollectionID cID) {
        collectionsSimple.put(cID, null);
    }

    /**
     * Caches and clears the first subset collection retrieved from an iterator.
     *
     * @return {@code true} iff a subset collection was cached and cleared.
     */
    public boolean clearSomeData() {
        Iterator<WaAS_CollectionID> ite = collections.keySet().iterator();
        while (ite.hasNext()) {
            WaAS_CollectionID cID = ite.next();
            WaAS_Collection c = collections.get(cID);
            if (c != null) {
                cacheSubsetCollection(cID, c);
                collections.put(cID, null);
            }
            return true;
        }
        ite = collectionsSimple.keySet().iterator();
        while (ite.hasNext()) {
            WaAS_CollectionID cID = ite.next();
            WaAS_CollectionSimple c = collectionsSimple.get(cID);
            if (c != null) {
                cacheSubsetCollectionSimple(cID, c);
                collectionsSimple.put(cID, null);
            }
            return true;
        }
        return false;
    }

    /**
     * Caches and cleared all subset collections.
     *
     * @return The number of subset collections cached and cleared.
     */
    public int clearAllData() {
        int r = 0;
        Iterator<WaAS_CollectionID> ite = collections.keySet().iterator();
        while (ite.hasNext()) {
            WaAS_CollectionID cID = ite.next();
            WaAS_Collection c = collections.get(cID);
            if (c != null) {
                cacheSubsetCollection(cID, c);
                collections.put(cID, null);
                r++;
            }
        }
        ite = collectionsSimple.keySet().iterator();
        while (ite.hasNext()) {
            WaAS_CollectionID cID = ite.next();
            WaAS_CollectionSimple c = collectionsSimple.get(cID);
            if (c != null) {
            cacheSubsetCollectionSimple(cID, c);
            collectionsSimple.put(cID, null);
            r++;
            }
        }
        return r;
    }

    /**
     * For caching a subset collection.
     *
     * @param cID the ID of subset collection to be cached.
     * @param o the subset collection to be cached.
     */
    public void cacheSubsetCollection(WaAS_CollectionID cID, Object o) {
        cache(getSubsetCollectionFile(cID), o);
    }

    /**
     * For caching a subset collection.
     *
     * @param cID the ID of subset collection to be cached.
     * @param o the subset collection to be cached.
     */
    public void cacheSubsetCollectionSimple(WaAS_CollectionID cID, Object o) {
        cache(getSubsetCollectionSimpleFile(cID), o);
    }

    /**
     * For loading a subset collection.
     *
     * @param cID the ID of subset collection to be loaded.
     * @return the subset collection loaded.
     */
    public Object loadSubsetCollection(WaAS_CollectionID cID) {
        return load(getSubsetCollectionFile(cID));
    }

    /**
     * For loading a subset collection.
     *
     * @param cID the ID of subset collection to be loaded.
     * @return the subset collection loaded.
     */
    public Object loadSubsetCollectionSimple(WaAS_CollectionID cID) {
        return load(getSubsetCollectionSimpleFile(cID));
    }

    /**
     * For getting a subset collection file.
     *
     * @param cID the ID of subset collection.
     * @return the subset collection file for cID.
     */
    public File getSubsetCollectionFile(WaAS_CollectionID cID) {
        return new File(env.files.getGeneratedWaASSubsetsDir(),
                env.strings.s_WaAS + env.strings.symbol_underscore + cID
                + env.files.DOT_DAT);
    }
    
    /**
     * For getting a subset collection simple file.
     *
     * @param cID the ID of subset collection.
     * @return the subset collection file for cID.
     */
    public File getSubsetCollectionSimpleFile(WaAS_CollectionID cID) {
        return new File(env.files.getGeneratedWaASSubsetsDir(),
                env.strings.s_WaAS + env.strings.symbol_underscore
                + env.strings.s_Simple + cID + env.files.DOT_DAT);
    }

    /**
     * Loads an Object from a File and reports this to the log.
     *
     * @param f the File to load an object from.
     * @return the object loaded.
     */
    protected Object load(File f) {
        String m = "load object from " + f.toString();
        env.logStartTag(m);
        Object r = env.ge.io.readObject(f);
        env.logEndTag(m);
        return r;
    }

    /**
     * Caches an Object to a File and reports this to the log.
     *
     * @param f the File to cache to.
     * @param o the Object to cache.
     */
    protected void cache(File f, Object o) {
        String m = "cache object to " + f.toString();
        env.logStartTag(m);
        env.ge.io.writeObject(o, f);
        env.logEndTag(m);
    }

}
