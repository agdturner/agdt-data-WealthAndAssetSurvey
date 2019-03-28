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

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Object;
import uk.ac.leeds.ccg.andyt.generic.io.Generic_IO;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Strings;
import uk.ac.leeds.ccg.andyt.generic.data.waas.io.WaAS_Files;

/**
 *
 * @author geoagdt
 */
public class WaAS_Data extends WaAS_Object {

    /**
     * Stores the number of waves in the WaAS
     */
    public static final byte NWAVES = 5;
    public static final byte W1 = 1;
    public static final byte W2 = 2;
    public static final byte W3 = 3;
    public static final byte W4 = 4;
    public static final byte W5 = 5;

    /**
     * The main WaAS data store. Keys are Collection IDs.
     */
    public HashMap<Short, WaAS_Collection> data;

    /**
     * The main WaAS data store. Keys are Collection IDs.
     */
    public HashMap<Short, WaAS_CollectionSimple> dataSimple;

    /**
     * Looks up from a CASEW1 to the Collection ID.
     */
    public HashMap<Short, Short> CASEW1ToCID;

    public WaAS_Data(WaAS_Environment we) {
        super(we);
        data = new HashMap<>();
        dataSimple = new HashMap<>();
        CASEW1ToCID = new HashMap<>();
    }

    /**
     *
     * @param cID collectionID
     * @return
     */
    public WaAS_Collection getCollection(short cID) {
        WaAS_Collection r = data.get(cID);
        if (r == null) {
            r = (WaAS_Collection) loadSubsetCollection(cID);
            data.put(cID, r);
        }
        return r;
    }

    /**
     *
     * @param cID collectionID
     * @return
     */
    public WaAS_CollectionSimple getCollectionSimple(short cID) {
        WaAS_CollectionSimple r = dataSimple.get(cID);
        if (r == null) {
            r = (WaAS_CollectionSimple) loadSubsetCollectionSimple(cID);
            dataSimple.put(cID, r);
        }
        return r;
    }

    /**
     * Sets the value for cID in {@link #data} to {@code null}.
     *
     * @param cID the ID of the subset collection to be set to {@code null}.
     */
    public void clearCollection(short cID) {
        data.put(cID, null);
    }

    /**
     * Sets the value for cID in {@link #dataSimple} to {@code null}.
     *
     * @param cID the ID of the subset collection to be set to {@code null}.
     */
    public void clearCollectionSimple(short cID) {
        dataSimple.put(cID, null);
    }

    /**
     * Caches and clears the first subset collection retrieved from an iterator.
     *
     * @return {@code true} iff a subset collection was cached and cleared.
     */
    public boolean clearSomeData() {
        Iterator<Short> ite = data.keySet().iterator();
        while (ite.hasNext()) {
            short cID = ite.next();
            WaAS_Collection c = data.get(cID);
            cacheSubsetCollection(cID, c);
            data.put(cID, null);
            return true;
        }
        ite = dataSimple.keySet().iterator();
        while (ite.hasNext()) {
            short cID = ite.next();
            WaAS_CollectionSimple c = dataSimple.get(cID);
            cacheSubsetCollectionSimple(cID, c);
            dataSimple.put(cID, null);
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
        Iterator<Short> ite = data.keySet().iterator();
        while (ite.hasNext()) {
            short cID = ite.next();
            WaAS_Collection c = data.get(cID);
            cacheSubsetCollection(cID, c);
            data.put(cID, null);
            r++;
        }
        ite = dataSimple.keySet().iterator();
        while (ite.hasNext()) {
            short cID = ite.next();
            WaAS_CollectionSimple c = dataSimple.get(cID);
            cacheSubsetCollectionSimple(cID, c);
            dataSimple.put(cID, null);
            r++;
        }
        return r;
    }

    /**
     * For caching a subset collection.
     *
     * @param cID the ID of subset collection to be cached.
     * @param o the subset collection to be cached.
     */
    public void cacheSubsetCollection(short cID, Object o) {
        cache(getWaASSubsetCollectionFile(cID), o);
    }

    /**
     * For caching a subset collection.
     *
     * @param cID the ID of subset collection to be cached.
     * @param o the subset collection to be cached.
     */
    public void cacheSubsetCollectionSimple(short cID, Object o) {
        cache(getWaASSubsetCollectionSimpleFile(cID), o);
    }

    /**
     * For loading a subset collection.
     *
     * @param cID the ID of subset collection to be loaded.
     * @return the subset collection loaded.
     */
    public Object loadSubsetCollection(short cID) {
        return load(getWaASSubsetCollectionFile(cID));
    }

    /**
     * For loading a subset collection.
     *
     * @param cID the ID of subset collection to be loaded.
     * @return the subset collection loaded.
     */
    public Object loadSubsetCollectionSimple(short cID) {
        return load(getWaASSubsetCollectionSimpleFile(cID));
    }

    /**
     * For getting a subset collection file.
     *
     * @param cID the ID of subset collection.
     * @return the subset collection file for cID.
     */
    public File getWaASSubsetCollectionFile(short cID) {
        return new File(env.files.getGeneratedWaASSubsetsDir(),
                WaAS_Strings.s_WaAS + WaAS_Strings.symbol_underscore + cID
                + WaAS_Files.DOT_DAT);
    }
    
    /**
     * For getting a subset collection simple file.
     *
     * @param cID the ID of subset collection.
     * @return the subset collection file for cID.
     */
    public File getWaASSubsetCollectionSimpleFile(short cID) {
        return new File(env.files.getGeneratedWaASSubsetsDir(),
                WaAS_Strings.s_WaAS + WaAS_Strings.symbol_underscore
                + WaAS_Strings.s_Simple + cID + WaAS_Files.DOT_DAT);
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
        Object r = Generic_IO.readObject(f);
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
        Generic_IO.writeObject(o, f);
        env.logEndTag(m);
    }

}
