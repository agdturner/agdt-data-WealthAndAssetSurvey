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
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W5ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W2ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W3ID;
import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import uk.ac.leeds.ccg.andyt.data.Data_Collection;
import uk.ac.leeds.ccg.andyt.data.Data_CollectionID;
import uk.ac.leeds.ccg.andyt.data.Data_Data;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Strings;

/**
 *
 * @author geoagdt
 */
public class WaAS_Data extends Data_Data {

    /**
     * A reference to the WaAS_Environment. This cannot be final.
     */
    public WaAS_Environment we;

    /**
     * For looking up a Data_CollectionID from an int.
     */
    public final HashMap<Integer, Data_CollectionID> cIDs;

    /**
     * For storing the number of Collections.
     */
    public int nOC;

    /**
     * The main WaAS data store in simple collections. Keys are Collection IDs.
     */
    public final HashMap<Data_CollectionID, Data_Collection> dataSimple;

    /**
     * Looks up from a CASEW1 to the Collection ID.
     */
    public final HashMap<WaAS_W1ID, Data_CollectionID> w1_To_c;

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
        super(e.de);
        we = e;
        //data = new HashMap<>();
        dataSimple = new HashMap<>();
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
    @Override
    public Data_Collection getCollection(Data_CollectionID cID) {
        Data_Collection r = (Data_Collection) data.get(cID);
        if (r == null) {
            r = (Data_Collection) loadSubsetCollection(cID);
            data.put(cID, r);
        }
        return r;
    }

    /**
     *
     * @param cID collectionID
     * @return
     */
    public Data_Collection getCollectionSimple(Data_CollectionID cID) {
        Data_Collection r = dataSimple.get(cID);
        if (r == null) {
            r = (Data_Collection) loadSubsetCollectionSimple(cID);
            dataSimple.put(cID, r);
        }
        return r;
    }

    /**
     * Sets the value for cID in {@link #data} to {@code null}.
     *
     * @param cID the ID of the subset collection to be set to {@code null}.
     */
    @Override
    public void clearCollection(Data_CollectionID cID) {
        String m = "clearCollection " + cID;
        we.logStartTagMem(m);
        data.put(cID, null);
        we.logStartTagMem(m);
    }

    /**
     * Sets the value for cID in {@link #dataSimple} to {@code null}.
     *
     * @param cID the ID of the subset collection to be set to {@code null}.
     */
    public void clearCollectionSimple(Data_CollectionID cID) {
        dataSimple.put(cID, null);
    }

    /**
     * Caches and clears the first subset collection retrieved from an iterator.
     *
     * @return {@code true} iff a subset collection was cached and cleared.
     */
    @Override
    public boolean clearSomeData() {
        if (super.clearSomeData()) {
            return true;
        } else {
            Iterator<Data_CollectionID> ite;
            ite = dataSimple.keySet().iterator();
            while (ite.hasNext()) {
                Data_CollectionID cID = ite.next();
                Data_Collection c = dataSimple.get(cID);
                if (c != null) {
                    cacheSubsetCollectionSimple(cID, c);
                    dataSimple.put(cID, null);
                }
                return true;
            }
            return false;
        }
    }

    /**
     * Caches and cleared all subset data.
     *
     * @return The number of subset data cached and cleared.
     */
    @Override
    public int clearAllData() {
        int r = 0;
        Iterator<Data_CollectionID> ite = data.keySet().iterator();
        while (ite.hasNext()) {
            Data_CollectionID cID = ite.next();
            Data_Collection c = (Data_Collection) data.get(cID);
            if (c != null) {
                cacheSubsetCollection(cID, c);
                data.put(cID, null);
                r++;
            }
        }
        ite = dataSimple.keySet().iterator();
        while (ite.hasNext()) {
            Data_CollectionID cID = ite.next();
            Data_Collection c = dataSimple.get(cID);
            if (c != null) {
                cacheSubsetCollectionSimple(cID, c);
                dataSimple.put(cID, null);
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
    public void cacheSubsetCollection(Data_CollectionID cID, Object o) {
        cache(getSubsetCollectionFile(cID), o);
    }

    /**
     * For caching a subset collection.
     *
     * @param cID the ID of subset collection to be cached.
     * @param o the subset collection to be cached.
     */
    public void cacheSubsetCollectionSimple(Data_CollectionID cID, Object o) {
        cache(getSubsetCollectionSimpleFile(cID), o);
    }

    /**
     * For loading a subset collection.
     *
     * @param cID the ID of subset collection to be loaded.
     * @return the subset collection loaded.
     */
    public Object loadSubsetCollection(Data_CollectionID cID) {
        return load(getSubsetCollectionFile(cID));
    }

    /**
     * For loading a subset collection.
     *
     * @param cID the ID of subset collection to be loaded.
     * @return the subset collection loaded.
     */
    public Object loadSubsetCollectionSimple(Data_CollectionID cID) {
        return load(getSubsetCollectionSimpleFile(cID));
    }

    /**
     * For getting a subset collection file.
     *
     * @param cID the ID of subset collection.
     * @return the subset collection file for cID.
     */
    public File getSubsetCollectionFile(Data_CollectionID cID) {
        return new File(we.files.getGeneratedWaASSubsetsDir(),
                WaAS_Strings.s_WaAS + WaAS_Strings.symbol_underscore + cID
                + we.files.DOT_DAT);
    }

    /**
     * For getting a subset collection simple file.
     *
     * @param cID the ID of subset collection.
     * @return the subset collection file for cID.
     */
    public File getSubsetCollectionSimpleFile(Data_CollectionID cID) {
        return new File(we.files.getGeneratedWaASSubsetsDir(),
                WaAS_Strings.s_WaAS + WaAS_Strings.symbol_underscore
                + WaAS_Strings.s_Simple + cID + we.files.DOT_DAT);
    }

}
