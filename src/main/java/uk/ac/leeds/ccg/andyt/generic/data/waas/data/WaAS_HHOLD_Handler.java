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

import java.io.BufferedReader;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.andyt.generic.io.Generic_IO;
import uk.ac.leeds.ccg.andyt.generic.util.Generic_Collections;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Strings;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave1_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave2_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave3_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave4_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave5_HHOLD_Record;

/**
 *
 * @author geoagdt
 */
public class WaAS_HHOLD_Handler extends WaAS_Handler {

    public WaAS_HHOLD_Handler(WaAS_Environment e, File indir) {
        super(e);
        TYPE = "hhold";
        INDIR = indir;
    }

    /**
     * Loads hhold WaAS records that are in all waves.
     *
     * @return an Object[] r with size 5. Each element is an Object[] containing
     * the data from loading each wave. The first element of these are TreeMaps
     * where the keys are the CASEWX for the wave and the values are the hhold
     * records for that specific wave. The second element of these are first
     * element of these are the ..
     */
    public Object[] load() {
        String m = "load";
        env.logStartTag(m);
        Object[] r = new Object[5];
        /**
         * Step 1: Initial loading
         */
        /**
         * Step 1.1: Wave 5 initial load. After this load the main set of data
         * contains all those Wave 5 records that have Wave 4 record
         * identifiers.
         *
         */
        r[4] = loadWave5(WaAS_Data.W5);
        TreeMap<Short, HashSet<Short>> CASEW4ToCASEW5;
        CASEW4ToCASEW5 = (TreeMap<Short, HashSet<Short>>) ((Object[]) r[4])[3];
        /**
         * Step 1.2: Wave 4 initial load. After this load the main set of data
         * contains all those Wave 4 records that have Wave 3 record identifiers
         * and that are in the main set loaded in Step 1.1.
         */
        r[3] = loadWave4(WaAS_Data.W4, CASEW4ToCASEW5.keySet());
        TreeMap<Short, HashSet<Short>> CASEW3ToCASEW4;
        CASEW3ToCASEW4 = (TreeMap<Short, HashSet<Short>>) ((Object[]) r[3])[3];
        /**
         * Step 1.3: Wave 3 initial load. After this load the main set of data
         * contains all those Wave 3 records that have Wave 2 record identifiers
         * and that are in the main set loaded in Step 1.2.
         */
        r[2] = loadWave3(WaAS_Data.W3, CASEW3ToCASEW4.keySet());
        TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3;
        CASEW2ToCASEW3 = (TreeMap<Short, HashSet<Short>>) ((Object[]) r[2])[3];
        /**
         * Step 1.4: Wave 2 initial load. After this load the main set of data
         * contains all those Wave 2 records that have Wave 1 record identifiers
         * and that are in the main set loaded in Step 1.3.
         */
        r[1] = loadWave2(WaAS_Data.W2, CASEW2ToCASEW3.keySet());
        TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2;
        CASEW1ToCASEW2 = (TreeMap<Short, HashSet<Short>>) ((Object[]) r[1])[3];
        TreeMap<Short, Short> CASEW2ToCASEW1;
        CASEW2ToCASEW1 = (TreeMap<Short, Short>) ((Object[]) r[1])[2];
        /**
         * Step 1.5: Wave 1 initial load. After this load the main set of data
         * contains all those Wave 1 records that are in the main set loaded in
         * Step 1.4.
         */
        r[0] = loadWave1(WaAS_Data.W1, CASEW1ToCASEW2.keySet());
        /**
         * Step 2: Check what is loaded and go through creating ID sets.
         */
        /**
         * Step 2.1: Log status of the main sets loaded in Step 1.
         */
        TreeMap<Short, WaAS_Wave1_HHOLD_Record> w1recs0;
        w1recs0 = (TreeMap<Short, WaAS_Wave1_HHOLD_Record>) ((Object[]) r[0])[0];
        env.log("There are " + w1recs0.size() + " w1recs.");
        TreeMap<Short, WaAS_Wave2_HHOLD_Record> w2recs0;
        w2recs0 = (TreeMap<Short, WaAS_Wave2_HHOLD_Record>) ((Object[]) r[1])[0];
        env.log("There are " + w2recs0.size() + " w2recs.");
        TreeMap<Short, WaAS_Wave3_HHOLD_Record> w3recs0;
        w3recs0 = (TreeMap<Short, WaAS_Wave3_HHOLD_Record>) ((Object[]) r[2])[0];
        env.log("There are " + w3recs0.size() + " w3recs.");
        TreeMap<Short, WaAS_Wave4_HHOLD_Record> w4recs0;
        w4recs0 = (TreeMap<Short, WaAS_Wave4_HHOLD_Record>) ((Object[]) r[3])[0];
        env.log("There are " + w4recs0.size() + " w4recs.");
        TreeMap<Short, WaAS_Wave5_HHOLD_Record> w5recs0;
        w5recs0 = (TreeMap<Short, WaAS_Wave5_HHOLD_Record>) ((Object[]) r[4])[0];
        env.log("There are " + w5recs0.size() + " w5recs.");
        /**
         * Step 2.2: Filter sets.
         */
        Iterator<Short> ite;
        WaAS_ID ID;
        short CASEW1;
        short CASEW2;
        short CASEW3;
        short CASEW4;
        short CASEW5;
        /**
         * Step 2.2.1: Wave 1.
         */
        TreeMap<Short, WaAS_Wave1_HHOLD_Record> w1recs;
        w1recs = new TreeMap<>();
        TreeSet<WaAS_ID> CASEW1IDs;
        CASEW1IDs = new TreeSet<>();
        ite = w1recs0.keySet().iterator();
        while (ite.hasNext()) {
            CASEW1 = ite.next();
            ID = new WaAS_ID(CASEW1, CASEW1);
            CASEW1IDs.add(ID);
            w1recs.put(CASEW1, w1recs0.get(CASEW1));
        }
        env.log("There are " + CASEW1IDs.size() + " CASEW1IDs.");
        cacheSubset(WaAS_Data.W1, w1recs);
        /**
         * Step 2.2.2: Wave 2.
         */
        TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2Subset;
        CASEW1ToCASEW2Subset = new TreeMap<>();
        TreeMap<Short, Short> CASEW2ToCASEW1Subset;
        CASEW2ToCASEW1Subset = new TreeMap<>();
        TreeMap<Short, WaAS_Wave2_HHOLD_Record> w2recs;
        w2recs = new TreeMap<>();
        TreeSet<WaAS_ID> CASEW2IDs;
        CASEW2IDs = new TreeSet<>();
        ite = w2recs0.keySet().iterator();
        while (ite.hasNext()) {
            CASEW2 = ite.next();
            CASEW1 = CASEW2ToCASEW1.get(CASEW2);
            ID = new WaAS_ID(CASEW1, CASEW2);
            CASEW2IDs.add(ID);
            w2recs.put(CASEW2, w2recs0.get(CASEW2));
            Generic_Collections.addToMap(CASEW1ToCASEW2Subset, CASEW1, CASEW2);
            CASEW2ToCASEW1Subset.put(CASEW2, CASEW1);
        }
        env.log("There are " + CASEW2IDs.size() + " CASEW2IDs.");
        cacheSubset(WaAS_Data.W2, w2recs);
        env.log("There are " + CASEW1ToCASEW2Subset.size() + " CASEW1ToCASEW2Subset mappings.");
        env.log("There are " + CASEW2ToCASEW1Subset.size() + " CASEW2ToCASEW1Subset mappings.");
        cacheSubsetLookups(WaAS_Data.W1, CASEW1ToCASEW2Subset, CASEW2ToCASEW1Subset);
        /**
         * Step 2.2.3: Wave 3.
         */
        TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3Subset;
        CASEW2ToCASEW3Subset = new TreeMap<>();
        TreeMap<Short, Short> CASEW3ToCASEW2Subset;
        CASEW3ToCASEW2Subset = new TreeMap<>();
        TreeMap<Short, WaAS_Wave3_HHOLD_Record> w3recs;
        w3recs = new TreeMap<>();
        TreeSet<WaAS_ID> CASEW3IDs;
        HashMap<Short, Short> CASEW3ToCASEW2;
        CASEW3ToCASEW2 = new HashMap<>();
        CASEW3IDs = new TreeSet<>();
        ite = w3recs0.keySet().iterator();
        while (ite.hasNext()) {
            CASEW3 = ite.next();
            CASEW2 = w3recs0.get(CASEW3).getCASEW2();
            if (CASEW2ToCASEW1Subset.containsKey(CASEW2)) {
                CASEW3ToCASEW2.put(CASEW3, CASEW2);
                CASEW1 = CASEW2ToCASEW1Subset.get(CASEW2);
                HashSet<Short> CASEW3s;
                CASEW3s = CASEW2ToCASEW3.get(CASEW2);
                Iterator<Short> ite2;
                ite2 = CASEW3s.iterator();
                while (ite2.hasNext()) {
                    CASEW3 = ite2.next();
                    ID = new WaAS_ID(CASEW1, CASEW3);
                    CASEW3IDs.add(ID);
                    w3recs.put(CASEW3, w3recs0.get(CASEW3));
                    Generic_Collections.addToMap(CASEW2ToCASEW3Subset, CASEW2, CASEW3);
                    CASEW3ToCASEW2Subset.put(CASEW3, CASEW2);
                }
            }
        }
        env.log("There are " + CASEW3IDs.size() + " CASEW3IDs.");
        cacheSubset(WaAS_Data.W3, w3recs);
        env.log("There are " + CASEW2ToCASEW3Subset.size() + " CASEW2ToCASEW3Subset mappings.");
        env.log("There are " + CASEW3ToCASEW2Subset.size() + " CASEW3ToCASEW2Subset mappings.");
        cacheSubsetLookups(WaAS_Data.W2, CASEW2ToCASEW3Subset, CASEW3ToCASEW2Subset);
        /**
         * Step 2.2.4: Wave 4.
         */
        TreeMap<Short, HashSet<Short>> CASEW3ToCASEW4Subset;
        CASEW3ToCASEW4Subset = new TreeMap<>();
        TreeMap<Short, Short> CASEW4ToCASEW3Subset;
        CASEW4ToCASEW3Subset = new TreeMap<>();
        TreeMap<Short, WaAS_Wave4_HHOLD_Record> w4recs;
        w4recs = new TreeMap<>();
        HashMap<Short, Short> CASEW4ToCASEW3;
        CASEW4ToCASEW3 = new HashMap<>();
        TreeSet<WaAS_ID> CASEW4IDs;
        CASEW4IDs = new TreeSet<>();
        ite = w4recs0.keySet().iterator();
        while (ite.hasNext()) {
            CASEW4 = ite.next();
            CASEW3 = w4recs0.get(CASEW4).getCASEW3();
            if (CASEW3ToCASEW2Subset.containsKey(CASEW3)) {
                CASEW4ToCASEW3.put(CASEW4, CASEW3);
                CASEW2 = CASEW3ToCASEW2Subset.get(CASEW3);
                CASEW1 = CASEW2ToCASEW1Subset.get(CASEW2);
                HashSet<Short> CASEW4s;
                CASEW4s = CASEW3ToCASEW4.get(CASEW3);
                Iterator<Short> ite2;
                ite2 = CASEW4s.iterator();
                while (ite2.hasNext()) {
                    CASEW4 = ite2.next();
                    ID = new WaAS_ID(CASEW1, CASEW4);
                    CASEW4IDs.add(ID);
                    w4recs.put(CASEW4, w4recs0.get(CASEW4));
                    Generic_Collections.addToMap(CASEW3ToCASEW4Subset, CASEW3, CASEW4);
                    CASEW4ToCASEW3Subset.put(CASEW4, CASEW3);
                }
            }
        }
        env.log("There are " + CASEW4IDs.size() + " CASEW4IDs.");
        cacheSubset(WaAS_Data.W4, w4recs);
        env.log("There are " + CASEW3ToCASEW4Subset.size() + " CASEW3ToCASEW4Subset mappings.");
        env.log("There are " + CASEW4ToCASEW3Subset.size() + " CASEW4ToCASEW3Subset mappings.");
        cacheSubsetLookups(WaAS_Data.W3, CASEW3ToCASEW4Subset, CASEW4ToCASEW3Subset);
        /**
         * Step 2.2.5: Wave 5.
         */
        TreeMap<Short, HashSet<Short>> CASEW4ToCASEW5Subset;
        CASEW4ToCASEW5Subset = new TreeMap<>();
        TreeMap<Short, Short> CASEW5ToCASEW4Subset;
        CASEW5ToCASEW4Subset = new TreeMap<>();
        TreeMap<Short, WaAS_Wave5_HHOLD_Record> w5recs;
        w5recs = new TreeMap<>();
        HashMap<Short, Short> CASEW5ToCASEW4;
        CASEW5ToCASEW4 = new HashMap<>();
        TreeSet<WaAS_ID> CASEW5IDs;
        CASEW5IDs = new TreeSet<>();
        ite = w5recs0.keySet().iterator();
        while (ite.hasNext()) {
            CASEW5 = ite.next();
            CASEW4 = w5recs0.get(CASEW5).getCASEW4();
            if (CASEW4ToCASEW3Subset.containsKey(CASEW4)) {
                CASEW5ToCASEW4.put(CASEW5, CASEW4);
                CASEW3 = CASEW4ToCASEW3Subset.get(CASEW4);
                CASEW2 = CASEW3ToCASEW2Subset.get(CASEW3);
                CASEW1 = CASEW2ToCASEW1Subset.get(CASEW2);
                HashSet<Short> CASEW5s;
                CASEW5s = CASEW4ToCASEW5.get(CASEW4);
                Iterator<Short> ite2;
                ite2 = CASEW5s.iterator();
                while (ite2.hasNext()) {
                    CASEW5 = ite2.next();
                    ID = new WaAS_ID(CASEW1, CASEW5);
                    CASEW5IDs.add(ID);
                    w5recs.put(CASEW5, w5recs0.get(CASEW5));
                    Generic_Collections.addToMap(CASEW4ToCASEW5Subset, CASEW4, CASEW5);
                    CASEW5ToCASEW4Subset.put(CASEW5, CASEW4);
                }
            }
        }
        env.log("There are " + CASEW5IDs.size() + " CASEW5IDs.");
        cacheSubset(WaAS_Data.W5, w5recs);
        env.log("There are " + CASEW4ToCASEW5Subset.size() + " CASEW4ToCASEW5Subset mappings.");
        env.log("There are " + CASEW5ToCASEW4Subset.size() + " CASEW5ToCASEW4Subset mappings.");
        cacheSubsetLookups(WaAS_Data.W4, CASEW4ToCASEW5Subset, CASEW5ToCASEW4Subset);
        env.logEndTag(m);
        return r;
    }

    /**
     * Loads all hhold WaAS from a cache in generated data if it exists and from
     * the source input data otherwise.It requires more than 4GB, but less than
     * 7GB of memory to hold all the data in memory.
     *
     * @param wave
     * @return an Object[] r with size 5. Each element is an Object[] containing
     * the data from loading each wave...
     */
    public Object[] loadAll(byte wave) {
        String m = "loadAll(" + wave + ")";
        env.logStartTag(m);
        Object[] r;
        r = new Object[5];
        /**
         * Step 1: Initial loading
         */
        /**
         * Step 1.1: Wave 5 initial load. After this load the main set of data
         * contains all those Wave 5 records that have Wave 4 record
         * identifiers.
         *
         */
        r[4] = loadWave5(WaAS_Data.W5);
        TreeMap<Short, HashSet<Short>> CASEW4ToCASEW5;
        CASEW4ToCASEW5 = (TreeMap<Short, HashSet<Short>>) ((Object[]) r[4])[3];
        /**
         * Step 1.2: Wave 4 initial load. After this load the main set of data
         * contains all those Wave 4 records that have Wave 3 record identifiers
         * and that are in the main set loaded in Step 1.1.
         */
        r[3] = loadWave4(WaAS_Data.W4, CASEW4ToCASEW5.keySet());
        TreeMap<Short, HashSet<Short>> CASEW3ToCASEW4;
        CASEW3ToCASEW4 = (TreeMap<Short, HashSet<Short>>) ((Object[]) r[3])[3];
        /**
         * Step 1.3: Wave 3 initial load. After this load the main set of data
         * contains all those Wave 3 records that have Wave 2 record identifiers
         * and that are in the main set loaded in Step 1.2.
         */
        r[2] = loadWave3(WaAS_Data.W3, CASEW3ToCASEW4.keySet());
        TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3;
        CASEW2ToCASEW3 = (TreeMap<Short, HashSet<Short>>) ((Object[]) r[2])[3];
        /**
         * Step 1.4: Wave 2 initial load. After this load the main set of data
         * contains all those Wave 2 records that have Wave 1 record identifiers
         * and that are in the main set loaded in Step 1.3.
         */
        r[1] = loadWave2(WaAS_Data.W2, CASEW2ToCASEW3.keySet());
        TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2;
        CASEW1ToCASEW2 = (TreeMap<Short, HashSet<Short>>) ((Object[]) r[1])[3];
        TreeMap<Short, Short> CASEW2ToCASEW1;
        CASEW2ToCASEW1 = (TreeMap<Short, Short>) ((Object[]) r[1])[2];
        /**
         * Step 1.5: Wave 1 initial load. After this load the main set of data
         * contains all those Wave 1 records that are in the main set loaded in
         * Step 1.4.
         */
        r[0] = loadWave1(WaAS_Data.W1, CASEW1ToCASEW2.keySet());
        /**
         * Step 2: Check what is loaded and go through creating ID sets.
         */
        /**
         * Step 2.1: Log status of the main sets loaded in Step 1.
         */
        TreeMap<Short, WaAS_Wave1_HHOLD_Record> w1recs0;
        w1recs0 = (TreeMap<Short, WaAS_Wave1_HHOLD_Record>) ((Object[]) r[0])[0];
        env.log("There are " + w1recs0.size() + " w1recs.");
        TreeMap<Short, WaAS_Wave2_HHOLD_Record> w2recs0;
        w2recs0 = (TreeMap<Short, WaAS_Wave2_HHOLD_Record>) ((Object[]) r[1])[0];
        env.log("There are " + w2recs0.size() + " w2recs.");
        TreeMap<Short, WaAS_Wave3_HHOLD_Record> w3recs0;
        w3recs0 = (TreeMap<Short, WaAS_Wave3_HHOLD_Record>) ((Object[]) r[2])[0];
        env.log("There are " + w3recs0.size() + " w3recs.");
        TreeMap<Short, WaAS_Wave4_HHOLD_Record> w4recs0;
        w4recs0 = (TreeMap<Short, WaAS_Wave4_HHOLD_Record>) ((Object[]) r[3])[0];
        env.log("There are " + w4recs0.size() + " w4recs.");
        TreeMap<Short, WaAS_Wave5_HHOLD_Record> w5recs0;
        w5recs0 = (TreeMap<Short, WaAS_Wave5_HHOLD_Record>) ((Object[]) r[4])[0];
        env.log("There are " + w5recs0.size() + " w5recs.");
        /**
         * Step 2.2: Filter sets.
         */
        Iterator<Short> ite;
        WaAS_ID ID;
        short CASEW1;
        short CASEW2;
        short CASEW3;
        short CASEW4;
        short CASEW5;
        /**
         * Step 2.2.1: Wave 1.
         */
        TreeMap<Short, WaAS_Wave1_HHOLD_Record> w1recs;
        w1recs = new TreeMap<>();
        TreeSet<WaAS_ID> CASEW1IDs;
        CASEW1IDs = new TreeSet<>();
        ite = w1recs0.keySet().iterator();
        while (ite.hasNext()) {
            CASEW1 = ite.next();
            ID = new WaAS_ID(CASEW1, CASEW1);
            CASEW1IDs.add(ID);
            w1recs.put(CASEW1, w1recs0.get(CASEW1));
        }
        env.log("There are " + CASEW1IDs.size() + " CASEW1IDs.");
        cacheSubset(WaAS_Data.W1, w1recs);
        /**
         * Step 2.2.2: Wave 2.
         */
        TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2Subset;
        CASEW1ToCASEW2Subset = new TreeMap<>();
        TreeMap<Short, Short> CASEW2ToCASEW1Subset;
        CASEW2ToCASEW1Subset = new TreeMap<>();
        TreeMap<Short, WaAS_Wave2_HHOLD_Record> w2recs;
        w2recs = new TreeMap<>();
        TreeSet<WaAS_ID> CASEW2IDs;
        CASEW2IDs = new TreeSet<>();
        ite = w2recs0.keySet().iterator();
        while (ite.hasNext()) {
            CASEW2 = ite.next();
            CASEW1 = CASEW2ToCASEW1.get(CASEW2);
            ID = new WaAS_ID(CASEW1, CASEW2);
            CASEW2IDs.add(ID);
            w2recs.put(CASEW2, w2recs0.get(CASEW2));
            Generic_Collections.addToMap(CASEW1ToCASEW2Subset, CASEW1, CASEW2);
            CASEW2ToCASEW1Subset.put(CASEW2, CASEW1);
        }
        env.log("There are " + CASEW2IDs.size() + " CASEW2IDs.");
        cacheSubset(WaAS_Data.W2, w2recs);
        env.log("There are " + CASEW1ToCASEW2Subset.size() + " CASEW1ToCASEW2Subset mappings.");
        env.log("There are " + CASEW2ToCASEW1Subset.size() + " CASEW2ToCASEW1Subset mappings.");
        cacheSubsetLookups(WaAS_Data.W1, CASEW1ToCASEW2Subset, CASEW2ToCASEW1Subset);
        /**
         * Step 2.2.3: Wave 3.
         */
        TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3Subset;
        CASEW2ToCASEW3Subset = new TreeMap<>();
        TreeMap<Short, Short> CASEW3ToCASEW2Subset;
        CASEW3ToCASEW2Subset = new TreeMap<>();
        TreeMap<Short, WaAS_Wave3_HHOLD_Record> w3recs;
        w3recs = new TreeMap<>();
        TreeSet<WaAS_ID> CASEW3IDs;
        HashMap<Short, Short> CASEW3ToCASEW2;
        CASEW3ToCASEW2 = new HashMap<>();
        CASEW3IDs = new TreeSet<>();
        ite = w3recs0.keySet().iterator();
        while (ite.hasNext()) {
            CASEW3 = ite.next();
            CASEW2 = w3recs0.get(CASEW3).getCASEW2();
            if (CASEW2ToCASEW1Subset.containsKey(CASEW2)) {
                CASEW3ToCASEW2.put(CASEW3, CASEW2);
                CASEW1 = CASEW2ToCASEW1Subset.get(CASEW2);
                HashSet<Short> CASEW3s;
                CASEW3s = CASEW2ToCASEW3.get(CASEW2);
                Iterator<Short> ite2;
                ite2 = CASEW3s.iterator();
                while (ite2.hasNext()) {
                    CASEW3 = ite2.next();
                    ID = new WaAS_ID(CASEW1, CASEW3);
                    CASEW3IDs.add(ID);
                    w3recs.put(CASEW3, w3recs0.get(CASEW3));
                    Generic_Collections.addToMap(CASEW2ToCASEW3Subset, CASEW2, CASEW3);
                    CASEW3ToCASEW2Subset.put(CASEW3, CASEW2);
                }
            }
        }
        env.log("There are " + CASEW3IDs.size() + " CASEW3IDs.");
        cacheSubset(WaAS_Data.W3, w3recs);
        env.log("There are " + CASEW2ToCASEW3Subset.size() + " CASEW2ToCASEW3Subset mappings.");
        env.log("There are " + CASEW3ToCASEW2Subset.size() + " CASEW3ToCASEW2Subset mappings.");
        cacheSubsetLookups(WaAS_Data.W2, CASEW2ToCASEW3Subset, CASEW3ToCASEW2Subset);
        /**
         * Step 2.2.4: Wave 4.
         */
        TreeMap<Short, HashSet<Short>> CASEW3ToCASEW4Subset;
        CASEW3ToCASEW4Subset = new TreeMap<>();
        TreeMap<Short, Short> CASEW4ToCASEW3Subset;
        CASEW4ToCASEW3Subset = new TreeMap<>();
        TreeMap<Short, WaAS_Wave4_HHOLD_Record> w4recs;
        w4recs = new TreeMap<>();
        HashMap<Short, Short> CASEW4ToCASEW3;
        CASEW4ToCASEW3 = new HashMap<>();
        TreeSet<WaAS_ID> CASEW4IDs;
        CASEW4IDs = new TreeSet<>();
        ite = w4recs0.keySet().iterator();
        while (ite.hasNext()) {
            CASEW4 = ite.next();
            CASEW3 = w4recs0.get(CASEW4).getCASEW3();
            if (CASEW3ToCASEW2Subset.containsKey(CASEW3)) {
                CASEW4ToCASEW3.put(CASEW4, CASEW3);
                CASEW2 = CASEW3ToCASEW2Subset.get(CASEW3);
                CASEW1 = CASEW2ToCASEW1Subset.get(CASEW2);
                HashSet<Short> CASEW4s;
                CASEW4s = CASEW3ToCASEW4.get(CASEW3);
                Iterator<Short> ite2;
                ite2 = CASEW4s.iterator();
                while (ite2.hasNext()) {
                    CASEW4 = ite2.next();
                    ID = new WaAS_ID(CASEW1, CASEW4);
                    CASEW4IDs.add(ID);
                    w4recs.put(CASEW4, w4recs0.get(CASEW4));
                    Generic_Collections.addToMap(CASEW3ToCASEW4Subset, CASEW3, CASEW4);
                    CASEW4ToCASEW3Subset.put(CASEW4, CASEW3);
                }
            }
        }
        env.log("There are " + CASEW4IDs.size() + " CASEW4IDs.");
        cacheSubset(WaAS_Data.W4, w4recs);
        env.log("There are " + CASEW3ToCASEW4Subset.size() + " CASEW3ToCASEW4Subset mappings.");
        env.log("There are " + CASEW4ToCASEW3Subset.size() + " CASEW4ToCASEW3Subset mappings.");
        cacheSubsetLookups(WaAS_Data.W3, CASEW3ToCASEW4Subset, CASEW4ToCASEW3Subset);
        /**
         * Step 2.2.5: Wave 5.
         */
        TreeMap<Short, HashSet<Short>> CASEW4ToCASEW5Subset;
        CASEW4ToCASEW5Subset = new TreeMap<>();
        TreeMap<Short, Short> CASEW5ToCASEW4Subset;
        CASEW5ToCASEW4Subset = new TreeMap<>();
        TreeMap<Short, WaAS_Wave5_HHOLD_Record> w5recs;
        w5recs = new TreeMap<>();
        HashMap<Short, Short> CASEW5ToCASEW4;
        CASEW5ToCASEW4 = new HashMap<>();
        TreeSet<WaAS_ID> CASEW5IDs;
        CASEW5IDs = new TreeSet<>();
        ite = w5recs0.keySet().iterator();
        while (ite.hasNext()) {
            CASEW5 = ite.next();
            CASEW4 = w5recs0.get(CASEW5).getCASEW4();
            if (CASEW4ToCASEW3Subset.containsKey(CASEW4)) {
                CASEW5ToCASEW4.put(CASEW5, CASEW4);
                CASEW3 = CASEW4ToCASEW3Subset.get(CASEW4);
                CASEW2 = CASEW3ToCASEW2Subset.get(CASEW3);
                CASEW1 = CASEW2ToCASEW1Subset.get(CASEW2);
                HashSet<Short> CASEW5s;
                CASEW5s = CASEW4ToCASEW5.get(CASEW4);
                Iterator<Short> ite2;
                ite2 = CASEW5s.iterator();
                while (ite2.hasNext()) {
                    CASEW5 = ite2.next();
                    ID = new WaAS_ID(CASEW1, CASEW5);
                    CASEW5IDs.add(ID);
                    w5recs.put(CASEW5, w5recs0.get(CASEW5));
                    Generic_Collections.addToMap(CASEW4ToCASEW5Subset, CASEW4, CASEW5);
                    CASEW5ToCASEW4Subset.put(CASEW5, CASEW4);
                }
            }
        }
        env.log("There are " + CASEW5IDs.size() + " CASEW5IDs.");
        cacheSubset(WaAS_Data.W5, w5recs);
        env.log("There are " + CASEW4ToCASEW5Subset.size() + " CASEW4ToCASEW5Subset mappings.");
        env.log("There are " + CASEW5ToCASEW4Subset.size() + " CASEW5ToCASEW4Subset mappings.");
        cacheSubsetLookups(WaAS_Data.W4, CASEW4ToCASEW5Subset, CASEW5ToCASEW4Subset);
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 5 records that are reportedly in Wave 4.
     *
     * @param wave
     *
     * @return r An Object[] of length 4 where:
     * <ul>
     * <li>r[0] is a TreeMap with keys as CASEW5 and values as
     * WaAS_Wave5_HHOLD_Records.</li>
     * <li>r[1] is an array of TreeSets where:
     * <ul>
     * <li>r[1][0] is a list of CASEW1 values in Wave 5 records.</li>
     * <li>r[1][1] is a list of CASEW2 values in Wave 5 records.</li>
     * <li>r[1][2] is a list of CASEW3 values in Wave 5 records.</li>
     * <li>r[1][3] is a list of CASEW4 values in Wave 5 records.</li>
     * <li>r[1][4] is a list of all CASEW5 values.</li>
     * <li>r[1][5] is a list of CASEW5 values for records that have CASEW4,
     * CASEW3, CASEW2 and CASEW1 values.</li>
     * </ul>
     * </li>
     * <li>r[2] is a TreeMap with keys as CASEW5 and values as CASEW4.</li>
     * <li>r[3] is a TreeMap with keys as CASEW4 and values as HashSets of
     * CASEW5 (which is normally expected to contain just one CASEW5).</li>
     * </ul>
     */
    public Object[] loadWave5(byte wave) {
        Object[] r;
        File cf;
        cf = getGeneratedFile(wave, "InW4");
        if (cf.exists()) {
            r = (Object[]) load(wave, cf);
        } else {
            File f;
            f = getInputFile(wave);
            r = new Object[4];
            TreeMap<Short, WaAS_Wave5_HHOLD_Record> r0;
            r0 = new TreeMap<>();
            r[0] = r0;
            TreeSet<Short>[] r1;
            r1 = getSetsNew(wave + 1);
            r[1] = r1;
            /**
             * Each hhold in Wave 5 comes from at most one hhold from Wave 4. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 4 into a hhold in Wave 5. This
             * is expected to be rare. One example explanation for this
             * happening is a someone returning to a hhold having left it.
             */
            TreeMap<Short, Short> CASEW5ToCASEW4;
            /**
             * There may be instances where hholds from Wave 4 split into two or
             * more hholds in Wave 5.
             */
            TreeMap<Short, HashSet<Short>> CASEW4ToCASEW5;
            CASEW5ToCASEW4 = new TreeMap<>();
            CASEW4ToCASEW5 = new TreeMap<>();
            r[2] = CASEW5ToCASEW4;
            r[3] = CASEW4ToCASEW5;
            String m;
            m = getMessage(wave, f);
            env.log("<" + m + ">");
            BufferedReader br;
            br = Generic_IO.getBufferedReader(f);
            int count;
            count = br.lines().skip(1).mapToInt(line -> {
                WaAS_Wave5_HHOLD_Record rec;
                rec = new WaAS_Wave5_HHOLD_Record(line);
                short CASEW4 = rec.getCASEW4();
                if (CASEW4 > Short.MIN_VALUE) {
                    if (!r1[3].add(CASEW4)) {
                        env.log("In Wave 5: hhold with CASEW4 "
                                + CASEW4 + " reportedly split into multiple "
                                + "hholds.");
                        return 1;
                    }
                }
                return 0;
            }).sum();
            env.log("There are " + count + " hholds from Wave 4 "
                    + "reportedly split into multiple hholds in Wave 5.");
            // Close and reopen br
            br = Generic_IO.closeAndGetBufferedReader(br, f);
            br.lines().skip(1).forEach(line -> {
                WaAS_Wave5_HHOLD_Record rec;
                rec = new WaAS_Wave5_HHOLD_Record(line);
                short CASEW5 = rec.getCASEW5();
                short CASEW4 = rec.getCASEW4();
                short CASEW3 = rec.getCASEW3();
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (CASEW4 > Short.MIN_VALUE) {
                    CASEW5ToCASEW4.put(CASEW5, CASEW4);
                    Generic_Collections.addToMap(CASEW4ToCASEW5, CASEW4, CASEW5);
                    r0.put(CASEW5, rec);
                }
                r1[4].add(CASEW5);
                if (CASEW3 > Short.MIN_VALUE) {
                    r1[2].add(CASEW3);
                }
                if (CASEW2 > Short.MIN_VALUE) {
                    r1[1].add(CASEW2);
                }
                if (CASEW1 > Short.MIN_VALUE) {
                    r1[0].add(CASEW1);
                    if (CASEW2 > Short.MIN_VALUE
                            && CASEW3 > Short.MIN_VALUE
                            && CASEW4 > Short.MIN_VALUE) {
                        r1[5].add(CASEW5);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.log("</" + m + ">");
            cache(wave, cf, r);
        }
        return r;
    }

    /**
     * Load All Wave 5 records.
     *
     * @param wave
     *
     * @return a TreeMap with keys as CASEW5 and values as
     * WaAS_Wave5_HHOLD_Records.
     */
    public TreeMap<Short, WaAS_Wave5_HHOLD_Record> loadAllWave5(byte wave) {
        TreeMap<Short, WaAS_Wave5_HHOLD_Record> r;
        File cf;
        cf = getGeneratedFile(wave, "All");
        if (cf.exists()) {
            r = (TreeMap<Short, WaAS_Wave5_HHOLD_Record>) load(wave, cf);
        } else {
            File f;
            f = getInputFile(wave);
            r = new TreeMap<>();
            String m;
            m = getMessage(wave, f);
            env.log("<" + m + ">");
            BufferedReader br;
            br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(line -> {
                WaAS_Wave5_HHOLD_Record rec;
                rec = new WaAS_Wave5_HHOLD_Record(line);
                short CASEW5 = rec.getCASEW5();
                r.put(CASEW5, rec);
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.log("</" + m + ">");
            cache(wave, cf, r);
        }
        return r;
    }

    /**
     * Load All Wave 4 records.
     *
     * @param wave
     *
     * @return a TreeMap with keys as CASEW4 and values as
     * WaAS_Wave4_HHOLD_Records.
     */
    public TreeMap<Short, WaAS_Wave4_HHOLD_Record> loadAllWave4(byte wave) {
        TreeMap<Short, WaAS_Wave4_HHOLD_Record> r;
        File cf;
        cf = getGeneratedFile(wave, "All");
        if (cf.exists()) {
            r = (TreeMap<Short, WaAS_Wave4_HHOLD_Record>) load(wave, cf);
        } else {
            File f;
            f = getInputFile(wave);
            r = new TreeMap<>();
            String m;
            m = getMessage(wave, f);
            env.log("<" + m + ">");
            BufferedReader br;
            br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(line -> {
                WaAS_Wave4_HHOLD_Record rec;
                rec = new WaAS_Wave4_HHOLD_Record(line);
                short CASEW4 = rec.getCASEW4();
                r.put(CASEW4, rec);
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.log("</" + m + ">");
            cache(wave, cf, r);
        }
        return r;
    }

    /**
     * Load All Wave 3 records.
     *
     * @param wave
     *
     * @return a TreeMap with keys as CASEW3 and values as
     * WaAS_Wave3_HHOLD_Records.
     */
    public TreeMap<Short, WaAS_Wave3_HHOLD_Record> loadAllWave3(byte wave) {
        TreeMap<Short, WaAS_Wave3_HHOLD_Record> r;
        File cf;
        cf = getGeneratedFile(wave, "All");
        if (cf.exists()) {
            r = (TreeMap<Short, WaAS_Wave3_HHOLD_Record>) load(wave, cf);
        } else {
            File f;
            f = getInputFile(wave);
            r = new TreeMap<>();
            String m;
            m = getMessage(wave, f);
            env.log("<" + m + ">");
            BufferedReader br;
            br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(line -> {
                WaAS_Wave3_HHOLD_Record rec;
                rec = new WaAS_Wave3_HHOLD_Record(line);
                short CASEW3 = rec.getCASEW3();
                r.put(CASEW3, rec);
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.log("</" + m + ">");
            cache(wave, cf, r);
        }
        return r;
    }

    /**
     * Load All Wave 2 records.
     *
     * @param wave
     *
     * @return a TreeMap with keys as CASEW2 and values as
     * WaAS_Wave2_HHOLD_Records.
     */
    public TreeMap<Short, WaAS_Wave2_HHOLD_Record> loadAllWave2(byte wave) {
        TreeMap<Short, WaAS_Wave2_HHOLD_Record> r;
        File cf;
        cf = getGeneratedFile(wave, "All");
        if (cf.exists()) {
            r = (TreeMap<Short, WaAS_Wave2_HHOLD_Record>) load(wave, cf);
        } else {
            File f;
            f = getInputFile(wave);
            r = new TreeMap<>();
            String m;
            m = getMessage(wave, f);
            env.log("<" + m + ">");
            BufferedReader br;
            br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(line -> {
                WaAS_Wave2_HHOLD_Record rec;
                rec = new WaAS_Wave2_HHOLD_Record(line);
                short CASEW2 = rec.getCASEW2();
                r.put(CASEW2, rec);
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.log("</" + m + ">");
            cache(wave, cf, r);
        }
        return r;
    }

    /**
     * Load All Wave 1 records.
     *
     * @param wave
     *
     * @return a TreeMap with keys as CASEW1 and values as
     * WaAS_Wave1_HHOLD_Records.
     */
    public TreeMap<Short, WaAS_Wave1_HHOLD_Record> loadAllWave1(byte wave) {
        String m = "loadAllWave1";
        env.logStartTag(m);
        TreeMap<Short, WaAS_Wave1_HHOLD_Record> r;
        File cf = getGeneratedFile(wave, "All");
        if (cf.exists()) {
            r = (TreeMap<Short, WaAS_Wave1_HHOLD_Record>) load(wave, cf);
        } else {
            r = new TreeMap<>();
            File f = getInputFile(wave);
            String m1 = getMessage(wave, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(line -> {
                WaAS_Wave1_HHOLD_Record rec = new WaAS_Wave1_HHOLD_Record(line);
                short CASEW1 = rec.getCASEW1();
                r.put(CASEW1, rec);
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(wave, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     *
     * @param wave
     * @param f
     * @return
     */
    public String getMessage(byte wave, File f) {
        return "load wave " + wave + " " + TYPE + " " + WaAS_Strings.s_WaAS
                + " from " + f;
    }

    /**
     *
     * @param wave
     * @return
     */
    protected TreeSet<WaAS_ID>[] getSets(byte wave) {
        TreeSet<WaAS_ID>[] r = new TreeSet[wave];
        for (int i = 0; i < wave; i++) {
            r[i] = new TreeSet<>();
        }
        return r;
    }

    /**
     *
     * @param n
     * @return
     */
    protected TreeSet<Short>[] getSetsNew(int n) {
        TreeSet<Short>[] r = new TreeSet[n];
        for (int i = 0; i < n; i++) {
            r[i] = new TreeSet<>();
        }
        return r;
    }

    /**
     *
     * @param wave
     * @param type Expected "" or "All"
     * @return
     */
    protected File getGeneratedFile(short wave, String type) {
        return new File(Files.getGeneratedWaASDir(), TYPE + WaAS_Strings.s_W
                + wave + type + WaAS_Strings.symbol_dot + WaAS_Strings.s_dat);
    }

    /**
     * Load Wave 4 records, that have Wave 5 records, and that are reportedly in
     * Wave 3.
     *
     * @param wave
     * @param s A set containing CASEW4 values from Wave 5.
     *
     * @return r - An Object[] of length 4:
     * <ul>
     * <li>r[0] is a TreeMap with keys as CASEW4 and values as
     * WaAS_Wave4_HHOLD_Records. It contains all CASEW4 values for records with
     * CASEW3 values and that are in {@code s}.</li>
     * <li>r[1] is an array of TreeSets where:
     * <ul>
     * <li>r[1][0] is a list of CASEW1 values in Wave 4 records.</li>
     * <li>r[1][1] is a list of CASEW2 values in Wave 4 records.</li>
     * <li>r[1][2] is a list of CASEW3 values in Wave 4 records.</li>
     * <li>r[1][3] is a list of all CASEW4 values.</li>
     * <li>r[1][4] is a list of CASEW4 values for records that have CASEW3,
     * CASEW2 and CASEW1 values.</li>
     * </ul>
     * </li>
     * <li>r[2] is a TreeMap with keys as CASEW4 and values as CASEW3.</li>
     * <li>r[3] is a TreeMap with keys as CASEW3 and values as HashSets of
     * CASEW4 (which is normally expected to contain just one CASEW4).</li>
     * </ul>
     */
    public Object[] loadWave4(byte wave, Set<Short> s) {
        String m = "loadWave4(byte, Set<Short>)";
        env.logStartTag(m);
        Object[] r;
        File cf = getGeneratedFile(wave, "InW3W5");
        if (cf.exists()) {
            r = (Object[]) load(wave, cf);
        } else {
            r = new Object[4];
            File f = getInputFile(wave);
            TreeMap<Short, WaAS_Wave4_HHOLD_Record> r0 = new TreeMap<>();
            r[0] = r0;
            TreeSet<Short>[] r1 = getSetsNew(wave + 1);
            r[1] = r1;
            /**
             * Each hhold in Wave 4 comes from at most one hhold from Wave 3. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 3 into a hhold in Wave 4. This
             * is expected to be rare. One example explanation for this
             * happening is a someone returning to a hhold having left it.
             */
            TreeMap<Short, Short> CASEW4ToCASEW3 = new TreeMap<>();
            r[2] = CASEW4ToCASEW3;
            /**
             * There may be instances where hholds from Wave 3 split into two or
             * more hholds in Wave 4.
             */
            TreeMap<Short, HashSet<Short>> CASEW3ToCASEW4 = new TreeMap<>();
            r[3] = CASEW3ToCASEW4;
            String m1 = getMessage(wave, f);
            env.logStartTag(m1);
            BufferedReader br;
            br = Generic_IO.getBufferedReader(f);
            int count;
            count = br.lines().skip(1).mapToInt(line -> {
                WaAS_Wave4_HHOLD_Record rec;
                rec = new WaAS_Wave4_HHOLD_Record(line);
                short CASEW3 = rec.getCASEW3();
                if (CASEW3 > Short.MIN_VALUE) {
                    if (!r1[2].add(CASEW3)) {
                        env.log("In Wave 4: hhold with CASEW3 "
                                + CASEW3 + " reportedly split into multiple "
                                + "hholds.");
                        return 1;
                    }
                }
                return 0;
            }).sum();
            env.log("There are " + count + " hholds from Wave 3 "
                    + "reportedly split into multiple hholds in Wave 4.");
            // Close and reopen br
            br = Generic_IO.closeAndGetBufferedReader(br, f);
            br.lines().skip(1).forEach(line -> {
                WaAS_Wave4_HHOLD_Record rec;
                rec = new WaAS_Wave4_HHOLD_Record(line);
                short CASEW4 = rec.getCASEW4();
                short CASEW3 = rec.getCASEW3();
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (s.contains(CASEW4)) {
                    if (CASEW3 > Short.MIN_VALUE) {
                        CASEW4ToCASEW3.put(CASEW4, CASEW3);
                        Generic_Collections.addToMap(CASEW3ToCASEW4, CASEW3, CASEW4);
                        r0.put(CASEW4, rec);
                    }
                }
                r1[3].add(CASEW4);
                if (CASEW2 > Short.MIN_VALUE) {
                    r1[1].add(CASEW2);
                }
                if (CASEW1 > Short.MIN_VALUE) {
                    r1[0].add(CASEW1);
                    if (CASEW2 > Short.MIN_VALUE && CASEW3 > Short.MIN_VALUE) {
                        r1[4].add(CASEW4);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(wave, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 4 records, that have Wave 5 records, and that are reportedly in
     * Wave 3.
     *
     * @param wave
     *
     * @return r An Object[] of length 4:
     * <ul>
     * <li>r[0] is a TreeMap with keys as CASEW4 and values as
     * WaAS_Wave4_HHOLD_Records. It contains all records from Wave 4 that have a
     * CASEW3 value.</li>
     * <li>r[1] is an array of TreeSets where:
     * <ul>
     * <li>r[1][0] is a list of CASEW1 values in Wave 4 records.</li>
     * <li>r[1][1] is a list of CASEW2 values in Wave 4 records.</li>
     * <li>r[1][2] is a list of CASEW3 values in Wave 4 records.</li>
     * <li>r[1][3] is a list of all CASEW4 values.</li>
     * <li>r[1][4] is a list of CASEW4 values for records that have CASEW3,
     * CASEW2 and CASEW1 values.</li>
     * </ul>
     * </li>
     * <li>r[2] is a TreeMap with keys as CASEW4 and values as CASEW3.</li>
     * <li>r[3] is a TreeMap with keys as CASEW3 and values as HashSets of
     * CASEW4 (which is normally expected to contain just one CASEW4).</li>
     * </ul>
     */
    public Object[] loadWave4(byte wave) {
        String m = "loadWave4(byte)";
        env.logStartTag(m);
        Object[] r;
        File cf = getGeneratedFile(wave, "InW3");
        if (cf.exists()) {
            r = (Object[]) load(wave, cf);
        } else {
            r = new Object[4];
            File f = getInputFile(wave);
            TreeMap<Short, WaAS_Wave4_HHOLD_Record> r0 = new TreeMap<>();
            r[0] = r0;
            TreeSet<Short>[] r1 = getSetsNew(wave + 1);
            r[1] = r1;
            /**
             * Each hhold in Wave 4 comes from at most one hhold from Wave 3. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 3 into a hhold in Wave 4. This
             * is expected to be rare. One example explanation for this
             * happening is a someone returning to a hhold having left it.
             */
            TreeMap<Short, Short> CASEW4ToCASEW3 = new TreeMap<>();
            r[2] = CASEW4ToCASEW3;
            /**
             * There may be instances where hholds from Wave 3 split into two or
             * more hholds in Wave 4.
             */
            TreeMap<Short, HashSet<Short>> CASEW3ToCASEW4 = new TreeMap<>();
            r[3] = CASEW3ToCASEW4;
            String m0 = getMessage(wave, f);
            env.logStartTag(m0);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            int count = br.lines().skip(1).mapToInt(line -> {
                WaAS_Wave4_HHOLD_Record rec;
                rec = new WaAS_Wave4_HHOLD_Record(line);
                short CASEW3 = rec.getCASEW3();
                if (CASEW3 > Short.MIN_VALUE) {
                    if (!r1[2].add(CASEW3)) {
                        env.log("In Wave 4: hhold with CASEW3 " + CASEW3
                                + " reportedly split into multiple hholds.");
                        return 1;
                    }
                }
                return 0;
            }).sum();
            env.log("There are " + count + " hholds from Wave 3 "
                    + "reportedly split into multiple hholds in Wave 4.");
            // Close and reopen br
            br = Generic_IO.closeAndGetBufferedReader(br, f);
            br.lines().skip(1).forEach(line -> {
                WaAS_Wave4_HHOLD_Record rec;
                rec = new WaAS_Wave4_HHOLD_Record(line);
                short CASEW4 = rec.getCASEW4();
                short CASEW3 = rec.getCASEW3();
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (CASEW3 > Short.MIN_VALUE) {
                    CASEW4ToCASEW3.put(CASEW4, CASEW3);
                    Generic_Collections.addToMap(CASEW3ToCASEW4, CASEW3, CASEW4);
                    r0.put(CASEW4, rec);
                }
                r1[3].add(CASEW4);
                if (CASEW2 > Short.MIN_VALUE) {
                    r1[1].add(CASEW2);
                }
                if (CASEW1 > Short.MIN_VALUE) {
                    r1[0].add(CASEW1);
                    if (CASEW2 > Short.MIN_VALUE && CASEW3 > Short.MIN_VALUE) {
                        r1[4].add(CASEW4);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m0);
            cache(wave, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 3 records, that have Wave 5 and Wave 4 records, and that are
     * reportedly in Wave 2.
     *
     * @param wave
     * @param s A set containing CASEW3 values from Wave 4 (for Wave 4 records
     * that have a Wave 5 record).
     *
     * @return r An Object[] of length 4:
     * <ul>
     * <li>r[0] is a TreeMap with keys as CASEW3 and values as
     * WaAS_Wave3_HHOLD_Records.</li>
     * <li>r[1] is an array of TreeSets where:
     * <ul>
     * <li>r[1][0] is a list of CASEW1 values in Wave 3 records.</li>
     * <li>r[1][1] is a list of CASEW2 values in Wave 3 records.</li>
     * <li>r[1][2] is a list of all CASEW3 values.</li>
     * <li>r[1][3] is a list of CASEW3 values for records that have CASEW2 and
     * CASEW1 values.</li>
     * </ul>
     * </li>
     * <li>r[2] is a TreeMap with keys as CASEW3 and values as CASEW2.</li>
     * <li>r[3] is a TreeMap with keys as CASEW2 and values as HashSets of
     * CASEW3 (which is normally expected to contain just one CASEW3).</li>
     * </ul>
     */
    public Object[] loadWave3(byte wave, Set<Short> s) {
        String m = "loadWave3(byte, Set<Short>)";
        env.logStartTag(m);
        Object[] r;
        File cf = getGeneratedFile(wave, "InW2W4W5");
        if (cf.exists()) {
            r = (Object[]) load(wave, cf);
        } else {
            r = new Object[4];
            File f = getInputFile(wave);
            TreeMap<Short, WaAS_Wave3_HHOLD_Record> r0 = new TreeMap<>();
            r[0] = r0;
            TreeSet<Short>[] r1 = getSetsNew(wave + 2);
            r[1] = r1;
            /**
             * Each hhold in Wave 3 comes from at most one hhold from Wave 2. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 2 into a hhold in Wave 3. This
             * is expected to be rare. One example explanation for this
             * happening is a someone returning to a hhold having left it.
             */
            TreeMap<Short, Short> CASEW3ToCASEW2 = new TreeMap<>();
            r[2] = CASEW3ToCASEW2;
            /**
             * There may be instances where hholds from Wave 2 split into two or
             * more hholds in Wave 3.
             */
            TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3 = new TreeMap<>();
            r[3] = CASEW2ToCASEW3;
            String m1 = getMessage(wave, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            int count = br.lines().skip(1).mapToInt(line -> {
                WaAS_Wave3_HHOLD_Record rec;
                rec = new WaAS_Wave3_HHOLD_Record(line);
                short CASEW2 = rec.getCASEW2();
                if (CASEW2 > Short.MIN_VALUE) {
                    if (!r1[1].add(CASEW2)) {
                        env.log("In Wave 3: hhold with CASEW2 " + CASEW2
                                + " reportedly split into multiple hholds.");
                        return 1;
                    }
                }
                return 0;
            }).sum();
            env.log("There are " + count + " hholds from Wave 2 "
                    + "reportedly split into multiple hholds in Wave 3.");
            // Close and reopen br
            br = Generic_IO.closeAndGetBufferedReader(br, f);
            br.lines().skip(1).forEach(line -> {
                WaAS_Wave3_HHOLD_Record rec = new WaAS_Wave3_HHOLD_Record(line);
                short CASEW3 = rec.getCASEW3();
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (s.contains(CASEW3)) {
                    if (CASEW2 > Short.MIN_VALUE) {
                        CASEW3ToCASEW2.put(CASEW3, CASEW2);
                        Generic_Collections.addToMap(CASEW2ToCASEW3, CASEW2, CASEW3);
                        r0.put(CASEW3, rec);

                    }
                }
                r1[2].add(CASEW3);
                if (CASEW1 > Short.MIN_VALUE) {
                    r1[0].add(CASEW1);
                    if (CASEW2 > Short.MIN_VALUE) {
                        r1[3].add(CASEW3);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(wave, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 2 records; that have Wave 5, Wave 4 and Wave 3 records; and
     * that are reportedly in Wave 1.
     *
     * @param wave
     * @param s A set containing CASEW2 values from Wave 3 (for Wave 3 records
     * that have a Wave 4 record that have a Wave 5 record).
     *
     * @return An Object[] r of length 4:
     * <ul>
     * <li>r[0] is a TreeMap with keys as CASEW2 and values as
     * WaAS_Wave2_HHOLD_Records.</li>
     * <li>r[1] is an array of TreeSets where:
     * <ul>
     * <li>r[1][0] is a list of CASEW1 values in Wave 2 records.</li>
     * <li>r[1][1] is a list of all CASEW2 values.</li>
     * <li>r[1][2] is a list of CASEW1 values in Wave 2 records that have Wave
     * 5, Wave 4 and Wave 3 records.</li>
     * </ul>
     * </li>
     * <li>r[2] is a TreeMap with keys as CASEW2 and values as CASEW1.</li>
     * <li>r[3] is a TreeMap with keys as CASEW1 and values as HashSets of
     * CASEW2 (which is normally expected to contain just one CASEW2).</li>
     * </ul>
     */
    public Object[] loadWave2(byte wave, Set<Short> s) {
        String m = "loadWave2(byte, Set<Short>)";
        env.logStartTag(m);
        Object[] r;
        File cf  = getGeneratedFile(wave, "InW1W3W4W5");
        if (cf.exists()) {
            r = (Object[]) load(wave, cf);
        } else {
            r = new Object[4];
            File f  = getInputFile(wave);
            TreeMap<Short, WaAS_Wave2_HHOLD_Record> r0  = new TreeMap<>();
            r[0] = r0;
            TreeSet<Short>[] r1  = getSetsNew(wave + 1);
            r[1] = r1;
            /**
             * Each hhold in Wave 2 comes from at most one hhold from Wave 1. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 1 into a hhold in Wave 2. This
             * is expected to be rare. One example explanation for this
             * happening is a someone returning to a hhold having left it.
             */
            TreeMap<Short, Short> CASEW2ToCASEW1 = new TreeMap<>();
            r[2] = CASEW2ToCASEW1;
            /**
             * There may be instances where hholds from Wave 1 split into two or
             * more hholds in Wave 2.
             */
            TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2 = new TreeMap<>();
            r[3] = CASEW1ToCASEW2;
            String m1 = getMessage(wave, f);
            env.logStartTag(m1);
            BufferedReader br  = Generic_IO.getBufferedReader(f);
            int  count = br.lines().skip(1).mapToInt(line -> {
                WaAS_Wave2_HHOLD_Record rec;
                rec = new WaAS_Wave2_HHOLD_Record(line);
                short CASEW1 = rec.getCASEW1();
                if (CASEW1 > Short.MIN_VALUE) {
                    if (!r1[0].add(CASEW1)) {
                        env.log("In Wave 2: hhold with CASEW1 " + CASEW1 + 
                                " reportedly split into multiple hholds.");
                        return 1;
                    }
                }
                return 0;
            }).sum();
            env.log("There are " + count + " hholds from Wave 1 "
                    + "reportedly split into multiple hholds in Wave 2.");
            // Close and reopen br
            br = Generic_IO.closeAndGetBufferedReader(br, f);
            br.lines().skip(1).forEach(line -> {
                WaAS_Wave2_HHOLD_Record rec = new WaAS_Wave2_HHOLD_Record(line);
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (s.contains(CASEW2)) {
                    if (CASEW1 > Short.MIN_VALUE) {
                        CASEW2ToCASEW1.put(CASEW2, CASEW1);
                        Generic_Collections.addToMap(CASEW1ToCASEW2, CASEW1, CASEW2);
                        r0.put(CASEW2, rec);
                        r1[2].add(CASEW1);
                    }
                }
                r1[1].add(CASEW2);
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(wave, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 1 records (that have Wave 5, Wave 4, Wave 3 and Wave 2
     * records).
     *
     * @param wave
     * @param s A set containing CASEW1 values from Wave 2 (for Wave 2 records
     * that have a Wave 3 records that have a Wave 4 record that have a Wave 5
     * record).
     *
     * @return An {@code Object[] r} of length 2:
     * <ul>
     * <li>{@code r[0]} is a {@code TreeMap<Short, WaAS_Wave1_HHOLD_Record>}
     * with keys as {@Link WaAS_Wave1_HHOLD_Record#CASEW1} and values as
     * WaAS_Wave1_HHOLD_Records.</li>
     * <li>r[1] is an array of TreeSets where:
     * <ul>
     * <li>r[1][0] is a list of all CASEW1 values.</li>
     * </ul>
     * </li>
     * <li>r[2] is a TreeMap with keys as CASEW2 and values as CASEW1.</li>
     * <li>r[3] is a TreeMap with keys as CASEW1 and values as HashSets of
     * CASEW2 (which is normally expected to contain just one CASEW2).</li>
     * </ul>
     */
    public Object[] loadWave1(byte wave, Set<Short> s) {
        String m = "loadWave1(byte, Set<Short>)";
        env.logStartTag(m);
        Object[] r;
        File cf  = getGeneratedFile(wave, "InW2W3W4W5");
        if (cf.exists()) {
            r = (Object[]) load(wave, cf);
        } else {
            r = new Object[2];
            File f = getInputFile(wave);
            TreeMap<Short, WaAS_Wave1_HHOLD_Record> r0  = new TreeMap<>();
            r[0] = r0;
            TreeSet<Short>[]  r1 = getSetsNew(wave);
            r[1] = r1;
            String  m1 = getMessage(wave, f);
            env.logStartTag( m1);
            BufferedReader br  = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(line -> {
                WaAS_Wave1_HHOLD_Record rec;
                rec = new WaAS_Wave1_HHOLD_Record(line);
                short CASEW1 = rec.getCASEW1();
                if (s.contains(CASEW1)) {
                    if (CASEW1 > Short.MIN_VALUE) {
                        r0.put(CASEW1, rec);
                    }
                }
                r1[0].add(CASEW1);
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
        env.logEndTag(m1);
            cache(wave, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<Short, WaAS_Wave1_HHOLD_Record> loadCacheSubsetWave1() {
        TreeMap<Short, WaAS_Wave1_HHOLD_Record> r;
        File cf = getFile(WaAS_Data.W1);
        if (cf.exists()) {
            r = (TreeMap<Short, WaAS_Wave1_HHOLD_Record>) Generic_IO.readObject(cf);
        } else {
            r = null;
        }
        return r;
    }

    /**
     *
     * @param wave
     * @return
     */
    protected File getFile(byte wave) {
        File dir = new File(Files.getGeneratedWaASDir(), WaAS_Strings.s_Subsets);
        return new File(dir, TYPE + wave + "." + WaAS_Strings.s_dat);
    }

    public TreeMap<Short, WaAS_Wave2_HHOLD_Record> loadCacheSubsetWave2() {
        TreeMap<Short, WaAS_Wave2_HHOLD_Record> r;
        File cf  = getFile(WaAS_Data.W2);
        if (cf.exists()) {
            r = (TreeMap<Short, WaAS_Wave2_HHOLD_Record>) Generic_IO.readObject(cf);
        } else {
            r = null;
        }
        return r;
    }

    public TreeMap<Short, WaAS_Wave3_HHOLD_Record> loadCacheSubsetWave3() {
        TreeMap<Short, WaAS_Wave3_HHOLD_Record> r;
        File cf = getFile(WaAS_Data.W3);
        if (cf.exists()) {
            r = (TreeMap<Short, WaAS_Wave3_HHOLD_Record>) Generic_IO.readObject(cf);
        } else {
            r = null;
        }
        return r;
    }

    public TreeMap<Short, WaAS_Wave4_HHOLD_Record> loadCacheSubsetWave4() {
        TreeMap<Short, WaAS_Wave4_HHOLD_Record> r;
        File cf = getFile(WaAS_Data.W4);
        if (cf.exists()) {
            r = (TreeMap<Short, WaAS_Wave4_HHOLD_Record>) Generic_IO.readObject(cf);
        } else {
            r = null;
        }
        return r;
    }

    public TreeMap<Short, WaAS_Wave5_HHOLD_Record> loadCacheSubsetWave5() {
        TreeMap<Short, WaAS_Wave5_HHOLD_Record> r;
        File cf = getFile(WaAS_Data.W5);
        if (cf.exists()) {
            r = (TreeMap<Short, WaAS_Wave5_HHOLD_Record>) Generic_IO.readObject(cf);
        } else {
            r = null;
        }
        return r;
    }

    /**
     * Value label information for Ten1W5
     * <ul>
     * <li>Value = 1.0	Label = Own it outright</li>
     * <li>Value = 2.0	Label = Buying it with the help of a mortgage or
     * loan</li>
     * <li>Value = 3.0	Label = Pay part rent and part mortgage (shared
     * ownership)</li>
     * <li>Value = 4.0	Label = Rent it</li>
     * <li>Value = 5.0	Label = Live here rent-free (including rent-free in
     * relatives friend</li>
     * <li>Value = 6.0	Label = Squatting</li>
     * <li>Value = -9.0	Label = Does not know</li>
     * <li>Value = -8.0	Label = No answer</li>
     * <li>Value = -7.0	Label = Does not apply</li>
     * <li>Value = -6.0	Label = Error/Partial</li>
     * </ul>
     *
     * @return
     */
    public TreeMap<Byte, String> getTenureNameMap() {
        TreeMap<Byte, String> r;
        r = new TreeMap<>();
        r.put((byte) 1, "Own it outright");
        r.put((byte) 2, "Buying it with the help of a mortgage or loan");
        r.put((byte) 3, "Pay part rent and part mortgage (shared ownership)");
        r.put((byte) 4, "Rent it");
        r.put((byte) 5, "Live here rent-free (including rent-free in relatives friend");
        r.put((byte) 6, "Squatting");
        r.put((byte) -9, "Does not know");
        r.put((byte) -8, "No answer");
        r.put((byte) -7, "Does not apply");
        r.put((byte) -6, "Error/Partial");
        return r;
    }

}
