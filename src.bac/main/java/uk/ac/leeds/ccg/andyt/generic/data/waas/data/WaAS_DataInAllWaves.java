/*
 * Copyright 2019 Centre for Computational Geography, University of Leeds.
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Object;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Strings;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W2Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W4Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W5Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W1Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W3Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W4ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W1ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W5ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W2ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W3ID;
import uk.ac.leeds.ccg.andyt.generic.util.Generic_Collections;

public class WaAS_DataInAllWaves extends WaAS_Object {

    public WaAS_W1Data w1Data;
    public TreeMap<WaAS_W1ID, WaAS_W1Record> w1recs;
    //public TreeSet<WaAS_ID2> w1IDs;
    public WaAS_W2Data w2Data;
    public TreeMap<WaAS_W2ID, WaAS_W2Record> w2recs;
    //public TreeSet<WaAS_ID2> w2IDs;
    public WaAS_W3Data w3Data;
    public TreeMap<WaAS_W3ID, WaAS_W3Record> w3recs;
    //public TreeSet<WaAS_ID2> w3IDs;
    public WaAS_W4Data w4Data;
    public TreeMap<WaAS_W4ID, WaAS_W4Record> w4recs;
    //public TreeSet<WaAS_ID2> w4IDs;
    public WaAS_W5Data w5Data;
    public TreeMap<WaAS_W5ID, WaAS_W5Record> w5recs;
    //public TreeSet<WaAS_ID2> w5IDs;

    /**
     * For storing a subset of the lookup.
     */
    public TreeMap<WaAS_W1ID, HashSet<WaAS_W2ID>> w1_To_w2;
    /**
     * For storing a subset of the lookup.
     */
    public TreeMap<WaAS_W2ID, WaAS_W1ID> w2_To_w1;
    /**
     * For storing a subset of the lookup.
     */
    public TreeMap<WaAS_W2ID, HashSet<WaAS_W3ID>> w2_To_w3;
    /**
     * For storing a subset of the lookup.
     */
    public TreeMap<WaAS_W3ID, WaAS_W2ID> w3_To_w2;
    /**
     * For storing a subset of the lookup.
     */
    public TreeMap<WaAS_W3ID, HashSet<WaAS_W4ID>> w3_To_w4;
    /**
     * For storing a subset of the lookup.
     */
    public TreeMap<WaAS_W4ID, WaAS_W3ID> w4_To_w3;
    /**
     * For storing a subset of the lookup.
     */
    public TreeMap<WaAS_W4ID, HashSet<WaAS_W5ID>> w4_To_w5;
    /**
     * For storing a subset of the lookup.
     */
    public TreeMap<WaAS_W5ID, WaAS_W4ID> w5_To_w4;

    public WaAS_DataInAllWaves(WaAS_Environment env) {
        super(env);
    }

    /**
     * Loads data that are in all waves.
     *
     * @param type
     * @throws java.io.FileNotFoundException
     */
    public void loadDataInAllWaves(String type) throws FileNotFoundException, IOException {
        String m = "loadDataInAllWaves()";
        env.logStartTag(m);
        /**
         * Step 1: Initial loading
         */
        /**
         * Step 1.1: Wave 5 initial load. After this load the lookup contains
         * Wave 5 records that have Wave 4 IDs.
         */
        w5Data = we.hh.loadW5InW4();
        /**
         * Step 1.2: Wave 4 initial load. After this load the main set of data
         * contains all those Wave 4 records that have Wave 3 record identifiers
         * and that are in the main set loaded in Step 1.1.
         */
        w4Data = we.hh.loadW4InSAndW3(w5Data.w5_To_w4.values(), WaAS_Strings.s__In_ + "w3w5");
        /**
         * Step 1.3: Wave 3 initial load. After this load the main set of data
         * contains all those Wave 3 records that have Wave 2 record identifiers
         * and that are in the main set loaded in Step 1.2.
         */
        w3Data = we.hh.loadW3InSAndW2(w4Data.w4_To_w3.values(), WaAS_Strings.s__In_ + "w2w4w5");
        /**
         * Step 1.4: Wave 2 initial load. After this load the main set of data
         * contains all those Wave 2 records that have Wave 1 record identifiers
         * and that are in the main set loaded in Step 1.3.
         */
        w2Data = we.hh.loadW2InSAndW1(w3Data.w3_To_w2.values(), WaAS_Strings.s__In_ + "w1w3w4w5");
        /**
         * Step 1.5: Wave 1 initial load. After this load the main set of data
         * contains all those Wave 1 records that are in the main set loaded in
         * Step 1.4.
         */
        w1Data = we.hh.loadW1(w2Data.w2_To_w1.values(), WaAS_Strings.s__In_ + "w2w3w4w5");
        /**
         * Step 2: Check what is loaded and go through creating ID sets.
         */
        /**
         * Step 2.1: Log status of the main sets loaded in Step 1.
         */
        env.log("There are " + w5Data.lookup.size() + " w5recs.");
        env.log("There are " + w4Data.lookup.size() + " w4recs.");
        env.log("There are " + w3Data.lookup.size() + " w3recs.");
        env.log("There are " + w2Data.lookup.size() + " w2recs.");
        env.log("There are " + w1Data.lookup.size() + " w1recs.");
        /**
         * Step 2.2: Filter sets.
         */
        /**
         * Step 2.2.1: Wave 1.
         */
        w1recs = new TreeMap<>();
        //w1IDs = new TreeSet<>();
        Iterator<WaAS_W1ID> iteW1 = w1Data.lookup.keySet().iterator();
        while (iteW1.hasNext()) {
            WaAS_W1ID w1ID = iteW1.next();
            w1recs.put(w1ID, w1Data.lookup.get(w1ID));
        }
        //env.log("w1IDs.size() " + w1IDs.size());
        we.hh.cacheSubset(we.W1, w1recs, type);
        w1Data = null; // Save some space
        //w1recs = null; // Save some space
        /**
         * Step 2.2.2: Wave 2.
         */
        w1_To_w2 = new TreeMap<>();
        w2_To_w1 = new TreeMap<>();
        w2recs = new TreeMap<>();
        //TreeSet<WaAS_W2ID> w2IDs = new TreeSet<>();
        Iterator<WaAS_W2ID> iteW2 = w2Data.lookup.keySet().iterator();
        while (iteW2.hasNext()) {
            WaAS_W2ID w2ID = iteW2.next();
            WaAS_W1ID w1ID = w2Data.w2_To_w1.get(w2ID);
            if (w1recs.keySet().contains(w1ID)) {
            w2recs.put(w2ID, w2Data.lookup.get(w2ID));
            Generic_Collections.addToMap(w1_To_w2, w1ID, w2ID);
            w2_To_w1.put(w2ID, w1ID);
            }
        }
        //env.log("w2IDs.size() " + w2IDs.size());
        we.hh.cacheSubset(we.W2, w2recs, type);
        env.log("w1_To_w2.size() " + w1_To_w2.size());
        env.log("w2_To_w1.size() " + w2_To_w1.size());
        we.hh.cacheSubsetLookups(we.W1, w1_To_w2, w2_To_w1);
        w2Data = null; // Save some space
        //w2recs = null; // Save some space
        /**
         * Step 2.2.3: Wave 3.
         */
        w2_To_w3 = new TreeMap<>();
        w3_To_w2 = new TreeMap<>();
        w3recs = new TreeMap<>();
        Iterator<WaAS_W3ID> iteW3 = w3Data.lookup.keySet().iterator();
        while (iteW3.hasNext()) {
            WaAS_W3ID w3ID = iteW3.next();
            WaAS_W2ID w2ID = w3Data.w3_To_w2.get(w3ID);
            //if (w2_To_w1.containsKey(w2ID)) {
            if (w2recs.keySet().contains(w2ID)) {
                w3recs.put(w3ID, w3Data.lookup.get(w3ID));
                w3_To_w2.put(w3ID, w2ID);
                Generic_Collections.addToMap(w2_To_w3, w2ID, w3ID);
                w3_To_w2.put(w3ID, w2ID);
            }
        }
        we.hh.cacheSubset(we.W3, w3recs, type);
        env.log("w2_To_w3.size() " + w2_To_w3.size());
        env.log("w3_To_w2.size() " + w3_To_w2.size());
        we.hh.cacheSubsetLookups(we.W2, w2_To_w3, w3_To_w2);
        w3Data = null; // Save some space
        //w3recs = null; // Save some space
        /**
         * Step 2.2.4: Wave 4.
         */
        w3_To_w4 = new TreeMap<>();
        w4_To_w3 = new TreeMap<>();
        w4recs = new TreeMap<>();
        Iterator<WaAS_W4ID> iteW4 = w4Data.lookup.keySet().iterator();
        while (iteW4.hasNext()) {
            WaAS_W4ID w4ID = iteW4.next();
            WaAS_W3ID w3ID = w4Data.w4_To_w3.get(w4ID);
            if (w3ID == null) {
                env.log("w4Data.w4_To_w3.get(w4ID) = null for w4ID " + w4ID);
            } else {
                //if (w3_To_w2.containsKey(w3ID)) {
                if (w3recs.keySet().contains(w3ID)) {
                    w4recs.put(w4ID, w4Data.lookup.get(w4ID));
                    w4_To_w3.put(w4ID, w3ID);
                    Generic_Collections.addToMap(w3_To_w4, w3ID, w4ID);
                }
            }
        }
        we.hh.cacheSubset(we.W4, w4recs, type);
        env.log("w3_To_w4.size() " + w3_To_w4.size());
        env.log("w4_To_w3.size() " + w4_To_w3.size());
        we.hh.cacheSubsetLookups(we.W3, w3_To_w4, w4_To_w3);
        w4Data = null; // Save some space
        //w4recs = null; // Save some space
        /**
         * Step 2.2.5: Wave 5.
         */
        w4_To_w5 = new TreeMap<>();
        w5_To_w4 = new TreeMap<>();
        w5recs = new TreeMap<>();
        Iterator<WaAS_W5ID> iteW5 = w5Data.lookup.keySet().iterator();
        while (iteW5.hasNext()) {
            WaAS_W5ID w5ID = iteW5.next();
            WaAS_W4ID w4ID = w5Data.w5_To_w4.get(w5ID);
            if (w4ID == null) {
                //env.log("w5Data.w5_To_w4.get(w5ID) = null for w5ID " + w5ID);
            } else {
                //if (w4_To_w3.containsKey(w4ID)) {
                if (w4recs.keySet().contains(w4ID)) {
                    w5recs.put(w5ID, w5Data.lookup.get(w5ID));
                    w5_To_w4.put(w5ID, w4ID);
                    Generic_Collections.addToMap(w4_To_w5, w4ID, w5ID);
                }
            }
        }
        we.hh.cacheSubset(we.W5, w5recs, type);
        env.log("w4_To_w5.size() " + w4_To_w5.size());
        env.log("w5_To_w4.size() " + w5_To_w4.size());
        we.hh.cacheSubsetLookups(we.W4, w4_To_w5, w5_To_w4);
        w5Data = null; // Save some space
        //w5recs = null; // Save some space
        env.logEndTag(m);
    }

}
