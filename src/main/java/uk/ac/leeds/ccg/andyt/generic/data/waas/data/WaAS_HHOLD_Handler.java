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
import java.util.ArrayList;
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
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W1HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W2HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W3HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W4HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W5HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.io.WaAS_Files;
import uk.ac.leeds.ccg.andyt.stats.Generic_Statistics;

/**
 *
 * @author geoagdt
 */
public class WaAS_HHOLD_Handler extends WaAS_Handler {

    public WaAS_HHOLD_Handler(WaAS_Environment e) {
        super(e, WaAS_Strings.s_hhold);
    }

    public class WaAS_HouseholdsInAllWaves {

        public WaAS_W1Data w1Data;
        public TreeMap<WaAS_W1ID, WaAS_W1HRecord> w1recs;
        public TreeSet<WaAS_ID2> w1IDs;
        public WaAS_W2Data w2Data;
        public TreeMap<WaAS_W2ID, WaAS_W2HRecord> w2recs;
        public TreeSet<WaAS_ID2> w2IDs;
        public WaAS_W3Data w3Data;
        public TreeMap<WaAS_W3ID, WaAS_W3HRecord> w3recs;
        public TreeSet<WaAS_ID2> w3IDs;
        public WaAS_W4Data w4Data;
        public TreeMap<WaAS_W4ID, WaAS_W4HRecord> w4recs;
        public TreeSet<WaAS_ID2> w4IDs;
        public WaAS_W5Data w5Data;
        public TreeMap<WaAS_W5ID, WaAS_W5HRecord> w5recs;
        public TreeSet<WaAS_ID2> w5IDs;
        
       public TreeMap<WaAS_W1ID, HashSet<WaAS_W2ID>> w1ToW2Subset;
           public TreeMap<WaAS_W2ID, WaAS_W1ID> w2ToW1Subset;
          public TreeMap<WaAS_W2ID, HashSet<WaAS_W3ID>> w2ToW3Subset;
          public TreeMap<WaAS_W3ID, WaAS_W2ID> w3ToW2Subset;
         public TreeMap<WaAS_W3ID, HashSet<WaAS_W4ID>> w3ToW4Subset;
         public TreeMap<WaAS_W4ID, WaAS_W3ID> w4ToW3Subset;
         public HashMap<WaAS_W4ID, WaAS_W3ID> W4ToW3;
        public TreeMap<WaAS_W4ID, HashSet<WaAS_W5ID>> w4ToW5Subset;
        public TreeMap<WaAS_W5ID, WaAS_W4ID> w5ToW4Subset;
                    HashMap<WaAS_W5ID, WaAS_W4ID> w5ToW4;

        
        public WaAS_HouseholdsInAllWaves(String type) {
            /**
             * Step 1: Initial loading
             */
            /**
             * Step 1.1: Wave 5 initial load. After this load the lookup
             * contains Wave 5 records that have Wave 4 IDs.
             */
            w5Data = loadW5();
            /**
             * Step 1.2: Wave 4 initial load. After this load the main set of
             * data contains all those Wave 4 records that have Wave 3 record
             * identifiers and that are in the main set loaded in Step 1.1.
             */
            w4Data = loadW4InS(w5Data.W4ToW5.keySet(), WaAS_Strings.s_InW3W5);
            /**
             * Step 1.3: Wave 3 initial load. After this load the main set of
             * data contains all those Wave 3 records that have Wave 2 record
             * identifiers and that are in the main set loaded in Step 1.2.
             */
            w3Data = loadW3InSAndW2(w4Data.W3ToW4.keySet(), WaAS_Strings.s_InW2W4W5);
            /**
             * Step 1.4: Wave 2 initial load. After this load the main set of
             * data contains all those Wave 2 records that have Wave 1 record
             * identifiers and that are in the main set loaded in Step 1.3.
             */
            w2Data = loadW2InSAndW1(w3Data.W2ToW3.keySet(), WaAS_Strings.s_InW1W3W4W5);
            /**
             * Step 1.5: Wave 1 initial load. After this load the main set of
             * data contains all those Wave 1 records that are in the main set
             * loaded in Step 1.4.
             */
            w1Data = loadW1(w2Data.W1ToW2.keySet(), WaAS_Strings.s_InW2W4W5);
            /**
             * Step 2: Check what is loaded and go through creating ID sets.
             */
            /**
             * Step 2.1: Log status of the main sets loaded in Step 1.
             */
            env.log("There are " + w1Data.lookup.size() + " w1recs.");
            env.log("There are " + w2Data.lookup.size() + " w2recs.");
            env.log("There are " + w3Data.lookup.size() + " w3recs.");
            env.log("There are " + w4Data.lookup.size() + " w4recs.");
            env.log("There are " + w5Data.lookup.size() + " w5recs.");
            /**
             * Step 2.2: Filter sets.
             */
            /**
             * Step 2.2.1: Wave 1.
             */
            w1recs = new TreeMap<>();
            w1IDs = new TreeSet<>();
            Iterator<WaAS_W1ID> iteW1 = w1Data.lookup.keySet().iterator();
            while (iteW1.hasNext()) {
                WaAS_W1ID w1ID = iteW1.next();
                WaAS_ID2 ID = new WaAS_ID2(w1ID, w1ID);
                w1IDs.add(ID);
                w1recs.put(w1ID, w1Data.lookup.get(w1ID));
            }
            env.log("w1IDs.size() " + w1IDs.size());
            cacheSubset(W1, w1recs, type);
            /**
             * Step 2.2.2: Wave 2.
             */
            w1ToW2Subset = new TreeMap<>();
            w2ToW1Subset = new TreeMap<>();
            w2recs = new TreeMap<>();
            w2IDs = new TreeSet<>();
            Iterator<WaAS_W2ID> iteW2 = w2Data.lookup.keySet().iterator();
            while (iteW2.hasNext()) {
                WaAS_W2ID w2ID = iteW2.next();
                WaAS_W1ID w1ID = w2Data.W2ToW1.get(w2ID);
                WaAS_ID2 ID = new WaAS_ID2(w1ID, w2ID);
                w2IDs.add(ID);
                w2recs.put(w2ID, w2Data.lookup.get(w2ID));
                Generic_Collections.addToMap(w1ToW2Subset, w1ID, w2ID);
                w2ToW1Subset.put(w2ID, w1ID);
            }
            env.log("w2IDs.size() " + w2IDs.size());
            cacheSubset(W2, w2recs, type);
            env.log("w1ToW2Subset.size() " + w1ToW2Subset.size());
            env.log("w2ToW1Subset.size() " + w2ToW1Subset.size());
            cacheSubsetLookups(W1, w1ToW2Subset, w2ToW1Subset);
            /**
             * Step 2.2.3: Wave 3.
             */
            w2ToW3Subset = new TreeMap<>();
            w3ToW2Subset = new TreeMap<>();
            w3recs = new TreeMap<>();
            //TreeSet<WaAS_ID2> w3IDs2 = new TreeSet<>();
            HashMap<WaAS_W3ID, WaAS_W2ID> w3ToW2 = new HashMap<>();
            Iterator<WaAS_W3ID> iteW3 = w3Data.lookup.keySet().iterator();
            while (iteW3.hasNext()) {
                WaAS_W3ID w3ID = iteW3.next();
                WaAS_W2ID w2ID = new WaAS_W2ID(w3Data.lookup.get(w3ID).getCASEW2());
                if (w2ToW1Subset.containsKey(w2ID)) {
                    w3ToW2.put(w3ID, w2ID);
                    WaAS_W1ID w1ID = w2ToW1Subset.get(w2ID);
                    HashSet<WaAS_W3ID> w3IDs2 = w3Data.W2ToW3.get(w2ID);
                    Iterator<WaAS_W3ID> ite2 = w3IDs2.iterator();
                    while (ite2.hasNext()) {
                        w3ID = ite2.next();
                        WaAS_ID2 ID = new WaAS_ID2(w1ID, w3ID);
                        //w3IDs2.add(ID);
                        w3IDs.add(ID);
                        w3recs.put(w3ID, w3Data.lookup.get(w3ID));
                        Generic_Collections.addToMap(w2ToW3Subset, w2ID, w3ID);
                        w3ToW2Subset.put(w3ID, w2ID);
                    }
                }
            }
            //env.log("w3IDs2.size() " + w3IDs2.size());
            env.log("w3IDs.size() " + w3IDs.size());
            cacheSubset(W3, w3recs, type);
            env.log("w2ToW3Subset.size() " + w2ToW3Subset.size());
            env.log("w3ToW2Subset.size() " + w3ToW2Subset.size());
            cacheSubsetLookups(W2, w2ToW3Subset, w3ToW2Subset);
            /**
             * Step 2.2.4: Wave 4.
             */
            w3ToW4Subset = new TreeMap<>();
            w4ToW3Subset = new TreeMap<>();
            w4recs = new TreeMap<>();
            W4ToW3 = new HashMap<>();
            w4IDs = new TreeSet<>();
            Iterator<WaAS_W4ID> iteW4 = w4Data.lookup.keySet().iterator();
            while (iteW4.hasNext()) {
                WaAS_W4ID w4ID = iteW4.next();
                WaAS_W3ID w3ID = new WaAS_W3ID(w4Data.lookup.get(w4ID).getCASEW3());
                if (w3ToW2Subset.containsKey(w3ID)) {
                    W4ToW3.put(w4ID, w3ID);
                    WaAS_W2ID w2ID = w3ToW2Subset.get(w3ID);
                    WaAS_W1ID w1ID = w2ToW1Subset.get(w2ID);
                    HashSet<WaAS_W4ID> w4IDs2 = w4Data.W3ToW4.get(w3ID);
                    Iterator<WaAS_W4ID> ite2 = w4IDs2.iterator();
                    while (ite2.hasNext()) {
                        w4ID = ite2.next();
                        WaAS_ID2 ID = new WaAS_ID2(w1ID, w4ID);
                        w4IDs.add(ID);
                        w4recs.put(w4ID, w4Data.lookup.get(w4ID));
                        Generic_Collections.addToMap(w3ToW4Subset, w3ID, w4ID);
                        w4ToW3Subset.put(w4ID, w3ID);
                    }
                }
            }
            env.log("w4IDs.size() " + w4IDs.size());
            cacheSubset(W4, w4recs, type);
            env.log("w3ToW4Subset.size() " + w3ToW4Subset.size());
            env.log("w4ToW3Subset.size() " + w4ToW3Subset.size());
            cacheSubsetLookups(W3, w3ToW4Subset, w4ToW3Subset);
            /**
             * Step 2.2.5: Wave 5.
             */
            w4ToW5Subset = new TreeMap<>();
            w5ToW4Subset = new TreeMap<>();
            w5recs = new TreeMap<>();
            w5ToW4 = new HashMap<>();
            //TreeSet<WaAS_ID2> w5IDs2 = new TreeSet<>();
            Iterator<WaAS_W5ID> iteW5 = w5Data.lookup.keySet().iterator();
            while (iteW5.hasNext()) {
                WaAS_W5ID w5ID = iteW5.next();
                WaAS_W4ID w4ID = new WaAS_W4ID(w5Data.lookup.get(w5ID).getCASEW4());
                if (w4ToW3Subset.containsKey(w4ID)) {
                    w5ToW4.put(w5ID, w4ID);
                    WaAS_W3ID w3ID = w4ToW3Subset.get(w4ID);
                    WaAS_W2ID w2ID = w3ToW2Subset.get(w3ID);
                    WaAS_W1ID w1ID = w2ToW1Subset.get(w2ID);
                    HashSet<WaAS_W5ID> w5IDs2 = w5Data.W4ToW5.get(w4ID);
                    Iterator<WaAS_W5ID> ite2 = w5IDs2.iterator();
                    while (ite2.hasNext()) {
                        w5ID = ite2.next();
                        WaAS_ID2 ID = new WaAS_ID2(w1ID, w5ID);
                        w5IDs.add(ID);
                        w5recs.put(w5ID, w5Data.lookup.get(w5ID));
                        Generic_Collections.addToMap(w4ToW5Subset, w4ID, w5ID);
                        w5ToW4Subset.put(w5ID, w4ID);
                    }
                }
            }
            env.log("w5IDs.size() " + w5IDs.size());
            //env.log("w5ID2s.size() " + w5ID2s.size());
            cacheSubset(W5, w5recs, type);
            env.log("w4ToW5Subset.size() " + w4ToW5Subset.size());
            env.log("w5ToW4Subset.size() " + w5ToW4Subset.size());
            cacheSubsetLookups(W4, w4ToW5Subset, w5ToW4Subset);
        }

    }

    /**
     * Loads hhold WaAS records that are in all waves.
     *
     * @param type Expecting {@link WaAS_Strings#s_InW1W2W3W4W5}.
     * @return an Object[] r with size 5. Each element is an Object[] containing
     * the data from loading each wave. The first element of these are TreeMaps
     * where the keys are the CASEWX for the wave and the values are the hhold
     * records for that specific wave. The second element of these are first
     * element of these are the ..
     */
    public WaAS_HouseholdsInAllWaves loadHouseholdsInAllWaves(String type) {
        String m = "loadHouseholdsInAllWaves(" + type + ")";
        env.logStartTag(m);
        WaAS_HouseholdsInAllWaves r = new WaAS_HouseholdsInAllWaves(type);
        env.logEndTag(m);
        return r;
    }

//    /**
//     * Loads all hhold in paired waves.
//     *
//     * @param wave Currently expected to be
//     * {@link #W2}, {@link #W3}, {@link #W4}, or {@link #W5}.
//     * @return an Object[] r of length 4. Each element is an Object[] containing
//     * the data from loading the wave.
//     * <ul>
//     * <li>If wave == {@link #W2} then:
//     * <ul>
//     * <li>r[0] is a TreeMap with keys as CASEW2 and values as
//     * WaAS_Wave2_HHOLD_Records. It contains all records from Wave 2 that have a
//     * CASEW2 value.</li>
//     * <li>r[1] is an array of TreeSets where:
//     * <ul>
//     * <li>r[1][0] is a list of CASEW1 values in Wave 2 records.</li>
//     * <li>r[1][1] is a list of all CASEW2 values.</li>
//     * <li>r[1][2] is a list of CASEW2 values for records that have CASEW1
//     * values.</li>
//     * </ul>
//     * </li>
//     * <li>r[2] is a TreeMap with keys as CASEW2 and values as CASEW2.</li>
//     * <li>r[3] is a TreeMap with keys as CASEW2 and values as HashSets of
//     * CASEW2 (which is normally expected to contain just one CASEW2).</li>
//     * </ul></li>
//     * <li>If wave == {@link #W3} then:
//     * <ul>
//     * <li>r[0] is a TreeMap with keys as w3ID and values as
//     * WaAS_Wave3_HHOLD_Records. It contains all records from Wave 3 that have a
//     * CASEW2 value.</li>
//     * <li>r[1] is an array of TreeSets where:
//     * <ul>
//     * <li>r[1][0] is a list of CASEW1 values in Wave 3 records.</li>
//     * <li>r[1][1] is a list of CASEW2 values in Wave 3 records.</li>
//     * <li>r[1][2] is a list of all w3ID values.</li>
//     * <li>r[1][3] is a list of w3ID values for records that have CASEW2 and
//     * CASEW1 values.</li>
//     * </ul>
//     * </li>
//     * <li>r[2] is a TreeMap with keys as w3ID and values as CASEW2.</li>
//     * <li>r[3] is a TreeMap with keys as CASEW2 and values as HashSets of
//     * w3ID (which is normally expected to contain just one w3ID).</li>
//     * </ul></li>
//     * <li>If wave == {@link #W4} then:
//     * <ul>
//     * <li>r[0] is a TreeMap with keys as CASEW4 and values as
//     * WaAS_Wave4_HHOLD_Records. It contains all records from Wave 4 that have a
//     * w3ID value.</li>
//     * <li>r[1] is an array of TreeSets where:
//     * <ul>
//     * <li>r[1][0] is a list of CASEW1 values in Wave 4 records.</li>
//     * <li>r[1][1] is a list of CASEW2 values in Wave 4 records.</li>
//     * <li>r[1][2] is a list of w3ID values in Wave 4 records.</li>
//     * <li>r[1][3] is a list of all CASEW4 values.</li>
//     * <li>r[1][4] is a list of CASEW4 values for records that have w3ID,
//     * CASEW2 and CASEW1 values.</li>
//     * </ul>
//     * </li>
//     * <li>r[2] is a TreeMap with keys as CASEW4 and values as w3ID.</li>
//     * <li>r[3] is a TreeMap with keys as w3ID and values as HashSets of
//     * CASEW4 (which is normally expected to contain just one CASEW4).</li>
//     * </ul></li>
//     * <li>If wave == {@link #W5} then:
//     * <ul>
//     * <li>r[0] is a TreeMap with keys as CASEW5 and values as
//     * WaAS_Wave5_HHOLD_Records.</li>
//     * <li>r[1] is an array of TreeSets where:
//     * <ul>
//     * <li>r[1][0] is a list of CASEW1 values in Wave 5 records.</li>
//     * <li>r[1][1] is a list of CASEW2 values in Wave 5 records.</li>
//     * <li>r[1][2] is a list of w3ID values in Wave 5 records.</li>
//     * <li>r[1][3] is a list of CASEW4 values in Wave 5 records.</li>
//     * <li>r[1][4] is a list of all CASEW5 values.</li>
//     * <li>r[1][5] is a list of CASEW5 values for records that have CASEW4,
//     * w3ID, CASEW2 and CASEW1 values.</li>
//     * </ul>
//     * </li>
//     * <li>r[2] is a TreeMap with keys as CASEW5 and values as CASEW4.</li>
//     * <li>r[3] is a TreeMap with keys as CASEW4 and values as HashSets of
//     * CASEW5 (which is normally expected to contain just one CASEW5).</li>
//     * </ul></li>
//     * </ul>
//     */
//    public Object[] loadHouseholdsInPreviousWave(byte wave) {
//        String m = "loadHouseholdsInPreviousWave(" + wave + ")";
//        env.logStartTag(m);
//        Object[] r = new Object[5];
//        if (wave == W2) {
//            r = loadW2InW1();
//        } else if (wave == W3) {
//            r = loadW3();
//        } else if (wave == W4) {
//            r = loadW4();
//        } else if (wave == W5) {
//            r = loadW5();
//        } else {
//            env.log("Erroneous Wave " + wave);
//        }
//        env.logEndTag(m);
//        return r;
//    }
    /**
     * Load Wave 5 records that are reportedly in Wave 4 (those with CASEW4
     * values).
     *
     * @return the loaded data
     */
    public WaAS_W5Data loadW5() {
        String m = "loadW5";
        env.logStartTag(m);
        WaAS_W5Data r;
        File cf = getSubsetCacheFile2(W5, "InW4");
        if (cf.exists()) {
            r = (WaAS_W5Data) load(W5, cf);
        } else {
            r = new WaAS_W5Data();
            File f = getInputFile(W5);
            /**
             * Each hhold in Wave 5 comes from at most one hhold from Wave 4. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 4 into a hhold in Wave 5. This
             * is expected to be rare. One example explanation for this
             * happening is someone returning to a hhold having left it.
             */
            String m1 = getMessage(W5, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            int count = br.lines().skip(1).mapToInt(l -> {
                WaAS_W5HRecord rec = new WaAS_W5HRecord(l);
                short CASEW4 = rec.getCASEW4();
                if (CASEW4 > Short.MIN_VALUE) {
                    if (!r.W4InW5.add(new WaAS_W4ID(CASEW4))) {
                        env.log("Between Wave 4 and 5: hhold with CASEW4 "
                                + CASEW4 + " reportedly split into multiple "
                                + "hholds.");
                        return 1;
                    }
                }
                return 0;
            }).sum();
            env.log("There are " + count + " hholds from Wave 4 that "
                    + "reportedly split into multiple hholds in Wave 5.");
            // Close and reopen br
            br = Generic_IO.closeAndGetBufferedReader(br, f);
            br.lines().skip(1).forEach(l -> {
                WaAS_W5HRecord rec = new WaAS_W5HRecord(l);
                WaAS_W5ID w5ID = new WaAS_W5ID(rec.getCASEW5());
                short CASEW4 = rec.getCASEW4();
                short CASEW3 = rec.getCASEW3();
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (CASEW4 > Short.MIN_VALUE) {
                    WaAS_W4ID w4ID = new WaAS_W4ID(CASEW4);
                    r.W5ToW4.put(w5ID, w4ID);
                    Generic_Collections.addToMap(r.W4ToW5, w4ID, w5ID);
                    r.lookup.put(w5ID, rec);
                }
                r.W5InW1W2W3W4.add(w5ID);
                if (CASEW3 > Short.MIN_VALUE) {
                    r.W3InW5.add(new WaAS_W3ID(CASEW3));
                }
                if (CASEW2 > Short.MIN_VALUE) {
                    r.w2IDsInW5.add(new WaAS_W2ID(CASEW2));
                }
                if (CASEW1 > Short.MIN_VALUE) {
                    r.w1IDsInW5.add(new WaAS_W1ID(CASEW3));
                    if (CASEW2 > Short.MIN_VALUE && CASEW3 > Short.MIN_VALUE
                            && CASEW4 > Short.MIN_VALUE) {
                        r.W5InW1W2W3W4.add(w5ID);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W5, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load All Wave 5 records.
     *
     * @return a TreeMap with keys as CASEW5 and values as
     * WaAS_Wave5_HHOLD_Records.
     */
    public TreeMap<WaAS_W5ID, WaAS_W5HRecord> loadAllW5() {
        String m = "loadAllW5";
        env.logStartTag(m);
        TreeMap<WaAS_W5ID, WaAS_W5HRecord> r;
        File cf = getGeneratedAllFile(W5);
        if (cf.exists()) {
            r = (TreeMap<WaAS_W5ID, WaAS_W5HRecord>) load(W5, cf);
        } else {
            r = new TreeMap<>();
            File f = getInputFile(W5);
            String m1 = getMessage(W5, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(l -> {
                WaAS_W5HRecord rec = new WaAS_W5HRecord(l);
                r.put(new WaAS_W5ID(rec.getCASEW5()), rec);
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W5, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load All Wave 4 records.
     *
     * @return a TreeMap with keys as CASEW4 and values as
     * WaAS_Wave4_HHOLD_Records.
     */
    public TreeMap<WaAS_W4ID, WaAS_W4HRecord> loadAllW4() {
        String m = "loadAllW4";
        env.logStartTag(m);
        TreeMap<WaAS_W4ID, WaAS_W4HRecord> r;
        File cf = getGeneratedAllFile(W4);
        if (cf.exists()) {
            r = (TreeMap<WaAS_W4ID, WaAS_W4HRecord>) load(W4, cf);
        } else {
            r = new TreeMap<>();
            File f = getInputFile(W4);
            String m1 = getMessage(W4, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(l -> {
                WaAS_W4HRecord rec = new WaAS_W4HRecord(l);
                r.put(new WaAS_W4ID(rec.getCASEW4()), rec);
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W4, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load All Wave 3 records.
     *
     * @return a TreeMap with keys as w3ID and values as
     * WaAS_Wave3_HHOLD_Records.
     */
    public TreeMap<WaAS_W3ID, WaAS_W3HRecord> loadAllW3() {
        String m = "loadAllW3";
        env.logStartTag(m);
        TreeMap<WaAS_W3ID, WaAS_W3HRecord> r;
        File cf = getGeneratedAllFile(W3);
        if (cf.exists()) {
            r = (TreeMap<WaAS_W3ID, WaAS_W3HRecord>) load(W3, cf);
        } else {
            r = new TreeMap<>();
            File f = getInputFile(W3);
            String m1 = getMessage(W3, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(l -> {
                WaAS_W3HRecord rec = new WaAS_W3HRecord(l);
                r.put(new WaAS_W3ID(rec.getCASEW3()), rec);
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W3, cf, r);
            env.logEndTag(m);
        }
        return r;
    }

    /**
     * Load All Wave 2 records.
     *
     * @return a TreeMap with keys as CASEW2 and values as
     * WaAS_Wave2_HHOLD_Records.
     */
    public TreeMap<WaAS_W2ID, WaAS_W2HRecord> loadAllW2() {
        String m = "loadAllW2";
        env.logStartTag(m);
        TreeMap<WaAS_W2ID, WaAS_W2HRecord> r;
        File cf = getGeneratedAllFile(W2);
        if (cf.exists()) {
            r = (TreeMap<WaAS_W2ID, WaAS_W2HRecord>) load(W2, cf);
        } else {
            r = new TreeMap<>();
            File f = getInputFile(W2);
            String m1 = getMessage(W2, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(l -> {
                WaAS_W2HRecord rec = new WaAS_W2HRecord(l);
                r.put(new WaAS_W2ID(rec.getCASEW2()), rec);
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W2, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load All Wave 1 records.
     *
     * @return a TreeMap with keys as CASEW1 and values as
     * WaAS_Wave1_HHOLD_Records.
     */
    public TreeMap<WaAS_W1ID, WaAS_W1HRecord> loadAllW1() {
        String m = "loadAllW1";
        env.logStartTag(m);
        TreeMap<WaAS_W1ID, WaAS_W1HRecord> r;
        File cf = getGeneratedAllFile(W1);
        if (cf.exists()) {
            r = (TreeMap<WaAS_W1ID, WaAS_W1HRecord>) load(W1, cf);
        } else {
            r = new TreeMap<>();
            File f = getInputFile(W1);
            String m1 = getMessage(W1, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(l -> {
                WaAS_W1HRecord rec = new WaAS_W1HRecord(l);
                r.put(new WaAS_W1ID(rec.getCASEW1()), rec);
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W1, cf, r);
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
     * @return
     */
    protected TreeSet<WaAS_W1ID> getSetW1() {
        TreeSet<WaAS_W1ID> r = new TreeSet<>();
        return r;
    }

    /**
     *
     * @return
     */
    protected TreeSet<WaAS_W2ID> getSetW2() {
        TreeSet<WaAS_W2ID> r = new TreeSet<>();
        return r;
    }

    /**
     *
     * @return
     */
    protected TreeSet<WaAS_W3ID> getSetW3() {
        TreeSet<WaAS_W3ID> r = new TreeSet<>();
        return r;
    }

    /**
     *
     * @return
     */
    protected TreeSet<WaAS_W4ID> getSetW4() {
        TreeSet<WaAS_W4ID> r = new TreeSet<>();
        return r;
    }

    /**
     *
     * @return
     */
    protected TreeSet<WaAS_W5ID> getSetW5() {
        TreeSet<WaAS_W5ID> r = new TreeSet<>();
        return r;
    }

    /**
     * For getting a specific generated File with type defaulted to
     * {@link WaAS_Strings#s_All}.
     * {@link #getGeneratedFile(short, java.lang.String)}
     *
     * @param wave the wave part of the filename.
     * @return a specific generated File.
     */
    protected File getGeneratedAllFile(byte wave) {
        return new File(files.getGeneratedWaASDir(), TYPE + WaAS_Strings.s_W
                + wave + WaAS_Strings.s_All + WaAS_Files.DOT_DAT);
    }

    /**
     * Load Wave 4 records that have CASEW4 values in {@code s}.
     *
     * @param s a set containing w3ID.
     * @param type for loading an already computed result. Expected values
     * include: "InW3W5" and "InW5".
     *
     * @return the loaded data
     */
    public WaAS_W4Data loadW4InS(Set<WaAS_W4ID> s, String type) {
        String m = "loadW4InS(Set<WaAS_W4ID>, " + type + ")";
        env.logStartTag(m);
        WaAS_W4Data r;
        File cf = getSubsetCacheFile2(W4, type);
        if (cf.exists()) {
            r = (WaAS_W4Data) load(W4, cf);
        } else {
            r = new WaAS_W4Data();
            File f = getInputFile(W4);
            /**
             * Each hhold in Wave 4 comes from at most one hhold from Wave 3. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 3 into a hhold in Wave 4. This
             * is expected to be rare. One example explanation for this
             * happening is someone returning to a hhold having left it.
             */
            String m1 = getMessage(W4, f);
            env.logStartTag(m1);
            BufferedReader br = loadW4Count(r, f);
            br.lines().skip(1).forEach(l -> {
                WaAS_W4HRecord rec = new WaAS_W4HRecord(l);
                WaAS_W4ID w4ID = new WaAS_W4ID(rec.getCASEW4());
                short CASEW3 = rec.getCASEW3();
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (s.contains(w4ID)) {
                    r.lookup.put(w4ID, rec);
                    if (CASEW3 > Short.MIN_VALUE) {
                        WaAS_W3ID w3ID = new WaAS_W3ID(CASEW3);
                        r.W4ToW3.put(w4ID, w3ID);
                        Generic_Collections.addToMap(r.W3ToW4, w3ID, w4ID);
                    }
                }
                r.AllW4.add(w4ID);
                if (CASEW2 > Short.MIN_VALUE) {
                    r.W2InW4.add(new WaAS_W2ID(CASEW2));
                }
                if (CASEW1 > Short.MIN_VALUE) {
                    r.W1InW4.add(new WaAS_W1ID(CASEW1));
                    if (CASEW2 > Short.MIN_VALUE
                            && CASEW3 > Short.MIN_VALUE) {
                        r.W4InW1W2W3.add(w4ID);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W4, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 4 records that have w3ID values in {@code s}.
     *
     * @param s a set containing w3ID.
     * @param type for loading an already computed result. Expected values
     * include: "InW3W5" and "InW5".
     *
     * @return the loaded data
     */
    public WaAS_W4Data loadW4InSAndW3(Set<WaAS_W3ID> s, String type) {
        String m = "loadW4(Set<WaAS_W3ID>, " + type + ")";
        env.logStartTag(m);
        WaAS_W4Data r;
        File cf = getSubsetCacheFile2(W4, type);
        if (cf.exists()) {
            r = (WaAS_W4Data) load(W4, cf);
        } else {
            r = new WaAS_W4Data();
            File f = getInputFile(W4);
            /**
             * Each hhold in Wave 4 comes from at most one hhold from Wave 3. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 3 into a hhold in Wave 4. This
             * is expected to be rare. One example explanation for this
             * happening is someone returning to a hhold having left it.
             */
            String m1 = getMessage(W4, f);
            env.logStartTag(m1);
            BufferedReader br = loadW4Count(r, f);
            br.lines().skip(1).forEach(l -> {
                WaAS_W4HRecord rec = new WaAS_W4HRecord(l);
                WaAS_W4ID w4ID = new WaAS_W4ID(rec.getCASEW4());
                short CASEW3 = rec.getCASEW3();
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (CASEW3 > Short.MIN_VALUE) {
                    WaAS_W3ID w3ID = new WaAS_W3ID(CASEW3);
                    if (s.contains(w3ID)) {
                        r.W4ToW3.put(w4ID, w3ID);
                        Generic_Collections.addToMap(r.W3ToW4, w3ID, w4ID);
                        r.lookup.put(w4ID, rec);
                    }
                }
                r.AllW4.add(w4ID);
                if (CASEW2 > Short.MIN_VALUE) {
                    r.W2InW4.add(new WaAS_W2ID(CASEW2));
                }
                if (CASEW1 > Short.MIN_VALUE) {
                    r.W1InW4.add(new WaAS_W1ID(CASEW1));
                    if (CASEW2 > Short.MIN_VALUE
                            && CASEW3 > Short.MIN_VALUE) {
                        r.W4InW1W2W3.add(w4ID);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W4, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    protected BufferedReader loadW4Count(WaAS_W4Data r, File f) {
        BufferedReader br = Generic_IO.getBufferedReader(f);
        int count = br.lines().skip(1).mapToInt(l -> {
            WaAS_W4HRecord rec = new WaAS_W4HRecord(l);
            short CASEW3 = rec.getCASEW3();
            if (CASEW3 > Short.MIN_VALUE) {
                if (!r.W3InW4.add(new WaAS_W3ID(CASEW3))) {
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
        return br;
    }

    /**
     * Load Wave 4 records that are reportedly in Wave 3 (those with w3ID
     * values).
     *
     * @return the loaded data
     */
    public WaAS_W4Data loadW4() {
        String m = "loadW4";
        env.logStartTag(m);
        WaAS_W4Data r;
        File cf = getSubsetCacheFile2(W4, "InW3");
        if (cf.exists()) {
            r = (WaAS_W4Data) load(W4, cf);
        } else {
            r = new WaAS_W4Data();
            File f = getInputFile(W4);
            /**
             * Each hhold in Wave 4 comes from at most one hhold from Wave 3. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 3 into a hhold in Wave 4. This
             * is expected to be rare. One example explanation for this
             * happening is someone returning to a hhold having left it.
             */
            String m0 = getMessage(W4, f);
            env.logStartTag(m0);
            BufferedReader br = loadW4Count(r, f);
            br.lines().skip(1).forEach(l -> {
                WaAS_W4HRecord rec = new WaAS_W4HRecord(l);
                WaAS_W4ID w4ID = new WaAS_W4ID(rec.getCASEW4());
                short CASEW3 = rec.getCASEW3();
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (CASEW3 > Short.MIN_VALUE) {
                    WaAS_W3ID w3ID = new WaAS_W3ID(CASEW3);
                    r.W4ToW3.put(w4ID, w3ID);
                    Generic_Collections.addToMap(r.W3ToW4, w3ID, w4ID);
                    r.lookup.put(w4ID, rec);
                }
                r.AllW4.add(w4ID);
                if (CASEW2 > Short.MIN_VALUE) {
                    r.W2InW4.add(new WaAS_W2ID(CASEW2));
                }
                if (CASEW1 > Short.MIN_VALUE) {
                    r.W1InW4.add(new WaAS_W1ID(CASEW1));
                    if (CASEW2 > Short.MIN_VALUE && CASEW3 > Short.MIN_VALUE) {
                        r.W4InW1W2W3.add(w4ID);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m0);
            cache(W4, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    protected BufferedReader loadW3Count(WaAS_W3Data r, File f) {
        BufferedReader br = Generic_IO.getBufferedReader(f);
        int count = br.lines().skip(1).mapToInt(l -> {
            WaAS_W3HRecord rec = new WaAS_W3HRecord(l);
            short CASEW2 = rec.getCASEW2();
            if (CASEW2 > Short.MIN_VALUE) {
                if (!r.W2InW3.add(new WaAS_W2ID(CASEW2))) {
                    env.log("In Wave 3: hhold with CASEW2 " + CASEW2
                            + " reportedly split into multiple hholds.");
                    return 1;
                }
            }
            return 0;
        }).sum();
        env.log("There are " + count + " hholds from Wave 2 that "
                + "reportedly split into multiple hholds in Wave 3.");
        // Close and reopen br
        br = Generic_IO.closeAndGetBufferedReader(br, f);
        return br;
    }

    /**
     * Load Wave 3 records that are reportedly in Wave 2 (those with CASEW2
     * values).
     *
     * @return the loaded data
     */
    public WaAS_W3Data loadW3() {
        String m = "loadW3";
        env.logStartTag(m);
        WaAS_W3Data r;
        File cf = getSubsetCacheFile2(W3, "InW2");
        if (cf.exists()) {
            r = (WaAS_W3Data) load(W3, cf);
        } else {
            r = new WaAS_W3Data();
            File f = getInputFile(W3);
            /**
             * Each hhold in Wave 3 comes from at most one hhold from Wave 2. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 2 into a hhold in Wave 3. This
             * is expected to be rare. One example explanation for this
             * happening is someone returning to a hhold having left it.
             */
            String m0 = getMessage(W3, f);
            env.logStartTag(m0);
            BufferedReader br = loadW3Count(r, f);
            br.lines().skip(1).forEach(l -> {
                WaAS_W3HRecord rec = new WaAS_W3HRecord(l);
                WaAS_W3ID w3ID = new WaAS_W3ID(rec.getCASEW3());
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (CASEW2 > Short.MIN_VALUE) {
                    WaAS_W2ID w2ID = new WaAS_W2ID(CASEW2);
                    r.W3ToW2.put(w3ID, w2ID);
                    Generic_Collections.addToMap(r.W2ToW3, w2ID, w3ID);
                    r.lookup.put(w3ID, rec);
                }
                r.AllW3.add(w3ID);
                if (CASEW1 > Short.MIN_VALUE) {
                    r.W1InW3.add(new WaAS_W1ID(CASEW1));
                    if (CASEW2 > Short.MIN_VALUE) {
                        r.W3InW1W2.add(w3ID);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m0);
            cache(W3, cf, r);
        }
        env.logEndTag(m);
        return r;

    }

    /**
     * Load Wave 3 records that have w3ID values in {@code s}.
     *
     * @param s a set containing w3ID values.
     * @param type for loading an already computed result. Expected values
     * include: "InW4".
     *
     * @return the loaded data
     */
    public WaAS_W3Data loadW3InS(Set<WaAS_W3ID> s, String type) {
        String m = "loadW3InS(Set<WaAS_W3ID>, " + type + ")";
        env.logStartTag(m);
        WaAS_W3Data r;
        File cf = getSubsetCacheFile2(W3, type);
        if (cf.exists()) {
            r = (WaAS_W3Data) load(W3, cf);
        } else {
            r = new WaAS_W3Data();
            File f = getInputFile(W3);
            String m1 = getMessage(W3, f);
            env.logStartTag(m1);
            BufferedReader br = loadW3Count(r, f);
            br.lines().skip(1).forEach(l -> {
                WaAS_W3HRecord rec = new WaAS_W3HRecord(l);
                WaAS_W3ID w3ID = new WaAS_W3ID(rec.getCASEW3());
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (s.contains(w3ID)) {
                    r.lookup.put(w3ID, rec);
                    if (CASEW2 > Short.MIN_VALUE) {
                        WaAS_W2ID w2ID = new WaAS_W2ID(CASEW2);
                        r.W3ToW2.put(w3ID, w2ID);
                        Generic_Collections.addToMap(r.W2ToW3, w2ID, w3ID);
                    }
                }
                r.AllW3.add(w3ID);
                if (CASEW1 > Short.MIN_VALUE) {
                    r.W1InW3.add(new WaAS_W1ID(CASEW1));
                    if (CASEW2 > Short.MIN_VALUE) {
                        r.W3InW1W2.add(w3ID);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W3, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 3 records that have w3ID values in {@code s}.
     *
     * @param s a set containing w3ID values.
     * @param type for loading an already computed result. Expected values
     * include: "InW2W4W5".
     *
     * @return the loaded data
     */
    public WaAS_W3Data loadW3InSAndW2(Set<WaAS_W3ID> s, String type) {
        String m = "loadW3InSAndW2(Set<WaAS_W3ID>, " + type + ")";
        env.logStartTag(m);
        WaAS_W3Data r;
        File cf = getSubsetCacheFile2(W3, type);
        if (cf.exists()) {
            r = (WaAS_W3Data) load(W3, cf);
        } else {
            r = new WaAS_W3Data();
            File f = getInputFile(W3);
            String m1 = getMessage(W3, f);
            env.logStartTag(m1);
            BufferedReader br = loadW3Count(r, f);
            br.lines().skip(1).forEach(l -> {
                WaAS_W3HRecord rec = new WaAS_W3HRecord(l);
                WaAS_W3ID w3ID = new WaAS_W3ID(rec.getCASEW3());
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (s.contains(w3ID)) {
                    if (CASEW2 > Short.MIN_VALUE) {
                        WaAS_W2ID w2ID = new WaAS_W2ID(CASEW2);
                        r.W3ToW2.put(w3ID, w2ID);
                        Generic_Collections.addToMap(r.W2ToW3, w2ID, w3ID);
                        r.lookup.put(w3ID, rec);
                    }
                }
                r.AllW3.add(w3ID);
                if (CASEW1 > Short.MIN_VALUE) {
                    r.W1InW3.add(new WaAS_W1ID(CASEW1));
                    if (CASEW2 > Short.MIN_VALUE) {
                        r.W3InW1W2.add(w3ID);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W3, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

//    /**
//     * Load Wave 3 records that have w3ID values in {@code s}.
//     *
//     * @param s a set containing w3ID values.
//     * @param type for loading an already computed result. Expected values
//     * include: "InW2W4W5" and "InW4".
//     *
//     * @return r An Object[] of length 4:
//     * <ul>
//     * <li>r[0] is a TreeMap with keys as w3ID and values as
//     * WaAS_Wave3_HHOLD_Records. For {@code type.equalsIgnoreCase("InW2W4W5")}
//     * this only contains records for households also in Wave 2, Wave 4 and Wave
//     * 5. For {@code type.equalsIgnoreCase("InW4")} this only contains records
//     * for households also in Wave 4.</li>
//     * <li>r[1] is an array of TreeSets where:
//     * <ul>
//     * <li>r[1][0] is a list of CASEW1 values in Wave 3 records.</li>
//     * <li>r[1][1] is a list of CASEW2 values in Wave 3 records.</li>
//     * <li>r[1][2] is a list of all w3ID values.</li>
//     * <li>r[1][3] is a list of w3ID values for records that have CASEW2 and
//     * CASEW1 values.</li>
//     * </ul>
//     * </li>
//     * <li>r[2] is a TreeMap with keys as w3ID and values as CASEW2.</li>
//     * <li>r[3] is a TreeMap with keys as CASEW2 and values as HashSets of
//     * w3ID (which is normally expected to contain just one w3ID).</li>
//     * </ul>
//     */
//    public Object[] loadW3(Set<Short> s, String type) {
//        String m = "loadW3(Set<Short>, " + type + ")";
//        env.logStartTag(m);
//        Object[] r;
//        File cf = getSubsetCacheFile2(W3, type);
//        if (cf.exists()) {
//            r = (Object[]) load(W3, cf);
//        } else {
//            r = new Object[4];
//            File f = getInputFile(W3);
//            TreeMap<Short, WaAS_W3HRecord> r0 = new TreeMap<>();
//            r[0] = r0;
//            TreeSet<Short>[] r1 = getSets(W3 + 1);
//            r[1] = r1;
//            /**
//             * Each hhold in Wave 3 comes from at most one hhold from Wave 2. It
//             * may be that in the person files there are individuals that have
//             * come from different hholds in Wave 2 into a hhold in Wave 3. This
//             * is expected to be rare. One example explanation for this
//             * happening is someone returning to a hhold having left it.
//             */
//            TreeMap<Short, Short> CASEW3ToCASEW2 = new TreeMap<>();
//            r[2] = CASEW3ToCASEW2;
//            /**
//             * There may be instances where hholds from Wave 2 split into two or
//             * more hholds in Wave 3.
//             */
//            TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3 = new TreeMap<>();
//            r[3] = CASEW2ToCASEW3;
//            String m1 = getMessage(W3, f);
//            env.logStartTag(m1);
//            BufferedReader br = Generic_IO.getBufferedReader(f);
//            int count = br.lines().skip(1).mapToInt(l -> {
//                WaAS_W3HRecord rec = new WaAS_W3HRecord(l);
//                short CASEW2 = rec.getCASEW2();
//                if (CASEW2 > Short.MIN_VALUE) {
//                    if (!r1[1].add(CASEW2)) {
//                        env.log("Between Wave 2 and 3: hhold with CASEW2 "
//                                + CASEW2 + " reportedly split into multiple "
//                                + "hholds.");
//                        return 1;
//                    }
//                }
//                return 0;
//            }).sum();
//            env.log("There are " + count + " hholds from Wave 2 "
//                    + "reportedly split into multiple hholds in Wave 3.");
//            // Close and reopen br
//            br = Generic_IO.closeAndGetBufferedReader(br, f);
//            if (type.equalsIgnoreCase("InW2W4W5")) {
//                br.lines().skip(1).forEach(l -> {
//                    WaAS_W3HRecord rec = new WaAS_W3HRecord(l);
//                    short w3ID = rec.getCASEW3();
//                    short CASEW2 = rec.getCASEW2();
//                    short CASEW1 = rec.getCASEW1();
//                    if (s.contains(w3ID)) {
//                        if (CASEW2 > Short.MIN_VALUE) {
//                            CASEW3ToCASEW2.put(w3ID, CASEW2);
//                            Generic_Collections.addToMap(CASEW2ToCASEW3, CASEW2,
//                                    w3ID);
//                            r0.put(w3ID, rec);
//                        }
//                    }
//                    r1[2].add(w3ID);
//                    if (CASEW1 > Short.MIN_VALUE) {
//                        r1[0].add(CASEW1);
//                        if (CASEW2 > Short.MIN_VALUE) {
//                            r1[3].add(w3ID);
//                        }
//                    }
//                });
//            } else if (type.equalsIgnoreCase("InW4")) {
//                br.lines().skip(1).forEach(l -> {
//                    WaAS_W3HRecord rec = new WaAS_W3HRecord(l);
//                    short w3ID = rec.getCASEW3();
//                    short CASEW2 = rec.getCASEW2();
//                    short CASEW1 = rec.getCASEW1();
//                    if (s.contains(w3ID)) {
//                        r0.put(w3ID, rec);
//                        if (CASEW2 > Short.MIN_VALUE) {
//                            CASEW3ToCASEW2.put(w3ID, CASEW2);
//                            Generic_Collections.addToMap(CASEW2ToCASEW3, CASEW2,
//                                    w3ID);
//                        }
//                    }
//                    r1[2].add(w3ID);
//                    if (CASEW1 > Short.MIN_VALUE) {
//                        r1[0].add(CASEW1);
//                        if (CASEW2 > Short.MIN_VALUE) {
//                            r1[3].add(w3ID);
//                        }
//                    }
//                });
//            } else {
//                env.log("Unrecognised type " + type);
//            }
//            // Close br
//            Generic_IO.closeBufferedReader(br);
//            env.logEndTag(m1);
//            cache(W3, cf, r);
//        }
//        env.logEndTag(m);
//        return r;
//    }
    /**
     * Load Wave 2 records that are reportedly in Wave 1 (those with CASEW1
     * values).
     *
     * @return the loaded data
     */
    public WaAS_W2Data loadW2InW1() {
        String m = "loadW2InW1";
        env.logStartTag(m);
        WaAS_W2Data r;
        File cf = getSubsetCacheFile2(W2, "InW1");
        if (cf.exists()) {
            r = (WaAS_W2Data) load(W2, cf);
        } else {
            r = new WaAS_W2Data();
            File f = getInputFile(W2);
            String m0 = getMessage(W2, f);
            env.logStartTag(m0);
            BufferedReader br = loadW2Count(r, f);
            br.lines().skip(1).forEach(l -> {
                WaAS_W2HRecord rec = new WaAS_W2HRecord(l);
                WaAS_W2ID w2ID = new WaAS_W2ID(rec.getCASEW2());
                short CASEW1 = rec.getCASEW1();
                if (CASEW1 > Short.MIN_VALUE) {
                    WaAS_W1ID w1ID = new WaAS_W1ID(CASEW1);
                    r.W2ToW1.put(w2ID, w1ID);
                    Generic_Collections.addToMap(r.W1ToW2, w1ID, w2ID);
                    r.lookup.put(w2ID, rec);
                    r.W2InW1.add(w2ID);
                }
                r.AllW2.add(w2ID);
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m0);
            cache(W3, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 2 records that CASEW2 values in {@code s}.
     *
     * @param s a set containing CASEW2 values.
     * @param type for loading an already computed result. Expected value:
     * {@link WaAS_Strings#s_InW1W2W3W4W5}
     *
     * @return the loaded data
     */
    public WaAS_W2Data loadW2InSAndW1(Set<WaAS_W2ID> s, String type) {
        String m = "loadW2InSAndW1(Set<WaAS_W2ID>, " + type + ")";
        env.logStartTag(m);
        WaAS_W2Data r;
        File cf = getSubsetCacheFile2(W2, type);
        if (cf.exists()) {
            r = (WaAS_W2Data) load(W2, cf);
        } else {
            r = new WaAS_W2Data();
            File f = getInputFile(W2);
            String m0 = getMessage(W2, f);
            env.logStartTag(m0);
            BufferedReader br = loadW2Count(r, f);
            br.lines().skip(1).forEach(l -> {
                WaAS_W2HRecord rec = new WaAS_W2HRecord(l);
                WaAS_W2ID w2ID = new WaAS_W2ID(rec.getCASEW2());
                short CASEW1 = rec.getCASEW1();
                if (s.contains(w2ID)) {
                    if (CASEW1 > Short.MIN_VALUE) {
                        WaAS_W1ID w1ID = new WaAS_W1ID(CASEW1);
                        r.W2ToW1.put(w2ID, w1ID);
                        Generic_Collections.addToMap(r.W1ToW2, w1ID, w2ID);
                        r.lookup.put(w2ID, rec);
                        r.W1InW2.add(w1ID);
                    }
                }
                r.AllW2.add(w2ID);
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m0);
            cache(W2, cf, r);
        }
        return r;
    }

    /**
     * Load Wave 2 records that CASEW2 values in {@code s}.
     *
     * @param s a set containing CASEW2 values.
     * @param type for loading an already computed result. Expected value:
     * {@link WaAS_Strings#s_InW3}
     *
     * @return the loaded data
     */
    public WaAS_W2Data loadW2InS(Set<WaAS_W2ID> s, String type) {
        String m = "loadW2InS(Set<WaAS_W2ID>, " + type + ")";
        env.logStartTag(m);
        WaAS_W2Data r;
        File cf = getSubsetCacheFile2(W2, type);
        if (cf.exists()) {
            r = (WaAS_W2Data) load(W2, cf);
        } else {
            r = new WaAS_W2Data();
            File f = getInputFile(W2);
            String m0 = getMessage(W2, f);
            env.logStartTag(m0);
            BufferedReader br = loadW2Count(r, f);
            br.lines().skip(1).forEach(l -> {
                WaAS_W2HRecord rec = new WaAS_W2HRecord(l);
                WaAS_W2ID w2ID = new WaAS_W2ID(rec.getCASEW2());
                short CASEW1 = rec.getCASEW1();
                if (s.contains(w2ID)) {
                    r.lookup.put(w2ID, rec);
                    if (CASEW1 > Short.MIN_VALUE) {
                        WaAS_W1ID w1ID = new WaAS_W1ID(CASEW1);
                        r.W2ToW1.put(w2ID, w1ID);
                        Generic_Collections.addToMap(r.W1ToW2, w1ID, w2ID);
                        r.W1InW2.add(w1ID);
                    }
                }
                r.AllW2.add(w2ID);
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m0);
            cache(W2, cf, r);
        }
        return r;
    }

    protected BufferedReader loadW2Count(WaAS_W2Data r, File f) {
        BufferedReader br = Generic_IO.getBufferedReader(f);
        int count = br.lines().skip(1).mapToInt(l -> {
            WaAS_W2HRecord rec = new WaAS_W2HRecord(l);
            short CASEW1 = rec.getCASEW1();
            if (CASEW1 > Short.MIN_VALUE) {
                if (!r.W1InW2.add(new WaAS_W1ID(CASEW1))) {
                    env.log("Between Wave 1 and 2: hhold with CASEW1 " + CASEW1
                            + " reportedly split into multiple hholds.");
                    return 1;
                }
            }
            return 0;
        }).sum();
        env.log("There are " + count + " hholds from Wave 1 that reportedly "
                + "split into multiple hholds in Wave 2.");
        // Close and reopen br
        br = Generic_IO.closeAndGetBufferedReader(br, f);
        return br;
    }

    /**
     * Load Wave 1 records that have CASEW1 values in {@code s}.
     *
     * @param s a set containing CASEW1 values.
     * @param type for loading an already computed result. Expected values
     * include: {@link WaAS_Strings#s_InW1W2W3W4W5} and
     * {@link WaAS_Strings#s_InW2}.
     *
     * @return the loaded data
     */
    public WaAS_W1Data loadW1(Set<WaAS_W1ID> s, String type) {
        String m = "loadW1(Set<Short>, " + type + ")";
        env.logStartTag(m);
        WaAS_W1Data r;
        File cf = getSubsetCacheFile2(W1, type);
        if (cf.exists()) {
            r = (WaAS_W1Data) load(W1, cf);
        } else {
            r = new WaAS_W1Data();
            File f = getInputFile(W1);
            String m1 = getMessage(W1, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(l -> {
                WaAS_W1HRecord rec = new WaAS_W1HRecord(l);
                short CASEW1 = rec.getCASEW1();
                if (CASEW1 > Short.MIN_VALUE) {
                    WaAS_W1ID w1ID = new WaAS_W1ID(CASEW1);
                    if (s.contains(w1ID)) {
                        r.lookup.put(w1ID, rec);
                    }
                    r.AllW1.add(w1ID);
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W1, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * TreeMap<Short, WaAS_W1HRecord>
     *
     * @param type
     * @return Object[]
     */
    public TreeMap<WaAS_W1ID, WaAS_W1HRecord> loadCachedSubsetW1(
            String type) {
        String m = "loadCachedSubsetW1(" + type + ")";
        env.logStartTag(m);
        TreeMap<WaAS_W1ID, WaAS_W1HRecord> r;
        File f = getSubsetCacheFile(W1, type);
        if (f.exists()) {
            r = (TreeMap<WaAS_W1ID, WaAS_W1HRecord>) Generic_IO.readObject(f);
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * TreeMap<Short, WaAS_W1HRecord>
     *
     * @param type
     * @return Object[]
     */
    public TreeMap<WaAS_W1ID, WaAS_W1HRecord> loadCachedSubset2W1( String type) {
        String m = "loadCachedSubset2W1(" + type + ")";
        env.logStartTag(m);
        TreeMap<WaAS_W1ID, WaAS_W1HRecord> r;
        File f = getSubsetCacheFile2(W1, type);
        if (f.exists()) {
            WaAS_W1Data o = (WaAS_W1Data) Generic_IO.readObject(f);
            r = (TreeMap<WaAS_W1ID, WaAS_W1HRecord>) o.lookup;
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<WaAS_W2ID, WaAS_W2HRecord> loadCachedSubsetW2( String type) {
        String m = "loadCachedSubsetW2(" + type + ")";
        env.logStartTag(m);
        TreeMap<WaAS_W2ID, WaAS_W2HRecord> r;
        File f = getSubsetCacheFile(W2, type);
        if (f.exists()) {
            r = (TreeMap<WaAS_W2ID, WaAS_W2HRecord>) Generic_IO.readObject(f);
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<WaAS_W2ID, WaAS_W2HRecord> loadCachedSubset2W2( String type) {
        String m = "loadCachedSubset2W2(" + type + ")";
        env.logStartTag(m);
        TreeMap<WaAS_W2ID, WaAS_W2HRecord> r;
        File f = getSubsetCacheFile2(W2, type);
        if (f.exists()) {
            WaAS_W2Data o = (WaAS_W2Data) Generic_IO.readObject(f);
            r = (TreeMap<WaAS_W2ID, WaAS_W2HRecord>) o.lookup;
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<WaAS_W3ID, WaAS_W3HRecord> loadCachedSubsetW3( String type) {
        String m = "loadCachedSubsetW3(" + type + ")";
        env.logStartTag(m);
        TreeMap<WaAS_W3ID, WaAS_W3HRecord> r;
        File f = getSubsetCacheFile(W3, type);
        if (f.exists()) {
            r = (TreeMap<WaAS_W3ID, WaAS_W3HRecord>) Generic_IO.readObject(f);
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<WaAS_W3ID, WaAS_W3HRecord> loadCachedSubset2W3( String type) {
        String m = "loadCachedSubset2W3(" + type + ")";
        env.logStartTag(m);
        TreeMap<WaAS_W3ID, WaAS_W3HRecord> r;
        File f = getSubsetCacheFile2(W3, type);
        if (f.exists()) {
            WaAS_W3Data o = (WaAS_W3Data) Generic_IO.readObject(f);
            r = (TreeMap<WaAS_W3ID, WaAS_W3HRecord>) o.lookup;
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<WaAS_W4ID, WaAS_W4HRecord> loadCachedSubsetW4( String type) {
        String m = "loadCachedSubsetW4(" + type + ")";
        env.logStartTag(m);
        TreeMap<WaAS_W4ID, WaAS_W4HRecord> r;
        File f = getSubsetCacheFile(W4, type);
        if (f.exists()) {
            r = (TreeMap<WaAS_W4ID, WaAS_W4HRecord>) Generic_IO.readObject(f);
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<WaAS_W4ID, WaAS_W4HRecord> loadCachedSubset2W4(  String type) {
        String m = "loadCachedSubset2W4(" + type + ")";
        env.logStartTag(m);
        TreeMap<WaAS_W4ID, WaAS_W4HRecord> r;
        File f = getSubsetCacheFile2(W4, type);
        if (f.exists()) {
            WaAS_W4Data o = (WaAS_W4Data) Generic_IO.readObject(f);
            r = (TreeMap<WaAS_W4ID, WaAS_W4HRecord>) o.lookup;
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<WaAS_W5ID, WaAS_W5HRecord> loadCachedSubsetW5( String type) {
        String m = "loadCachedSubsetW5(" + type + ")";
        env.logStartTag(m);
        TreeMap<WaAS_W5ID, WaAS_W5HRecord> r;
        File f = getSubsetCacheFile(W5, type);
        if (f.exists()) {
            r = (TreeMap<WaAS_W5ID, WaAS_W5HRecord>) Generic_IO.readObject(f);
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<WaAS_W5ID, WaAS_W5HRecord> loadCachedSubset2W5( String type) {
        String m = "loadCachedSubset2W5(" + type + ")";
        env.logStartTag(m);
        TreeMap<WaAS_W5ID, WaAS_W5HRecord> r;
        File f = getSubsetCacheFile2(W5, type);
        if (f.exists()) {
            WaAS_W5Data o = (WaAS_W5Data) Generic_IO.readObject(f);
            r = (TreeMap<WaAS_W5ID, WaAS_W5HRecord>) o.lookup;
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
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

    /**
     *
     * @param variableName
     * @param wave
     * @param GORSubsetsAndLookups
     * @param data
     * @param subset Subset of CASEW1 for all records to be included.
     * @return Map with keys as GOR and Values as map with keys as CASEWX and
     * values as HVALUE.
     */
    public HashMap<Byte, HashMap<WaAS_ID, Double>> getVariableForGORSubsets(
            String variableName, byte wave,
            WaAS_GORSubsetsAndLookups GORSubsetsAndLookups, WaAS_Data data,
            HashSet<WaAS_W1ID> subset) {
        HashMap<Byte, HashMap<WaAS_ID, Double>> r = new HashMap<>();
        Iterator<Byte> ite = GORSubsetsAndLookups.GOR2W1IDSet.keySet().iterator();
        while (ite.hasNext()) {
            r.put(ite.next(), new HashMap<>());
        }
        if (wave == WaAS_Data.W1) {

            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(w1ID -> {
                    if (subset.contains(w1ID)) {
                        WaAS_CombinedRecord cr = c.getData().get(w1ID);
                        WaAS_W1HRecord w1 = cr.w1Rec.getHhold();
                        Byte GOR = GORSubsetsAndLookups.W1ID2GOR.get(w1ID);
                        Generic_Collections.addToMap(r, GOR, w1ID, w1.getHVALUE());
                    }
                });
                data.clearCollection(cID);
            });
        } else if (wave == WaAS_Data.W2) {
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(w1ID -> {
                    if (subset.contains(w1ID)) {
                        WaAS_CombinedRecord cr = c.getData().get(w1ID);
                        HashMap<WaAS_W2ID, WaAS_W2Record> recs = cr.w2Recs;
                        Iterator<WaAS_W2ID> ite2 = recs.keySet().iterator();
                        while (ite2.hasNext()) {
                            WaAS_W2ID w2ID = ite2.next();
                            Byte GOR = GORSubsetsAndLookups.W2ID2GOR.get(w2ID);
                            WaAS_W2HRecord w2 = recs.get(w2ID).getHhold();
                            Generic_Collections.addToMap(r, GOR, w2ID, w2.getHVALUE());
                        }
                    }
                });
                data.clearCollection(cID);
            });
        } else if (wave == WaAS_Data.W3) {
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_CombinedRecord cr = c.getData().get(CASEW1);
                        HashMap<WaAS_W2ID, HashMap<WaAS_W3ID, WaAS_W3Record>> recs = cr.w3Recs;
                        Iterator<WaAS_W2ID> ite1 = recs.keySet().iterator();
                        while (ite1.hasNext()) {
                            WaAS_W2ID w2ID = ite1.next();
                            HashMap<WaAS_W3ID, WaAS_W3Record> w3_2 = recs.get(w2ID);
                            Iterator<WaAS_W3ID> ite2 = w3_2.keySet().iterator();
                            while (ite2.hasNext()) {
                                WaAS_W3ID w3ID = ite2.next();
                                Byte GOR = GORSubsetsAndLookups.W3ID2GOR.get(w3ID);
                                WaAS_W3HRecord w3 = w3_2.get(w3ID).getHhold();
                                Generic_Collections.addToMap(r, GOR, w3ID, w3.getHVALUE());
                            }
                        }
                    }
                });
                data.clearCollection(cID);
            });
        } else if (wave == WaAS_Data.W4) {
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(w1ID -> {
                    if (subset.contains(w1ID)) {
                        WaAS_CombinedRecord cr = c.getData().get(w1ID);
                        HashMap<WaAS_W2ID, HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, WaAS_W4Record>>> recs = cr.w4Recs;
                        Iterator<WaAS_W2ID> ite1 = recs.keySet().iterator();
                        while (ite1.hasNext()) {
                            WaAS_W2ID w2ID = ite1.next();
                            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, WaAS_W4Record>> w4_2 = recs.get(w2ID);
                            Iterator<WaAS_W3ID> ite2 = w4_2.keySet().iterator();
                            while (ite2.hasNext()) {
                                WaAS_W3ID w3ID = ite2.next();
                                HashMap<WaAS_W4ID, WaAS_W4Record> w4_3 = w4_2.get(w3ID);
                                Iterator<WaAS_W4ID> ite3 = w4_3.keySet().iterator();
                                while (ite3.hasNext()) {
                                    WaAS_W4ID w4ID = ite3.next();
                                    Byte GOR = GORSubsetsAndLookups.W4ID2GOR.get(w4ID);
                                    WaAS_W4HRecord w4 = w4_3.get(w4ID).getHhold();
                                    Generic_Collections.addToMap(r, GOR, w4ID, w4.getHVALUE());
                                }
                            }
                        }
                    }
                });
                data.clearCollection(cID);
            });
        } else if (wave == WaAS_Data.W5) {
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_CombinedRecord cr = c.getData().get(CASEW1);
                        HashMap<WaAS_W2ID, HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>>>> recs = cr.w5Recs;
                        Iterator<WaAS_W2ID> ite1 = recs.keySet().iterator();
                        while (ite1.hasNext()) {
                            WaAS_W2ID w2ID = ite1.next();
                            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>>> w5_2 = recs.get(w2ID);
                            Iterator<WaAS_W3ID> ite2 = w5_2.keySet().iterator();
                            while (ite2.hasNext()) {
                                WaAS_W3ID w3ID = ite2.next();
                                HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>> w5_3 = w5_2.get(w3ID);
                                Iterator<WaAS_W4ID> ite3 = w5_3.keySet().iterator();
                                while (ite3.hasNext()) {
                                    WaAS_W4ID w4ID = ite3.next();
                                    HashMap<WaAS_W5ID, WaAS_W5Record> w5_4 = w5_3.get(w4ID);
                                    Iterator<WaAS_W5ID> ite4 = w5_4.keySet().iterator();
                                    while (ite4.hasNext()) {
                                        WaAS_W5ID w5ID = ite4.next();
                                        Byte GOR = GORSubsetsAndLookups.W5ID2GOR.get(w5ID);
                                        WaAS_W5HRecord w5 = w5_4.get(w5ID).getHhold();
                                        Generic_Collections.addToMap(r, GOR, w5ID, w5.getHVALUE());
                                    }
                                }
                            }
                        }
                    }
                });
                data.clearCollection(cID);
            });
        }
        return r;
    }

    /**
     * Get variable for GOR for Wave 1.
     *
     * @param vName Variable name.
     * @param gors
     * @param m Expecting ? to be WaAS_W1HRecord or WaAS_W2HRecord or
     * WaAS_W3HRecord or WaAS_W4HRecord or WaAS_W5HRecord.
     * @param wave
     * @return Map with keys as GOR and Values as map with keys as CASEWX and
     * values as HVALUE.
     */
    public HashMap<Byte, HashMap<WaAS_W1ID, Double>> getVariableForGORW1(
            String vName, ArrayList<Byte> gors,
            TreeMap<WaAS_W1ID, WaAS_W1HRecord> m, byte wave) {
        String m0 = "getVariableForGORW1(...)";
        env.logStartTag(m0);
        HashMap<Byte, HashMap<WaAS_W1ID, Double>> r = new HashMap<>();
        gors.stream().forEach(gor -> {
            r.put(gor, new HashMap<>());
        });
        int countNegative = 0;
        int countZero = 0;
        Iterator<WaAS_W1ID> ite = m.keySet().iterator();
        if (vName.equalsIgnoreCase(WaAS_Strings.s_HVALUE)) {
            while (ite.hasNext()) {
                WaAS_W1ID w1ID = ite.next();
                WaAS_W1HRecord recH = (WaAS_W1HRecord) m.get(w1ID);
                Byte GOR = recH.getGOR();
                double v = recH.getHVALUE();
                if (v == 0.0d) {
                    countZero++;
                } else if (v < 0.0d) {
                    countNegative++;
                }
                Generic_Collections.addToMap(r, GOR, w1ID, v);
            }
        } else if (vName.equalsIgnoreCase(WaAS_Strings.s_HPROPW)) {
            while (ite.hasNext()) {
                WaAS_W1ID w1ID = ite.next();
                WaAS_W1HRecord recH = (WaAS_W1HRecord) m.get(w1ID);
                Byte GOR = recH.getGOR();
                double v = recH.getHPROPW();
                if (v == 0.0d) {
                    countZero++;
                } else if (v < 0.0d) {
                    countNegative++;
                }
                Generic_Collections.addToMap(r, GOR, w1ID, v);
            }
        } else if (vName.equalsIgnoreCase(WaAS_Strings.s_TOTWLTH)) {
            while (ite.hasNext()) {
                WaAS_W1ID w1ID = ite.next();
                WaAS_W1HRecord recH = (WaAS_W1HRecord) m.get(w1ID);
                Byte GOR = recH.getGOR();
                double v = recH.getTOTWLTH();
                if (v == 0.0d) {
                    countZero++;
                } else if (v < 0.0d) {
                    countNegative++;
                }
                Generic_Collections.addToMap(r, GOR, w1ID, v);
            }
        } else {
            env.log("Unrecognised variable " + vName);
        }
        env.log(vName + " for GOR W" + wave);
        env.log("count " + m.size());
        env.log("countZero " + countZero);
        env.log("countNegative " + countNegative);
        env.logEndTag(m0);
        return r;
    }

    /**
     * Get variable for GOR for Wave 1.
     *
     * @param vName Variable name.
     * @param gors
     * @param m Expecting ? to be WaAS_W1HRecord or WaAS_W2HRecord or
     * WaAS_W3HRecord or WaAS_W5HRecord or WaAS_W5HRecord.
     * @param wave
     * @return Map with keys as GOR and Values as map with keys as CASEWX and
     * values as HVALUE.
     */
    public HashMap<Byte, HashMap<WaAS_W5ID, Double>> getVariableForGORW5(
            String vName, ArrayList<Byte> gors, TreeMap<WaAS_W5ID, WaAS_W5HRecord> m, byte wave) {
        String m0 = "getVariableForGORW5(...)";
        env.logStartTag(m0);
        HashMap<Byte, HashMap<WaAS_W5ID, Double>> r = new HashMap<>();
        gors.stream().forEach(gor -> {
            r.put(gor, new HashMap<>());
        });
        int countNegative = 0;
        int countZero = 0;
        Iterator<WaAS_W5ID> ite = m.keySet().iterator();
        if (vName.equalsIgnoreCase(WaAS_Strings.s_HVALUE)) {
            while (ite.hasNext()) {
                WaAS_W5ID w5ID = ite.next();
                WaAS_W5HRecord recH = (WaAS_W5HRecord) m.get(w5ID);
                Byte GOR = recH.getGOR();
                double v = recH.getHVALUE();
                if (v == 0.0d) {
                    countZero++;
                } else if (v < 0.0d) {
                    countNegative++;
                }
                Generic_Collections.addToMap(r, GOR, w5ID, v);
            }
        } else if (vName.equalsIgnoreCase(WaAS_Strings.s_HPROPW)) {
            while (ite.hasNext()) {
                WaAS_W5ID w5ID = ite.next();
                WaAS_W5HRecord recH = (WaAS_W5HRecord) m.get(w5ID);
                Byte GOR = recH.getGOR();
                double v = recH.getHPROPW();
                if (v == 0.0d) {
                    countZero++;
                } else if (v < 0.0d) {
                    countNegative++;
                }
                Generic_Collections.addToMap(r, GOR, w5ID, v);
            }
        } else if (vName.equalsIgnoreCase(WaAS_Strings.s_TOTWLTH)) {
            while (ite.hasNext()) {
                WaAS_W5ID w5ID = ite.next();
                WaAS_W5HRecord recH = (WaAS_W5HRecord) m.get(w5ID);
                Byte GOR = recH.getGOR();
                double v = recH.getTOTWLTH();
                if (v == 0.0d) {
                    countZero++;
                } else if (v < 0.0d) {
                    countNegative++;
                }
                Generic_Collections.addToMap(r, GOR, w5ID, v);
            }

        } else {
            env.log("Unrecognised variable " + vName);
        }
        env.log(vName + " for GOR W" + wave);
        env.log("count " + m.size());
        env.log("countZero " + countZero);
        env.log("countNegative " + countNegative);
        env.logEndTag(m0);
        return r;
    }

    /**
     * Get HVALUE Total Household Property Wealth for each wave in the subsets.
     *
     * @param variableName
     * @param gors
     * @param GORSubsetsAndLookups
     * @param GORNameLookup
     * @param data
     * @param subset
     * @return
     */
    public TreeMap<Byte, Double> getChangeVariableSubset(String variableName,
            ArrayList<Byte> gors, WaAS_GORSubsetsAndLookups GORSubsetsAndLookups,
            TreeMap<Byte, String> GORNameLookup, WaAS_Data data,
            HashSet<WaAS_W1ID> subset) {
        TreeMap<Byte, Double> r = new TreeMap<>();
        HashMap<Byte, HashMap<WaAS_ID, Double>>[] variableSubsets;
        variableSubsets = new HashMap[WaAS_Data.NWAVES];
        for (byte w = 0; w < WaAS_Data.NWAVES; w++) {
            variableSubsets[w] = getVariableForGORSubsets(variableName,
                    (byte) (w + 1), GORSubsetsAndLookups, data, subset);
        }
        double countW1 = 0;
        double countZeroW1 = 0;
        double countNegativeW1 = 0;
        double countW5 = 0;
        double countZeroW5 = 0;
        double countNegativeW5 = 0;
        env.log(variableName + " for each wave in the subsets.");
        String h = "GORNumber,GORName," + variableName + "5_Average-"
                + variableName + "1_Average";
        for (byte w = 1; w < WaAS_Data.NWAVES + 1; w++) {
            h += "," + variableName + "W" + w + "_Count," + variableName + "W"
                    + w + "_ZeroCount," + variableName + "W" + w
                    + "_NegativeCount," + variableName + "W" + w + "_Average";
        }
        env.log(h);
        Iterator<Byte> ite;
        ite = gors.iterator();
        while (ite.hasNext()) {
            byte gor = ite.next();
            double[][] var = new double[WaAS_Data.NWAVES][];
            for (byte w = 0; w < WaAS_Data.NWAVES; w++) {
                var[w] = Generic_Statistics.getSummaryStatistics(
                        variableSubsets[w].get(gor).values());
            }
            countW1 += var[0][5];
            countZeroW1 += var[0][6];
            countNegativeW1 += var[0][7];
            countW5 += var[4][5];
            countZeroW5 += var[4][6];
            countNegativeW5 += var[4][7];
            double diff = var[4][4] - var[0][4];
            String s = "" + gor + "," + GORNameLookup.get(gor) + "," + diff;
            for (byte w = 0; w < WaAS_Data.NWAVES; w++) {
                s += "," + var[w][4] + "," + var[w][5] + ","
                        + var[w][6] + "," + var[w][7];
            }
            env.log(s);
            r.put(gor, diff);
        }
        env.log(variableName + " For Wave 1 Subset");
        env.log("" + countW1 + "\t Count");
        env.log("" + countZeroW1 + "\t Zero");
        env.log("" + countNegativeW1 + "\t Negative");
        env.log(variableName + " For Wave 5 Subset");
        env.log("" + countW5 + "\t Count");
        env.log("" + countZeroW5 + "\t Zero");
        env.log("" + countNegativeW5 + "\t Negative");
        return r;
    }

    /**
     * Get the variableName for each wave for all records.
     *
     * @param vName Variable name
     * @param gors
     * @param GORNameLookup
     * @return
     */
    public <K> TreeMap<Byte, Double> getChangeVariableAll(String vName,
            ArrayList<Byte> gors, TreeMap<Byte, String> GORNameLookup) {
        TreeMap<Byte, Double> r = new TreeMap<>();
        TreeMap<WaAS_W1ID, WaAS_W1HRecord> allW1 = loadAllW1();
        HashMap<Byte, HashMap<WaAS_W1ID, Double>> vAllW1 = getVariableForGORW1(vName, gors, allW1, (byte) 1);
        allW1 = null; // Set to null to free memory.
        TreeMap<WaAS_W5ID, WaAS_W5HRecord> allW5 = loadAllW5();
        HashMap<Byte, HashMap<WaAS_W5ID, Double>> vAllW5 = getVariableForGORW5(vName, gors, allW5, (byte) 5);
        allW5 = null; // Set to null to free memory.
        env.log(vName + " Total Household Property Wealth for each wave "
                + "for all records.");
        String h = "GORNumber,GORName," + vName + "_Average-"
                + vName + "1_Average";
        for (byte w = 1; w < WaAS_Data.NWAVES + 1; w++) {
            if (w == 1 || w == 5) {
                h += "," + vName + "W" + w + "_Count," + vName
                        + "W" + w + "_ZeroCount," + vName + "W" + w
                        + "_NegativeCount," + vName + "W" + w
                        + "_Average";
            }
        }
        env.log(h);
        Iterator<Byte> ite = gors.iterator();
        while (ite.hasNext()) {
            byte gor = ite.next();
            double[][] v = new double[WaAS_Data.NWAVES][];
            for (byte w = 0; w < WaAS_Data.NWAVES; w++) {
                if (w == 0) {
                    v[w] = Generic_Statistics.getSummaryStatistics(
                            vAllW1.get(gor).values());
                } else if (w == 4) {
                    v[w] = Generic_Statistics.getSummaryStatistics(
                            vAllW5.get(gor).values());
                }
            }
            double diff = v[4][4] - v[0][4];
            String s = "" + gor + "," + GORNameLookup.get(gor) + "," + diff;
            for (byte w = 0; w < WaAS_Data.NWAVES; w++) {
                if (w == 0 || w == 4) {
                    s += "," + v[w][4] + "," + v[w][5] + "," + v[w][6] + ","
                            + v[w][7];
                }
            }
            env.log(s);
            r.put(gor, diff);
        }
        return r;
    }

}
