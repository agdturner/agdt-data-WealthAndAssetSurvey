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
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave1Or2Or3Or4Or5_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave1_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave2_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave3_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave4_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave5_HHOLD_Record;
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
    public Object[] loadHouseholdsInAllWaves(String type) {
        String m = "loadHouseholdsInAllWaves";
        env.logStartTag(m);
        Object[] r = new Object[5];
        /**
         * Step 1: Initial loading
         */
        /**
         * Step 1.1: Wave 5 initial load. After this load the main set of data
         * contains all those Wave 5 records that have Wave 4 record
         * identifiers.
         */
        r[4] = loadW5();
        TreeMap<Short, HashSet<Short>> CASEW4ToCASEW5;
        CASEW4ToCASEW5 = (TreeMap<Short, HashSet<Short>>) ((Object[]) r[4])[3];
        /**
         * Step 1.2: Wave 4 initial load. After this load the main set of data
         * contains all those Wave 4 records that have Wave 3 record identifiers
         * and that are in the main set loaded in Step 1.1.
         */
        r[3] = loadW4(CASEW4ToCASEW5.keySet(), WaAS_Strings.s_InW3W5);
        TreeMap<Short, HashSet<Short>> CASEW3ToCASEW4;
        CASEW3ToCASEW4 = (TreeMap<Short, HashSet<Short>>) ((Object[]) r[3])[3];
        /**
         * Step 1.3: Wave 3 initial load. After this load the main set of data
         * contains all those Wave 3 records that have Wave 2 record identifiers
         * and that are in the main set loaded in Step 1.2.
         */
        r[2] = loadW3(CASEW3ToCASEW4.keySet(), WaAS_Strings.s_InW2W4W5);
        TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3;
        CASEW2ToCASEW3 = (TreeMap<Short, HashSet<Short>>) ((Object[]) r[2])[3];
        /**
         * Step 1.4: Wave 2 initial load. After this load the main set of data
         * contains all those Wave 2 records that have Wave 1 record identifiers
         * and that are in the main set loaded in Step 1.3.
         */
        r[1] = loadW2(CASEW2ToCASEW3.keySet(), WaAS_Strings.s_InW1W3W4W5);
        TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2;
        CASEW1ToCASEW2 = (TreeMap<Short, HashSet<Short>>) ((Object[]) r[1])[3];
        TreeMap<Short, Short> CASEW2ToCASEW1;
        CASEW2ToCASEW1 = (TreeMap<Short, Short>) ((Object[]) r[1])[2];
        /**
         * Step 1.5: Wave 1 initial load. After this load the main set of data
         * contains all those Wave 1 records that are in the main set loaded in
         * Step 1.4.
         */
        r[0] = loadW1(CASEW1ToCASEW2.keySet(), WaAS_Strings.s_InW2W4W5);
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
        /**
         * Step 2.2.1: Wave 1.
         */
        TreeMap<Short, WaAS_Wave1_HHOLD_Record> w1recs = new TreeMap<>();
        TreeSet<WaAS_ID> CASEW1IDs = new TreeSet<>();
        ite = w1recs0.keySet().iterator();
        while (ite.hasNext()) {
            short CASEW1 = ite.next();
            WaAS_ID ID = new WaAS_ID(CASEW1, CASEW1);
            CASEW1IDs.add(ID);
            w1recs.put(CASEW1, w1recs0.get(CASEW1));
        }
        env.log("There are " + CASEW1IDs.size() + " CASEW1IDs.");
        cacheSubset(W1, w1recs, type);
        /**
         * Step 2.2.2: Wave 2.
         */
        TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2Subset = new TreeMap<>();
        TreeMap<Short, Short> CASEW2ToCASEW1Subset = new TreeMap<>();
        TreeMap<Short, WaAS_Wave2_HHOLD_Record> w2recs = new TreeMap<>();
        TreeSet<WaAS_ID> CASEW2IDs = new TreeSet<>();
        ite = w2recs0.keySet().iterator();
        while (ite.hasNext()) {
            short CASEW2 = ite.next();
            short CASEW1 = CASEW2ToCASEW1.get(CASEW2);
            WaAS_ID ID = new WaAS_ID(CASEW1, CASEW2);
            CASEW2IDs.add(ID);
            w2recs.put(CASEW2, w2recs0.get(CASEW2));
            Generic_Collections.addToMap(CASEW1ToCASEW2Subset, CASEW1, CASEW2);
            CASEW2ToCASEW1Subset.put(CASEW2, CASEW1);
        }
        env.log("There are " + CASEW2IDs.size() + " CASEW2IDs.");
        cacheSubset(W2, w2recs, type);
        env.log("There are " + CASEW1ToCASEW2Subset.size() + " CASEW1ToCASEW2Subset mappings.");
        env.log("There are " + CASEW2ToCASEW1Subset.size() + " CASEW2ToCASEW1Subset mappings.");
        cacheSubsetLookups(W1, CASEW1ToCASEW2Subset, CASEW2ToCASEW1Subset);
        /**
         * Step 2.2.3: Wave 3.
         */
        TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3Subset = new TreeMap<>();
        TreeMap<Short, Short> CASEW3ToCASEW2Subset = new TreeMap<>();
        TreeMap<Short, WaAS_Wave3_HHOLD_Record> w3recs = new TreeMap<>();
        TreeSet<WaAS_ID> CASEW3IDs = new TreeSet<>();
        HashMap<Short, Short> CASEW3ToCASEW2 = new HashMap<>();
        ite = w3recs0.keySet().iterator();
        while (ite.hasNext()) {
            short CASEW3 = ite.next();
            short CASEW2 = w3recs0.get(CASEW3).getCASEW2();
            if (CASEW2ToCASEW1Subset.containsKey(CASEW2)) {
                CASEW3ToCASEW2.put(CASEW3, CASEW2);
                short CASEW1 = CASEW2ToCASEW1Subset.get(CASEW2);
                HashSet<Short> CASEW3s = CASEW2ToCASEW3.get(CASEW2);
                Iterator<Short> ite2 = CASEW3s.iterator();
                while (ite2.hasNext()) {
                    CASEW3 = ite2.next();
                    WaAS_ID ID = new WaAS_ID(CASEW1, CASEW3);
                    CASEW3IDs.add(ID);
                    w3recs.put(CASEW3, w3recs0.get(CASEW3));
                    Generic_Collections.addToMap(CASEW2ToCASEW3Subset, CASEW2,
                            CASEW3);
                    CASEW3ToCASEW2Subset.put(CASEW3, CASEW2);
                }
            }
        }
        env.log("There are " + CASEW3IDs.size() + " CASEW3IDs.");
        cacheSubset(W3, w3recs, type);
        env.log("There are " + CASEW2ToCASEW3Subset.size()
                + " CASEW2ToCASEW3Subset mappings.");
        env.log("There are " + CASEW3ToCASEW2Subset.size()
                + " CASEW3ToCASEW2Subset mappings.");
        cacheSubsetLookups(W2, CASEW2ToCASEW3Subset,
                CASEW3ToCASEW2Subset);
        /**
         * Step 2.2.4: Wave 4.
         */
        TreeMap<Short, HashSet<Short>> CASEW3ToCASEW4Subset = new TreeMap<>();
        TreeMap<Short, Short> CASEW4ToCASEW3Subset = new TreeMap<>();
        TreeMap<Short, WaAS_Wave4_HHOLD_Record> w4recs = new TreeMap<>();
        HashMap<Short, Short> CASEW4ToCASEW3 = new HashMap<>();
        TreeSet<WaAS_ID> CASEW4IDs = new TreeSet<>();
        ite = w4recs0.keySet().iterator();
        while (ite.hasNext()) {
            short CASEW4 = ite.next();
            short CASEW3 = w4recs0.get(CASEW4).getCASEW3();
            if (CASEW3ToCASEW2Subset.containsKey(CASEW3)) {
                CASEW4ToCASEW3.put(CASEW4, CASEW3);
                short CASEW2 = CASEW3ToCASEW2Subset.get(CASEW3);
                short CASEW1 = CASEW2ToCASEW1Subset.get(CASEW2);
                HashSet<Short> CASEW4s = CASEW3ToCASEW4.get(CASEW3);
                Iterator<Short> ite2 = CASEW4s.iterator();
                while (ite2.hasNext()) {
                    CASEW4 = ite2.next();
                    WaAS_ID ID = new WaAS_ID(CASEW1, CASEW4);
                    CASEW4IDs.add(ID);
                    w4recs.put(CASEW4, w4recs0.get(CASEW4));
                    Generic_Collections.addToMap(CASEW3ToCASEW4Subset, CASEW3,
                            CASEW4);
                    CASEW4ToCASEW3Subset.put(CASEW4, CASEW3);
                }
            }
        }
        env.log("There are " + CASEW4IDs.size() + " CASEW4IDs.");
        cacheSubset(W4, w4recs, type);
        env.log("There are " + CASEW3ToCASEW4Subset.size()
                + " CASEW3ToCASEW4Subset mappings.");
        env.log("There are " + CASEW4ToCASEW3Subset.size()
                + " CASEW4ToCASEW3Subset mappings.");
        cacheSubsetLookups(W3, CASEW3ToCASEW4Subset,
                CASEW4ToCASEW3Subset);
        /**
         * Step 2.2.5: Wave 5.
         */
        TreeMap<Short, HashSet<Short>> CASEW4ToCASEW5Subset = new TreeMap<>();
        TreeMap<Short, Short> CASEW5ToCASEW4Subset = new TreeMap<>();
        TreeMap<Short, WaAS_Wave5_HHOLD_Record> w5recs = new TreeMap<>();
        HashMap<Short, Short> CASEW5ToCASEW4 = new HashMap<>();
        TreeSet<WaAS_ID> CASEW5IDs = new TreeSet<>();
        ite = w5recs0.keySet().iterator();
        while (ite.hasNext()) {
            short CASEW5 = ite.next();
            short CASEW4 = w5recs0.get(CASEW5).getCASEW4();
            if (CASEW4ToCASEW3Subset.containsKey(CASEW4)) {
                CASEW5ToCASEW4.put(CASEW5, CASEW4);
                short CASEW3 = CASEW4ToCASEW3Subset.get(CASEW4);
                short CASEW2 = CASEW3ToCASEW2Subset.get(CASEW3);
                short CASEW1 = CASEW2ToCASEW1Subset.get(CASEW2);
                HashSet<Short> CASEW5s = CASEW4ToCASEW5.get(CASEW4);
                Iterator<Short> ite2 = CASEW5s.iterator();
                while (ite2.hasNext()) {
                    CASEW5 = ite2.next();
                    WaAS_ID ID = new WaAS_ID(CASEW1, CASEW5);
                    CASEW5IDs.add(ID);
                    w5recs.put(CASEW5, w5recs0.get(CASEW5));
                    Generic_Collections.addToMap(CASEW4ToCASEW5Subset, CASEW4,
                            CASEW5);
                    CASEW5ToCASEW4Subset.put(CASEW5, CASEW4);
                }
            }
        }
        env.log("There are " + CASEW5IDs.size() + " CASEW5IDs.");
        cacheSubset(W5, w5recs, type);
        env.log("There are " + CASEW4ToCASEW5Subset.size()
                + " CASEW4ToCASEW5Subset mappings.");
        env.log("There are " + CASEW5ToCASEW4Subset.size()
                + " CASEW5ToCASEW4Subset mappings.");
        cacheSubsetLookups(W4, CASEW4ToCASEW5Subset, CASEW5ToCASEW4Subset);
        env.logEndTag(m);
        return r;
    }

    /**
     * Loads all hhold in paired waves.
     *
     * @param wave Currently expected to be
     * {@link #W2}, {@link #W3}, {@link #W4}, or {@link #W5}.
     * @return an Object[] r of length 4. Each element is an Object[] containing
     * the data from loading the wave.
     * <ul>
     * <li>If wave == {@link #W2} then:
     * <ul>
     * <li>r[0] is a TreeMap with keys as CASEW2 and values as
     * WaAS_Wave2_HHOLD_Records. It contains all records from Wave 2 that have a
     * CASEW2 value.</li>
     * <li>r[1] is an array of TreeSets where:
     * <ul>
     * <li>r[1][0] is a list of CASEW1 values in Wave 2 records.</li>
     * <li>r[1][1] is a list of all CASEW2 values.</li>
     * <li>r[1][2] is a list of CASEW2 values for records that have CASEW1
     * values.</li>
     * </ul>
     * </li>
     * <li>r[2] is a TreeMap with keys as CASEW2 and values as CASEW2.</li>
     * <li>r[3] is a TreeMap with keys as CASEW2 and values as HashSets of
     * CASEW2 (which is normally expected to contain just one CASEW2).</li>
     * </ul></li>
     * <li>If wave == {@link #W3} then:
     * <ul>
     * <li>r[0] is a TreeMap with keys as CASEW3 and values as
     * WaAS_Wave3_HHOLD_Records. It contains all records from Wave 3 that have a
     * CASEW2 value.</li>
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
     * </ul></li>
     * <li>If wave == {@link #W4} then:
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
     * </ul></li>
     * <li>If wave == {@link #W5} then:
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
     * </ul></li>
     * </ul>
     */
    public Object[] loadHouseholdsInPreviousWave(byte wave) {
        String m = "loadHouseholdsInPreviousWave(" + wave + ")";
        env.logStartTag(m);
        Object[] r = new Object[5];
        if (wave == W2) {
            r = loadW2();
        } else if (wave == W3) {
            r = loadW3();
        } else if (wave == W4) {
            r = loadW4();
        } else if (wave == W5) {
            r = loadW5();
        } else {
            env.log("Erroneous Wave " + wave);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 5 records that are reportedly in Wave 4 (those with CASEW4
     * values).
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
    public Object[] loadW5() {
        String m = "loadW5";
        env.logStartTag(m);
        Object[] r;
        File cf = getSubsetCacheFile2(W5, "InW4");
        if (cf.exists()) {
            r = (Object[]) load(W5, cf);
        } else {
            r = new Object[4];
            File f = getInputFile(W5);
            TreeMap<Short, WaAS_Wave5_HHOLD_Record> r0 = new TreeMap<>();
            r[0] = r0;
            TreeSet<Short>[] r1 = getSets(W5 + 1);
            r[1] = r1;
            /**
             * Each hhold in Wave 5 comes from at most one hhold from Wave 4. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 4 into a hhold in Wave 5. This
             * is expected to be rare. One example explanation for this
             * happening is someone returning to a hhold having left it.
             */
            TreeMap<Short, Short> CASEW5ToCASEW4 = new TreeMap<>();
            r[2] = CASEW5ToCASEW4;
            /**
             * There may be instances where hholds from Wave 4 split into two or
             * more hholds in Wave 5.
             */
            TreeMap<Short, HashSet<Short>> CASEW4ToCASEW5 = new TreeMap<>();
            r[3] = CASEW4ToCASEW5;
            String m1 = getMessage(W5, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            int count = br.lines().skip(1).mapToInt(l -> {
                WaAS_Wave5_HHOLD_Record rec = new WaAS_Wave5_HHOLD_Record(l);
                short CASEW4 = rec.getCASEW4();
                if (CASEW4 > Short.MIN_VALUE) {
                    if (!r1[3].add(CASEW4)) {
                        env.log("Between Wave 4 and 5: hhold with CASEW4 "
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
            br.lines().skip(1).forEach(l -> {
                WaAS_Wave5_HHOLD_Record rec = new WaAS_Wave5_HHOLD_Record(l);
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
                    if (CASEW2 > Short.MIN_VALUE && CASEW3 > Short.MIN_VALUE
                            && CASEW4 > Short.MIN_VALUE) {
                        r1[5].add(CASEW5);
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
    public TreeMap<Short, WaAS_Wave5_HHOLD_Record> loadAllW5() {
        String m = "loadAllW5";
        env.logStartTag(m);
        TreeMap<Short, WaAS_Wave5_HHOLD_Record> r;
        File cf = getGeneratedAllFile(W5);
        if (cf.exists()) {
            r = (TreeMap<Short, WaAS_Wave5_HHOLD_Record>) load(W5, cf);
        } else {
            r = new TreeMap<>();
            File f = getInputFile(W5);
            String m1 = getMessage(W5, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(l -> {
                WaAS_Wave5_HHOLD_Record rec = new WaAS_Wave5_HHOLD_Record(l);
                short CASEW5 = rec.getCASEW5();
                r.put(CASEW5, rec);
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
    public TreeMap<Short, WaAS_Wave4_HHOLD_Record> loadAllW4() {
        String m = "loadAllW4";
        env.logStartTag(m);
        TreeMap<Short, WaAS_Wave4_HHOLD_Record> r;
        File cf = getGeneratedAllFile(W4);
        if (cf.exists()) {
            r = (TreeMap<Short, WaAS_Wave4_HHOLD_Record>) load(W4, cf);
        } else {
            r = new TreeMap<>();
            File f = getInputFile(W4);
            String m1 = getMessage(W4, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(l -> {
                WaAS_Wave4_HHOLD_Record rec = new WaAS_Wave4_HHOLD_Record(l);
                short CASEW4 = rec.getCASEW4();
                r.put(CASEW4, rec);
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
     * @return a TreeMap with keys as CASEW3 and values as
     * WaAS_Wave3_HHOLD_Records.
     */
    public TreeMap<Short, WaAS_Wave3_HHOLD_Record> loadAllW3() {
        String m = "loadAllW3";
        env.logStartTag(m);
        TreeMap<Short, WaAS_Wave3_HHOLD_Record> r;
        File cf = getGeneratedAllFile(W3);
        if (cf.exists()) {
            r = (TreeMap<Short, WaAS_Wave3_HHOLD_Record>) load(W3, cf);
        } else {
            r = new TreeMap<>();
            File f = getInputFile(W3);
            String m1 = getMessage(W3, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(l -> {
                WaAS_Wave3_HHOLD_Record rec = new WaAS_Wave3_HHOLD_Record(l);
                short CASEW3 = rec.getCASEW3();
                r.put(CASEW3, rec);
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
    public TreeMap<Short, WaAS_Wave2_HHOLD_Record> loadAllW2() {
        String m = "loadAllW2";
        env.logStartTag(m);
        TreeMap<Short, WaAS_Wave2_HHOLD_Record> r;
        File cf = getGeneratedAllFile(W2);
        if (cf.exists()) {
            r = (TreeMap<Short, WaAS_Wave2_HHOLD_Record>) load(W2, cf);
        } else {
            r = new TreeMap<>();
            File f = getInputFile(W2);
            String m1 = getMessage(W2, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(l -> {
                WaAS_Wave2_HHOLD_Record rec = new WaAS_Wave2_HHOLD_Record(l);
                short CASEW2 = rec.getCASEW2();
                r.put(CASEW2, rec);
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
    public TreeMap<Short, WaAS_Wave1_HHOLD_Record> loadAllW1() {
        String m = "loadAllW1";
        env.logStartTag(m);
        TreeMap<Short, WaAS_Wave1_HHOLD_Record> r;
        File cf = getGeneratedAllFile(W1);
        if (cf.exists()) {
            r = (TreeMap<Short, WaAS_Wave1_HHOLD_Record>) load(W1, cf);
        } else {
            r = new TreeMap<>();
            File f = getInputFile(W1);
            String m1 = getMessage(W1, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(l -> {
                WaAS_Wave1_HHOLD_Record rec = new WaAS_Wave1_HHOLD_Record(l);
                short CASEW1 = rec.getCASEW1();
                r.put(CASEW1, rec);
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
     * @param n
     * @return
     */
    protected TreeSet<Short>[] getSets(int n) {
        TreeSet<Short>[] r = new TreeSet[n];
        for (int i = 0; i < n; i++) {
            r[i] = new TreeSet<>();
        }
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
    protected File getGeneratedAllFile(short wave) {
        return new File(files.getGeneratedWaASDir(), TYPE + WaAS_Strings.s_W
                + wave + WaAS_Strings.s_All + WaAS_Files.DOT_DAT);
    }

    /**
     * Load Wave 4 records that have CASEW4 values in {@code s}.
     *
     * @param s a set containing CASEW3 values.
     * @param type for loading an already computed result. Expected values
     * include: "InW3W5" and "InW5".
     *
     * @return r - An Object[] of length 4:
     * <ul>
     * <li>r[0] is a TreeMap with keys as CASEW4 and values as
     * WaAS_Wave4_HHOLD_Records. For {@code type.equalsIgnoreCase("InW3W5")}
     * this only contains records for households also in Wave 3 and Wave 5. For
     * {@code type.equalsIgnoreCase("InW4")} this only contains records for
     * households also in Wave 5.</li>
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
    public Object[] loadW4(Set<Short> s, String type) {
        String m = "loadW4(Set<Short>, " + type + ")";
        env.logStartTag(m);
        Object[] r;
        File cf = getSubsetCacheFile2(W4, type);
        if (cf.exists()) {
            r = (Object[]) load(W4, cf);
        } else {
            r = new Object[4];
            File f = getInputFile(W4);
            TreeMap<Short, WaAS_Wave4_HHOLD_Record> r0 = new TreeMap<>();
            r[0] = r0;
            TreeSet<Short>[] r1 = getSets(W4 + 1);
            r[1] = r1;
            /**
             * Each hhold in Wave 4 comes from at most one hhold from Wave 3. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 3 into a hhold in Wave 4. This
             * is expected to be rare. One example explanation for this
             * happening is someone returning to a hhold having left it.
             */
            TreeMap<Short, Short> CASEW4ToCASEW3 = new TreeMap<>();
            r[2] = CASEW4ToCASEW3;
            /**
             * There may be instances where hholds from Wave 3 split into two or
             * more hholds in Wave 4.
             */
            TreeMap<Short, HashSet<Short>> CASEW3ToCASEW4 = new TreeMap<>();
            r[3] = CASEW3ToCASEW4;
            String m1 = getMessage(W4, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            int count = br.lines().skip(1).mapToInt(l -> {
                WaAS_Wave4_HHOLD_Record rec = new WaAS_Wave4_HHOLD_Record(l);
                short CASEW3 = rec.getCASEW3();
                if (CASEW3 > Short.MIN_VALUE) {
                    if (!r1[2].add(CASEW3)) {
                        env.log("Between Wave 3 and 4: hhold with CASEW3 "
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
            if (type.equalsIgnoreCase("InW3W5")) {
                br.lines().skip(1).forEach(l -> {
                    WaAS_Wave4_HHOLD_Record rec = new WaAS_Wave4_HHOLD_Record(l);
                    short CASEW4 = rec.getCASEW4();
                    short CASEW3 = rec.getCASEW3();
                    short CASEW2 = rec.getCASEW2();
                    short CASEW1 = rec.getCASEW1();
                    if (s.contains(CASEW4)) {
                        if (CASEW3 > Short.MIN_VALUE) {
                            CASEW4ToCASEW3.put(CASEW4, CASEW3);
                            Generic_Collections.addToMap(CASEW3ToCASEW4, CASEW3,
                                    CASEW4);
                            r0.put(CASEW4, rec);
                        }
                    }
                    r1[3].add(CASEW4);
                    if (CASEW2 > Short.MIN_VALUE) {
                        r1[1].add(CASEW2);
                    }
                    if (CASEW1 > Short.MIN_VALUE) {
                        r1[0].add(CASEW1);
                        if (CASEW2 > Short.MIN_VALUE
                                && CASEW3 > Short.MIN_VALUE) {
                            r1[4].add(CASEW4);
                        }
                    }
                });
            } else if (type.equalsIgnoreCase("InW5")) {
                br.lines().skip(1).forEach(l -> {
                    WaAS_Wave4_HHOLD_Record rec = new WaAS_Wave4_HHOLD_Record(l);
                    short CASEW4 = rec.getCASEW4();
                    short CASEW3 = rec.getCASEW3();
                    short CASEW2 = rec.getCASEW2();
                    short CASEW1 = rec.getCASEW1();
                    if (s.contains(CASEW4)) {
                        r0.put(CASEW4, rec);
                        if (CASEW3 > Short.MIN_VALUE) {
                            CASEW4ToCASEW3.put(CASEW4, CASEW3);
                            Generic_Collections.addToMap(CASEW3ToCASEW4, CASEW3,
                                    CASEW4);
                        }
                    }
                    r1[3].add(CASEW4);
                    if (CASEW2 > Short.MIN_VALUE) {
                        r1[1].add(CASEW2);
                    }
                    if (CASEW1 > Short.MIN_VALUE) {
                        r1[0].add(CASEW1);
                        if (CASEW2 > Short.MIN_VALUE
                                && CASEW3 > Short.MIN_VALUE) {
                            r1[4].add(CASEW4);
                        }
                    }
                });
            } else {
                env.log("Unrecognised type " + type);
            }
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W4, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 4 records that are reportedly in Wave 3 (those with CASEW3
     * values).
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
    public Object[] loadW4() {
        String m = "loadW4";
        env.logStartTag(m);
        Object[] r;
        File cf = getSubsetCacheFile2(W4, "InW3");
        if (cf.exists()) {
            r = (Object[]) load(W4, cf);
        } else {
            r = new Object[4];
            File f = getInputFile(W4);
            TreeMap<Short, WaAS_Wave4_HHOLD_Record> r0 = new TreeMap<>();
            r[0] = r0;
            TreeSet<Short>[] r1 = getSets(W4 + 1);
            r[1] = r1;
            /**
             * Each hhold in Wave 4 comes from at most one hhold from Wave 3. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 3 into a hhold in Wave 4. This
             * is expected to be rare. One example explanation for this
             * happening is someone returning to a hhold having left it.
             */
            TreeMap<Short, Short> CASEW4ToCASEW3 = new TreeMap<>();
            r[2] = CASEW4ToCASEW3;
            /**
             * There may be instances where hholds from Wave 3 split into two or
             * more hholds in Wave 4.
             */
            TreeMap<Short, HashSet<Short>> CASEW3ToCASEW4 = new TreeMap<>();
            r[3] = CASEW3ToCASEW4;
            String m0 = getMessage(W4, f);
            env.logStartTag(m0);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            int count = br.lines().skip(1).mapToInt(l -> {
                WaAS_Wave4_HHOLD_Record rec = new WaAS_Wave4_HHOLD_Record(l);
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
            br.lines().skip(1).forEach(l -> {
                WaAS_Wave4_HHOLD_Record rec = new WaAS_Wave4_HHOLD_Record(l);
                short CASEW4 = rec.getCASEW4();
                short CASEW3 = rec.getCASEW3();
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (CASEW3 > Short.MIN_VALUE) {
                    CASEW4ToCASEW3.put(CASEW4, CASEW3);
                    Generic_Collections.addToMap(CASEW3ToCASEW4, CASEW3,
                            CASEW4);
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
            cache(W4, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 3 records that are reportedly in Wave 2 (those with CASEW2
     * values).
     *
     * @return r An Object[] of length 4:
     * <ul>
     * <li>r[0] is a TreeMap with keys as CASEW3 and values as
     * WaAS_Wave3_HHOLD_Records. It contains all records from Wave 3 that have a
     * CASEW2 value.</li>
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
    public Object[] loadW3() {
        String m = "loadW3";
        env.logStartTag(m);
        Object[] r;
        File cf = getSubsetCacheFile2(W3, "InW2");
        if (cf.exists()) {
            r = (Object[]) load(W3, cf);
        } else {
            r = new Object[4];
            File f = getInputFile(W3);
            TreeMap<Short, WaAS_Wave3_HHOLD_Record> r0 = new TreeMap<>();
            r[0] = r0;
            TreeSet<Short>[] r1 = getSets(W3 + 1);
            r[1] = r1;
            /**
             * Each hhold in Wave 3 comes from at most one hhold from Wave 2. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 2 into a hhold in Wave 3. This
             * is expected to be rare. One example explanation for this
             * happening is someone returning to a hhold having left it.
             */
            TreeMap<Short, Short> CASEW3ToCASEW2 = new TreeMap<>();
            r[2] = CASEW3ToCASEW2;
            /**
             * There may be instances where hholds from Wave 2 split into two or
             * more hholds in Wave 3.
             */
            TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3 = new TreeMap<>();
            r[3] = CASEW2ToCASEW3;
            String m0 = getMessage(W3, f);
            env.logStartTag(m0);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            int count = br.lines().skip(1).mapToInt(l -> {
                WaAS_Wave3_HHOLD_Record rec = new WaAS_Wave3_HHOLD_Record(l);
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
            br.lines().skip(1).forEach(l -> {
                WaAS_Wave3_HHOLD_Record rec = new WaAS_Wave3_HHOLD_Record(l);
                short CASEW3 = rec.getCASEW3();
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (CASEW2 > Short.MIN_VALUE) {
                    CASEW3ToCASEW2.put(CASEW3, CASEW2);
                    Generic_Collections.addToMap(CASEW2ToCASEW3, CASEW2,
                            CASEW3);
                    r0.put(CASEW3, rec);
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
            env.logEndTag(m0);
            cache(W3, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 3 records that have CASEW3 values in {@code s}.
     *
     * @param s a set containing CASEW3 values.
     * @param type for loading an already computed result. Expected values
     * include: "InW2W4W5" and "InW4".
     *
     * @return r An Object[] of length 4:
     * <ul>
     * <li>r[0] is a TreeMap with keys as CASEW3 and values as
     * WaAS_Wave3_HHOLD_Records. For {@code type.equalsIgnoreCase("InW2W4W5")}
     * this only contains records for households also in Wave 2, Wave 4 and Wave
     * 5. For {@code type.equalsIgnoreCase("InW4")} this only contains records
     * for households also in Wave 4.</li>
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
    public Object[] loadW3(Set<Short> s, String type) {
        String m = "loadW3(Set<Short>, " + type + ")";
        env.logStartTag(m);
        Object[] r;
        File cf = getSubsetCacheFile2(W3, type);
        if (cf.exists()) {
            r = (Object[]) load(W3, cf);
        } else {
            r = new Object[4];
            File f = getInputFile(W3);
            TreeMap<Short, WaAS_Wave3_HHOLD_Record> r0 = new TreeMap<>();
            r[0] = r0;
            TreeSet<Short>[] r1 = getSets(W3 + 1);
            r[1] = r1;
            /**
             * Each hhold in Wave 3 comes from at most one hhold from Wave 2. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 2 into a hhold in Wave 3. This
             * is expected to be rare. One example explanation for this
             * happening is someone returning to a hhold having left it.
             */
            TreeMap<Short, Short> CASEW3ToCASEW2 = new TreeMap<>();
            r[2] = CASEW3ToCASEW2;
            /**
             * There may be instances where hholds from Wave 2 split into two or
             * more hholds in Wave 3.
             */
            TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3 = new TreeMap<>();
            r[3] = CASEW2ToCASEW3;
            String m1 = getMessage(W3, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            int count = br.lines().skip(1).mapToInt(l -> {
                WaAS_Wave3_HHOLD_Record rec = new WaAS_Wave3_HHOLD_Record(l);
                short CASEW2 = rec.getCASEW2();
                if (CASEW2 > Short.MIN_VALUE) {
                    if (!r1[1].add(CASEW2)) {
                        env.log("Between Wave 2 and 3: hhold with CASEW2 "
                                + CASEW2 + " reportedly split into multiple "
                                + "hholds.");
                        return 1;
                    }
                }
                return 0;
            }).sum();
            env.log("There are " + count + " hholds from Wave 2 "
                    + "reportedly split into multiple hholds in Wave 3.");
            // Close and reopen br
            br = Generic_IO.closeAndGetBufferedReader(br, f);
            if (type.equalsIgnoreCase("InW2W4W5")) {
                br.lines().skip(1).forEach(l -> {
                    WaAS_Wave3_HHOLD_Record rec = new WaAS_Wave3_HHOLD_Record(l);
                    short CASEW3 = rec.getCASEW3();
                    short CASEW2 = rec.getCASEW2();
                    short CASEW1 = rec.getCASEW1();
                    if (s.contains(CASEW3)) {
                        if (CASEW2 > Short.MIN_VALUE) {
                            CASEW3ToCASEW2.put(CASEW3, CASEW2);
                            Generic_Collections.addToMap(CASEW2ToCASEW3, CASEW2,
                                    CASEW3);
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
            } else if (type.equalsIgnoreCase("InW4")) {
                br.lines().skip(1).forEach(l -> {
                    WaAS_Wave3_HHOLD_Record rec = new WaAS_Wave3_HHOLD_Record(l);
                    short CASEW3 = rec.getCASEW3();
                    short CASEW2 = rec.getCASEW2();
                    short CASEW1 = rec.getCASEW1();
                    if (s.contains(CASEW3)) {
                        r0.put(CASEW3, rec);
                        if (CASEW2 > Short.MIN_VALUE) {
                            CASEW3ToCASEW2.put(CASEW3, CASEW2);
                            Generic_Collections.addToMap(CASEW2ToCASEW3, CASEW2,
                                    CASEW3);
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
            } else {
                env.log("Unrecognised type " + type);
            }
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W3, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 2 records that are reportedly in Wave 1 (those with CASEW1
     * values).
     *
     * @return r An Object[] of length 3:
     * <ul>
     * <li>r[0] is a TreeMap with keys as CASEW2 and values as
     * WaAS_Wave2_HHOLD_Records. It contains all records from Wave 2 that have a
     * CASEW2 value.</li>
     * <li>r[1] is an array of TreeSets where:
     * <ul>
     * <li>r[1][0] is a list of CASEW1 values in Wave 2 records.</li>
     * <li>r[1][1] is a list of all CASEW2 values.</li>
     * <li>r[1][2] is a list of CASEW2 values for records that have CASEW1
     * values.</li>
     * </ul>
     * </li>
     * <li>r[2] is a TreeMap with keys as CASEW2 and values as CASEW2.</li>
     * <li>r[3] is a TreeMap with keys as CASEW2 and values as HashSets of
     * CASEW2 (which is normally expected to contain just one CASEW2).</li>
     * </ul>
     */
    public Object[] loadW2() {
        String m = "loadW2";
        env.logStartTag(m);
        Object[] r;
        File cf = getSubsetCacheFile2(W2, "InW1");
        if (cf.exists()) {
            r = (Object[]) load(W2, cf);
        } else {
            r = new Object[4];
            File f = getInputFile(W2);
            TreeMap<Short, WaAS_Wave2_HHOLD_Record> r0 = new TreeMap<>();
            r[0] = r0;
            TreeSet<Short>[] r1 = getSets(W2 + 1);
            r[1] = r1;
            /**
             * Each hhold in Wave 2 comes from at most one hhold from Wave 1. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 1 into a hhold in Wave 2. This
             * is expected to be rare. One example explanation for this
             * happening is someone returning to a hhold having left it.
             */
            TreeMap<Short, Short> CASEW2ToCASEW1 = new TreeMap<>();
            r[2] = CASEW2ToCASEW1;
            /**
             * There may be instances where hholds from Wave 1 split into two or
             * more hholds in Wave 2.
             */
            TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2 = new TreeMap<>();
            r[3] = CASEW1ToCASEW2;
            String m0 = getMessage(W2, f);
            env.logStartTag(m0);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            int count = br.lines().skip(1).mapToInt(l -> {
                WaAS_Wave2_HHOLD_Record rec = new WaAS_Wave2_HHOLD_Record(l);
                short CASEW1 = rec.getCASEW1();
                if (CASEW1 > Short.MIN_VALUE) {
                    if (!r1[0].add(CASEW1)) {
                        env.log("In Wave 2: hhold with CASEW1 " + CASEW1
                                + " reportedly split into multiple hholds.");
                        return 1;
                    }
                }
                return 0;
            }).sum();
            /**
             * <li>r[1][0] is a list of CASEW1 values in Wave 2 records.</li>
             * <li>r[1][1] is a list of all CASEW2 values.</li>
             * <li>r[1][2] is a list of CASEW2 values for records that have
             * CASEW1
             */
            env.log("There are " + count + " hholds from Wave 1 "
                    + "reportedly split into multiple hholds in Wave 2.");
            // Close and reopen br
            br = Generic_IO.closeAndGetBufferedReader(br, f);
            br.lines().skip(1).forEach(l -> {
                WaAS_Wave2_HHOLD_Record rec = new WaAS_Wave2_HHOLD_Record(l);
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (CASEW1 > Short.MIN_VALUE) {
                    CASEW2ToCASEW1.put(CASEW2, CASEW1);
                    Generic_Collections.addToMap(CASEW1ToCASEW2, CASEW1,
                            CASEW2);
                    r0.put(CASEW2, rec);
                    r1[2].add(CASEW2);
                }
                r1[1].add(CASEW2);
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
     * @param type for loading an already computed result. Expected values
     * include: {@link WaAS_Strings#s_InW1W2W3W4W5}, {@link WaAS_Strings#s_InW3}
     * and {@link WaAS_Strings#s_InW1}
     *
     * @return An {@code Object[] r} of length 4:
     * <ul>
     * <li>{@code r[0]} is a TreeMap with keys as CASEW2 and values as
     * WaAS_Wave2_HHOLD_Records. For {@code type.equalsIgnoreCase("InW1W3W4W5")}
     * this only contains records for households also in Wave 1, Wave 3, Wave 4
     * and Wave 5. For {@code type.equalsIgnoreCase("InW3")} this only contains
     * records for households also in Wave 3.</li>
     * <li>{@code r[1]} is an array of TreeSets where:
     * <ul>
     * <li>{@code r[1][0]} is a list of CASEW1 values in Wave 2 records.</li>
     * <li>{@code r[1][1]} is a list of all CASEW2 values.</li>
     * <li>{@code r[1][2]} is a list of CASEW1 values in Wave 2 records that
     * have Wave 5, Wave 4 and Wave 3 records.</li>
     * </ul>
     * </li>
     * <li>{@code r[2]} is a TreeMap with keys as CASEW2 and values as
     * CASEW1.</li>
     * <li>{@code r[3]} is a TreeMap with keys as CASEW1 and values as HashSets
     * of CASEW2 (which is normally expected to contain just one CASEW2).</li>
     * </ul>
     */
    public Object[] loadW2(Set<Short> s, String type) {
        String m = "loadW2(Set<Short>, " + type + ")";
        env.logStartTag(m);
        Object[] r;
        File cf = getSubsetCacheFile2(W2, type);
        if (cf.exists()) {
            r = (Object[]) load(W2, cf);
        } else {
            r = new Object[4];
            File f = getInputFile(W2);
            TreeMap<Short, WaAS_Wave2_HHOLD_Record> r0 = new TreeMap<>();
            r[0] = r0;
            TreeSet<Short>[] r1 = getSets(W2 + 1);
            r[1] = r1;
            /**
             * Each hhold in Wave 2 comes from at most one hhold from Wave 1. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 1 into a hhold in Wave 2. This
             * is expected to be rare. One example explanation for this
             * happening is someone returning to a hhold having left it.
             */
            TreeMap<Short, Short> CASEW2ToCASEW1 = new TreeMap<>();
            r[2] = CASEW2ToCASEW1;
            /**
             * There may be instances where hholds from Wave 1 split into two or
             * more hholds in Wave 2.
             */
            TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2 = new TreeMap<>();
            r[3] = CASEW1ToCASEW2;
            String m1 = getMessage(W2, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            int count = br.lines().skip(1).mapToInt(l -> {
                WaAS_Wave2_HHOLD_Record rec = new WaAS_Wave2_HHOLD_Record(l);
                short CASEW1 = rec.getCASEW1();
                if (CASEW1 > Short.MIN_VALUE) {
                    if (!r1[0].add(CASEW1)) {
                        env.log("Between Wave 1 and 2: hhold with CASEW1 "
                                + CASEW1 + " reportedly split into multiple "
                                + "hholds.");
                        return 1;
                    }
                }
                return 0;
            }).sum();
            env.log("There are " + count + " hholds from Wave 1 "
                    + "reportedly split into multiple hholds in Wave 2.");
            // Close and reopen br
            br = Generic_IO.closeAndGetBufferedReader(br, f);
            if (type.equalsIgnoreCase("InW1W3W4W5")) {
                br.lines().skip(1).forEach(l -> {
                    WaAS_Wave2_HHOLD_Record rec = new WaAS_Wave2_HHOLD_Record(l);
                    short CASEW2 = rec.getCASEW2();
                    short CASEW1 = rec.getCASEW1();
                    if (s.contains(CASEW2)) {
                        if (CASEW1 > Short.MIN_VALUE) {
                            CASEW2ToCASEW1.put(CASEW2, CASEW1);
                            Generic_Collections.addToMap(CASEW1ToCASEW2, CASEW1,
                                    CASEW2);
                            r0.put(CASEW2, rec);
                            r1[2].add(CASEW1);
                        }
                    }
                    r1[1].add(CASEW2);
                });
            } else if (type.equalsIgnoreCase("InW3")) {
                br.lines().skip(1).forEach(l -> {
                    WaAS_Wave2_HHOLD_Record rec = new WaAS_Wave2_HHOLD_Record(l);
                    short CASEW2 = rec.getCASEW2();
                    short CASEW1 = rec.getCASEW1();
                    if (s.contains(CASEW2)) {
                        r0.put(CASEW2, rec);
                        if (CASEW1 > Short.MIN_VALUE) {
                            CASEW2ToCASEW1.put(CASEW2, CASEW1);
                            Generic_Collections.addToMap(CASEW1ToCASEW2, CASEW1,
                                    CASEW2);
                            r1[2].add(CASEW1);
                        }
                    }
                    r1[1].add(CASEW2);
                });
            } else {
                env.log("Unrecognised type " + type);
            }
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W2, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 1 records that have CASEW1 values in {@code s}.
     *
     * @param s a set containing CASEW1 values.
     * @param type for loading an already computed result. Expected values
     * include: {@link WaAS_Strings#s_InW1W2W3W4W5} and
     * {@link WaAS_Strings#s_InW2}.
     *
     * @return An {@code Object[] r} of length 2:
     * <ul>
     * <li>{@code r[0]} is a {@code TreeMap<Short, WaAS_Wave1_HHOLD_Record>}
     * with keys as {@Link WaAS_Wave1_HHOLD_Record#CASEW1} and values as
     * WaAS_Wave1_HHOLD_Records. For {@code type.equalsIgnoreCase("InW2W3W4W5")}
     * this only contains records for households also in Wave 2, Wave 3, Wave 4
     * and Wave 5. For {@code type.equalsIgnoreCase("InW2")} this only contains
     * records for households also in Wave 2.</li>
     * <li>{@code r[1]} is an array of TreeSets where:
     * <ul>
     * <li>{@code r[1][0]} is a list of all CASEW1 values.</li>
     * </ul>
     * </li>
     * <li>{@code r[2]} is a TreeMap with keys as CASEW2 and values as
     * CASEW1.</li>
     * <li>{@code r[3]} is a TreeMap with keys as CASEW1 and values as HashSets
     * of CASEW2 (which is normally expected to contain just one CASEW2).</li>
     * </ul>
     */
    public Object[] loadW1(Set<Short> s, String type) {
        String m = "loadW1(Set<Short>, " + type + ")";
        env.logStartTag(m);
        Object[] r;
        File cf = getSubsetCacheFile2(W1, type);
        if (cf.exists()) {
            r = (Object[]) load(W1, cf);
        } else {
            r = new Object[2];
            File f = getInputFile(W1);
            TreeMap<Short, WaAS_Wave1_HHOLD_Record> r0 = new TreeMap<>();
            r[0] = r0;
            TreeSet<Short>[] r1 = getSets(W1);
            r[1] = r1;
            String m1 = getMessage(W1, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(l -> {
                WaAS_Wave1_HHOLD_Record rec = new WaAS_Wave1_HHOLD_Record(l);
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
            cache(W1, cf, r);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * TreeMap<Short, WaAS_Wave1_HHOLD_Record>
     *
     * @param type
     * @return Object[]
     */
    public TreeMap<Short, WaAS_Wave1_HHOLD_Record> loadCachedSubsetW1(
            String type) {
        String m = "loadCachedSubsetW1(" + type + ")";
        env.logStartTag(m);
        TreeMap<Short, WaAS_Wave1_HHOLD_Record> r;
        File f = getSubsetCacheFile(W1, type);
        if (f.exists()) {
            r = (TreeMap<Short, WaAS_Wave1_HHOLD_Record>) Generic_IO.readObject(f);
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * TreeMap<Short, WaAS_Wave1_HHOLD_Record>
     *
     * @param type
     * @return Object[]
     */
    public TreeMap<Short, WaAS_Wave1_HHOLD_Record> loadCachedSubset2W1(
            String type) {
        String m = "loadCachedSubset2W1(" + type + ")";
        env.logStartTag(m);
        TreeMap<Short, WaAS_Wave1_HHOLD_Record> r;
        File f = getSubsetCacheFile2(W1, type);
        if (f.exists()) {
            Object[] o = (Object[]) Generic_IO.readObject(f);
            r = (TreeMap<Short, WaAS_Wave1_HHOLD_Record>) o[0];
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<Short, WaAS_Wave2_HHOLD_Record> loadCachedSubsetW2(
            String type) {
        String m = "loadCachedSubsetW2(" + type + ")";
        env.logStartTag(m);
        TreeMap<Short, WaAS_Wave2_HHOLD_Record> r;
        File f = getSubsetCacheFile(W2, type);
        if (f.exists()) {
            r = (TreeMap<Short, WaAS_Wave2_HHOLD_Record>) Generic_IO.readObject(f);
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<Short, WaAS_Wave2_HHOLD_Record> loadCachedSubset2W2(
            String type) {
        String m = "loadCachedSubset2W2(" + type + ")";
        env.logStartTag(m);
        TreeMap<Short, WaAS_Wave2_HHOLD_Record> r;
        File f = getSubsetCacheFile2(W2, type);
        if (f.exists()) {
            Object[] o = (Object[]) Generic_IO.readObject(f);
            r = (TreeMap<Short, WaAS_Wave2_HHOLD_Record>) o[0];
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<Short, WaAS_Wave3_HHOLD_Record> loadCachedSubsetW3(
            String type) {
        String m = "loadCachedSubsetW3(" + type + ")";
        env.logStartTag(m);
        TreeMap<Short, WaAS_Wave3_HHOLD_Record> r;
        File f = getSubsetCacheFile(W3, type);
        if (f.exists()) {
            r = (TreeMap<Short, WaAS_Wave3_HHOLD_Record>) Generic_IO.readObject(f);
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<Short, WaAS_Wave3_HHOLD_Record> loadCachedSubset2W3(
            String type) {
        String m = "loadCachedSubset2W3(" + type + ")";
        env.logStartTag(m);
        TreeMap<Short, WaAS_Wave3_HHOLD_Record> r;
        File f = getSubsetCacheFile2(W3, type);
        if (f.exists()) {
            Object[] o = (Object[]) Generic_IO.readObject(f);
            r = (TreeMap<Short, WaAS_Wave3_HHOLD_Record>) o[0];
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<Short, WaAS_Wave4_HHOLD_Record> loadCachedSubsetW4(
            String type) {
        String m = "loadCachedSubsetW4(" + type + ")";
        env.logStartTag(m);
        TreeMap<Short, WaAS_Wave4_HHOLD_Record> r;
        File f = getSubsetCacheFile(W4, type);
        if (f.exists()) {
            r = (TreeMap<Short, WaAS_Wave4_HHOLD_Record>) Generic_IO.readObject(f);
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<Short, WaAS_Wave4_HHOLD_Record> loadCachedSubset2W4(
            String type) {
        String m = "loadCachedSubset2W4(" + type + ")";
        env.logStartTag(m);
        TreeMap<Short, WaAS_Wave4_HHOLD_Record> r;
        File f = getSubsetCacheFile2(W4, type);
        if (f.exists()) {
            Object[] o = (Object[]) Generic_IO.readObject(f);
            r = (TreeMap<Short, WaAS_Wave4_HHOLD_Record>) o[0];
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<Short, WaAS_Wave5_HHOLD_Record> loadCachedSubsetW5(
            String type) {
        String m = "loadCachedSubsetW5(" + type + ")";
        env.logStartTag(m);
        TreeMap<Short, WaAS_Wave5_HHOLD_Record> r;
        File f = getSubsetCacheFile(W5, type);
        if (f.exists()) {
            r = (TreeMap<Short, WaAS_Wave5_HHOLD_Record>) Generic_IO.readObject(f);
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<Short, WaAS_Wave5_HHOLD_Record> loadCachedSubset2W5(
            String type) {
        String m = "loadCachedSubset2W5(" + type + ")";
        env.logStartTag(m);
        TreeMap<Short, WaAS_Wave5_HHOLD_Record> r;
        File f = getSubsetCacheFile2(W5, type);
        if (f.exists()) {
            Object[] o = (Object[]) Generic_IO.readObject(f);
            r = (TreeMap<Short, WaAS_Wave5_HHOLD_Record>) o[0];
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
     * @param GORSubsets
     * @param GORLookups
     * @param data
     * @param subset Subset of CASEW1 for all records to be included.
     * @return Map with keys as GOR and Values as map with keys as CASEWX and
     * values as HVALUE.
     */
    public HashMap<Byte, HashMap<Short, Double>> getVariableForGORSubsets(
            String variableName, byte wave,
            HashMap<Byte, HashSet<Short>>[] GORSubsets,
            HashMap<Short, Byte>[] GORLookups, WaAS_Data data,
            HashSet<Short> subset) {
        HashMap<Byte, HashMap<Short, Double>> r = new HashMap<>();
        Iterator<Byte> ite = GORSubsets[wave - 1].keySet().iterator();
        while (ite.hasNext()) {
            r.put(ite.next(), new HashMap<>());
        }
        if (wave == WaAS_Data.W1) {
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr = c.getData().get(CASEW1);
                        WaAS_Wave1_HHOLD_Record w1 = cr.w1Record.getHhold();
                        Byte GOR = GORLookups[wave - 1].get(CASEW1);
                        Generic_Collections.addToMap(r, GOR, CASEW1,
                                w1.getHVALUE());
                    }
                });
                data.clearCollection(cID);
            });
        } else if (wave == WaAS_Data.W2) {
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr = c.getData().get(CASEW1);
                        HashMap<Short, WaAS_Wave2_Record> w2Records;
                        w2Records = cr.w2Records;
                        Iterator<Short> ite2 = w2Records.keySet().iterator();
                        while (ite2.hasNext()) {
                            Short CASEW2 = ite2.next();
                            Byte GOR = GORLookups[wave - 1].get(CASEW2);
                            WaAS_Wave2_HHOLD_Record w2;
                            w2 = w2Records.get(CASEW2).getHhold();
                            Generic_Collections.addToMap(r, GOR, CASEW2,
                                    w2.getHVALUE());
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
                        WaAS_Combined_Record cr = c.getData().get(CASEW1);
                        HashMap<Short, HashMap<Short, WaAS_Wave3_Record>> w3Records;
                        w3Records = cr.w3Records;
                        Iterator<Short> ite1 = w3Records.keySet().iterator();
                        while (ite1.hasNext()) {
                            Short CASEW2 = ite1.next();
                            HashMap<Short, WaAS_Wave3_Record> w3_2;
                            w3_2 = w3Records.get(CASEW2);
                            Iterator<Short> ite2;
                            ite2 = w3_2.keySet().iterator();
                            while (ite2.hasNext()) {
                                Short CASEW3 = ite2.next();
                                Byte GOR = GORLookups[wave - 1].get(CASEW3);
                                WaAS_Wave3_HHOLD_Record w3;
                                w3 = w3_2.get(CASEW3).getHhold();
                                Generic_Collections.addToMap(r, GOR, CASEW3,
                                        w3.getHVALUE());
                            }
                        }
                    }
                });
                data.clearCollection(cID);
            });
        } else if (wave == WaAS_Data.W4) {
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr = c.getData().get(CASEW1);
                        HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave4_Record>>> w4Records;
                        w4Records = cr.w4Records;
                        Iterator<Short> ite1 = w4Records.keySet().iterator();
                        while (ite1.hasNext()) {
                            Short CASEW2 = ite1.next();
                            HashMap<Short, HashMap<Short, WaAS_Wave4_Record>> w4_2;
                            w4_2 = w4Records.get(CASEW2);
                            Iterator<Short> ite2 = w4_2.keySet().iterator();
                            while (ite2.hasNext()) {
                                Short CASEW3 = ite2.next();
                                HashMap<Short, WaAS_Wave4_Record> w4_3;
                                w4_3 = w4_2.get(CASEW3);
                                Iterator<Short> ite3 = w4_3.keySet().iterator();
                                while (ite3.hasNext()) {
                                    Short CASEW4 = ite3.next();
                                    Byte GOR = GORLookups[wave - 1].get(CASEW4);
                                    WaAS_Wave4_HHOLD_Record w4;
                                    w4 = w4_3.get(CASEW4).getHhold();
                                    Generic_Collections.addToMap(r, GOR, CASEW4, w4.getHVALUE());
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
                        WaAS_Combined_Record cr = c.getData().get(CASEW1);
                        HashMap<Short, HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>>> w5Records;
                        w5Records = cr.w5Records;
                        Iterator<Short> ite1 = w5Records.keySet().iterator();
                        while (ite1.hasNext()) {
                            Short CASEW2 = ite1.next();
                            HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>> w5_2;
                            w5_2 = w5Records.get(CASEW2);
                            Iterator<Short> ite2 = w5_2.keySet().iterator();
                            while (ite2.hasNext()) {
                                Short CASEW3 = ite2.next();
                                HashMap<Short, HashMap<Short, WaAS_Wave5_Record>> w5_3;
                                w5_3 = w5_2.get(CASEW3);
                                Iterator<Short> ite3 = w5_3.keySet().iterator();
                                while (ite3.hasNext()) {
                                    Short CASEW4 = ite3.next();
                                    HashMap<Short, WaAS_Wave5_Record> w5_4;
                                    w5_4 = w5_3.get(CASEW4);
                                    Iterator<Short> ite4;
                                    ite4 = w5_4.keySet().iterator();
                                    while (ite4.hasNext()) {
                                        Short CASEW5 = ite4.next();
                                        Byte GOR = GORLookups[wave - 1].get(CASEW5);
                                        WaAS_Wave5_HHOLD_Record w5;
                                        w5 = w5_4.get(CASEW5).getHhold();
                                        Generic_Collections.addToMap(r, GOR, CASEW5, w5.getHVALUE());
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
     * Get the HVALUE.
     *
     * @param vName Variable name.
     * @param gors
     * @param m Expecting ? to be WaAS_Wave1_HHOLD_Record or
     * WaAS_Wave2_HHOLD_Record or WaAS_Wave3_HHOLD_Record or
     * WaAS_Wave4_HHOLD_Record or WaAS_Wave5_HHOLD_Record.
     * @param wave
     * @return Map with keys as GOR and Values as map with keys as CASEWX and
     * values as HVALUE.
     */
    public HashMap<Byte, HashMap<Short, Double>> getVariableForGOR(
            String vName, ArrayList<Byte> gors, TreeMap<Short, ?> m,
            //TreeMap<Short, WaAS_Wave1Or2Or3Or4Or5_HHOLD_Record> m, 
            byte wave) {
        HashMap<Byte, HashMap<Short, Double>> r = new HashMap<>();
        gors.stream().forEach(gor -> {
            r.put(gor, new HashMap<>());
        });
        int countNegative = 0;
        int countZero = 0;
        Iterator<Short> ite = m.keySet().iterator();
        if (vName.equalsIgnoreCase(WaAS_Strings.s_HVALUE)) {
            while (ite.hasNext()) {
                Short CASEWX = ite.next();
                WaAS_Wave1Or2Or3Or4Or5_HHOLD_Record rec;
                rec = (WaAS_Wave1Or2Or3Or4Or5_HHOLD_Record) m.get(CASEWX);
                Byte GOR = rec.getGOR();
                double v = rec.getHVALUE();
                if (v == 0.0d) {
                    countZero++;
                } else if (v < 0.0d) {
                    countNegative++;
                }
                Generic_Collections.addToMap(r, GOR, CASEWX, v);
            }
        } else if (vName.equalsIgnoreCase(WaAS_Strings.s_HPROPW)) {
            while (ite.hasNext()) {
                Short CASEWX = ite.next();
                WaAS_Wave1Or2Or3Or4Or5_HHOLD_Record rec;
                rec = (WaAS_Wave1Or2Or3Or4Or5_HHOLD_Record) m.get(CASEWX);
                Byte GOR = rec.getGOR();
                double v = rec.getHPROPW();
                if (v == 0.0d) {
                    countZero++;
                } else if (v < 0.0d) {
                    countNegative++;
                }
                Generic_Collections.addToMap(r, GOR, CASEWX, v);
            }
        } else {

        }
        env.log(vName + " for GOR W" + wave);
        env.log("count " + m.size());
        env.log("countZero " + countZero);
        env.log("countNegative " + countNegative);
        return r;
    }

    /**
     * Get HVALUE Total Household Property Wealth for each wave in the subsets.
     *
     * @param variableName
     * @param gors
     * @param GORSubsets
     * @param GORLookups
     * @param GORNameLookup
     * @param data
     * @param subset
     * @return
     */
    public TreeMap<Byte, Double> getChangeVariableSubset(String variableName,
            ArrayList<Byte> gors, HashMap<Byte, HashSet<Short>>[] GORSubsets,
            HashMap<Short, Byte>[] GORLookups,
            TreeMap<Byte, String> GORNameLookup, WaAS_Data data,
            HashSet<Short> subset) {
        TreeMap<Byte, Double> r = new TreeMap<>();
        HashMap<Byte, HashMap<Short, Double>>[] variableSubsets;
        variableSubsets = new HashMap[WaAS_Data.NWAVES];
        for (byte w = 0; w < WaAS_Data.NWAVES; w++) {
            variableSubsets[w] = getVariableForGORSubsets(variableName,
                    (byte) (w + 1), GORSubsets, GORLookups, data, subset);
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
    public TreeMap<Byte, Double> getChangeVariableAll(String vName,
            ArrayList<Byte> gors, TreeMap<Byte, String> GORNameLookup) {
        TreeMap<Byte, Double> r = new TreeMap<>();
        HashMap<Byte, HashMap<Short, Double>>[] vAll;
        vAll = new HashMap[WaAS_Data.NWAVES];
        TreeMap<Short, WaAS_Wave1_HHOLD_Record> allW1 = loadAllW1();
        vAll[0] = getVariableForGOR(vName, gors, allW1, (byte) 1);
        allW1 = null; // Set to null to free memory.
        TreeMap<Short, WaAS_Wave5_HHOLD_Record> allW5 = loadAllW5();
        vAll[4] = getVariableForGOR(vName, gors, allW5, (byte) 5);
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
                if (w == 0 || w == 4) {
                    v[w] = Generic_Statistics.getSummaryStatistics(
                            vAll[w].get(gor).values());
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
