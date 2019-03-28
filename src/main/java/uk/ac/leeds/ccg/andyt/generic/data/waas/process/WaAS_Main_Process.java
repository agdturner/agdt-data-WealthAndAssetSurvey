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
package uk.ac.leeds.ccg.andyt.generic.data.waas.process;

import java.io.BufferedReader;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.TreeSet;
import uk.ac.leeds.ccg.andyt.generic.core.Generic_Environment;
import uk.ac.leeds.ccg.andyt.generic.io.Generic_IO;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.andyt.generic.data.waas.io.WaAS_Files;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Object;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Strings;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Collection;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_CollectionSimple;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Combined_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Combined_Record_Simple;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Data;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_HHOLD_Handler;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_PERSON_Handler;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Wave2_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Wave3_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Wave4_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Wave5_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave1_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave2_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave3_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave4_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_Wave5_HHOLD_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave1_PERSON_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave2_PERSON_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave3_PERSON_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave4_PERSON_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave5_PERSON_Record;

/**
 *
 * @author geoagdt
 */
public class WaAS_Main_Process extends WaAS_Object {

    // For convenience
    protected final WaAS_Data data;
    protected transient final byte NWAVES;
    protected transient final byte W1;
    protected transient final byte W2;
    protected transient final byte W3;
    protected transient final byte W4;
    protected transient final byte W5;
    protected final WaAS_Files files;

    public WaAS_Main_Process(WaAS_Environment env) {
        super(env);
        data = env.data;
        NWAVES = WaAS_Data.NWAVES;
        W1 = WaAS_Data.W1;
        W2 = WaAS_Data.W2;
        W3 = WaAS_Data.W3;
        W4 = WaAS_Data.W4;
        W5 = WaAS_Data.W5;
        files = env.files;
    }

    public static void main(String[] args) {
        WaAS_Environment env = new WaAS_Environment(new Generic_Environment());
        WaAS_Main_Process p = new WaAS_Main_Process(env);
        // Main switches
        //p.doJavaCodeGeneration = true;
        p.doLoadAllHouseholdsRecords = true;
        p.doLoadHouseholdsAndIndividualsInAllWaves = true;
        p.doLoadHouseholdsInPairedWaves = true;
        p.run();
    }

    public void run() {
        if (doJavaCodeGeneration) {
            runJavaCodeGeneration();
        }
        WaAS_HHOLD_Handler hH = new WaAS_HHOLD_Handler(env);
        int chunkSize = 256; //1024; 512; 256;
        if (doLoadAllHouseholdsRecords) {
            loadAllHouseholdRecords(hH);
        }
        if (doLoadHouseholdsAndIndividualsInAllWaves) {
            loadHouseholdsInAllWaves(hH);
            mergePersonAndHouseholdDataIntoCollections(
                    WaAS_Strings.s_InW1W2W3W4W5, hH, chunkSize);
        }
        if (doLoadHouseholdsInPairedWaves) {
            if (true) {
                Object[] W2InW1 = hH.loadHouseholdsInPreviousWave(W2);
                TreeMap<Short, WaAS_Wave2_HHOLD_Record> W2InW1Recs;
                W2InW1Recs = (TreeMap<Short, WaAS_Wave2_HHOLD_Record>) W2InW1[0];
                TreeSet<Short>[] W2InW1Sets = (TreeSet<Short>[]) W2InW1[1];
                TreeSet<Short> W2InW1CASEW1 = W2InW1Sets[0];
                Object[] W1InW2 = hH.loadW1(W2InW1CASEW1, WaAS_Strings.s_InW2);
                TreeMap<Short, WaAS_Wave1_HHOLD_Record> W1InW2Recs;
                W1InW2Recs = (TreeMap<Short, WaAS_Wave1_HHOLD_Record>) W1InW2[0];
                env.log(W2InW1Recs.size() + "\t W2InW1Recs.size()");
                env.log(W1InW2Recs.size() + "\t W1InW2Recs.size()");

            }
            if (true) {
                Object[] W3InW2 = hH.loadHouseholdsInPreviousWave(W3);
                TreeMap<Short, WaAS_Wave3_HHOLD_Record> W3InW2Recs;
                W3InW2Recs = (TreeMap<Short, WaAS_Wave3_HHOLD_Record>) W3InW2[0];
                TreeSet<Short>[] W3InW2Sets = (TreeSet<Short>[]) W3InW2[1];
                TreeSet<Short> W3InW2CASEW2 = W3InW2Sets[1];
                Object[] W2InW3 = hH.loadW2(W3InW2CASEW2, WaAS_Strings.s_InW3);
                TreeMap<Short, WaAS_Wave1_HHOLD_Record> W2InW3Recs;
                W2InW3Recs = (TreeMap<Short, WaAS_Wave1_HHOLD_Record>) W2InW3[0];
                env.log(W3InW2Recs.size() + "\t W3InW2Recs.size()");
                env.log(W2InW3Recs.size() + "\t W2InW3Recs.size()");
            }
            if (true) {
                Object[] W4InW3 = hH.loadHouseholdsInPreviousWave(W4);
                TreeMap<Short, WaAS_Wave4_HHOLD_Record> W4InW3Recs;
                W4InW3Recs = (TreeMap<Short, WaAS_Wave4_HHOLD_Record>) W4InW3[0];
                TreeSet<Short>[] W4InW3Sets = (TreeSet<Short>[]) W4InW3[1];
                TreeSet<Short> W4InW3CASEW3 = W4InW3Sets[2];
                Object[] W3InW4 = hH.loadW3(W4InW3CASEW3, WaAS_Strings.s_InW4);
                TreeMap<Short, WaAS_Wave1_HHOLD_Record> W3InW4Recs;
                W3InW4Recs = (TreeMap<Short, WaAS_Wave1_HHOLD_Record>) W3InW4[0];
                env.log(W4InW3Recs.size() + "\t W4InW3Recs.size()");
                env.log(W3InW4Recs.size() + "\t W3InW4Recs.size()");
            }
            if (true) {
                Object[] W5InW4 = hH.loadHouseholdsInPreviousWave(W5);
                TreeMap<Short, WaAS_Wave5_HHOLD_Record> W5InW4Recs;
                W5InW4Recs = (TreeMap<Short, WaAS_Wave5_HHOLD_Record>) W5InW4[0];
                TreeSet<Short>[] W5InW4Sets = (TreeSet<Short>[]) W5InW4[1];
                TreeSet<Short> W5InW4CASEW4 = W5InW4Sets[3];
                Object[] W4InW5 = hH.loadW4(W5InW4CASEW4, WaAS_Strings.s_InW5);
                TreeMap<Short, WaAS_Wave1_HHOLD_Record> W4InW5Recs;
                W4InW5Recs = (TreeMap<Short, WaAS_Wave1_HHOLD_Record>) W4InW5[0];
                env.log(W5InW4Recs.size() + "\t W5InW4Recs.size()");
                env.log(W4InW5Recs.size() + "\t W4InW5Recs.size()");
            }
            mergePersonAndHouseholdDataIntoCollections(
                    //indir, outdir,
                    WaAS_Strings.s_Paired, hH, chunkSize);
        }
        HashSet<Short> subset = hH.getStableHouseholdCompositionSubset(data);
        initDataSimple(subset);
        env.cacheData();
    }

    /**
     * Read input data and create household all sets.
     *
     * @param hH
     * @return
     */
    public Object[] loadAllHouseholdRecords(WaAS_HHOLD_Handler hH) {
        String m0 = "loadAllHouseholdRecords";
        env.logStartTag(m0);
        Object[] r = new Object[NWAVES];
        TreeMap<Short, WaAS_Wave1_HHOLD_Record> allW1 = hH.loadAllW1();
        TreeMap<Short, WaAS_Wave2_HHOLD_Record> allW2 = hH.loadAllW2();
        TreeMap<Short, WaAS_Wave3_HHOLD_Record> allW3 = hH.loadAllW3();
        TreeMap<Short, WaAS_Wave4_HHOLD_Record> allW4 = hH.loadAllW4();
        TreeMap<Short, WaAS_Wave5_HHOLD_Record> allW5 = hH.loadAllW5();
        r[0] = allW1;
        r[1] = allW2;
        r[2] = allW3;
        r[3] = allW4;
        r[4] = allW5;
        env.logEndTag(m0);
        return r;
    }

    /**
     * Merge Person and Household Data into collections.
     *
     * @param type
     * @param hH
     * @param chunkSize
     */
    public void mergePersonAndHouseholdDataIntoCollections(String type,
            WaAS_HHOLD_Handler hH, int chunkSize) {
        String m = "mergePersonAndHouseholdDataIntoCollections";
        env.logStartTag(m);
        WaAS_PERSON_Handler pH = new WaAS_PERSON_Handler(env);
        /**
         * Wave 1
         */
        Object[] o = mergePersonAndHouseholdDataIntoCollectionsW1(data, type,
                pH, hH, chunkSize);
        int nOC = (Integer) o[0];
        TreeMap<Short, HashSet<Short>> CIDToCASEW1;
        CIDToCASEW1 = (TreeMap<Short, HashSet<Short>>) o[1];
        HashMap<Short, Short> CASEW1ToCID = (HashMap<Short, Short>) o[2];
        /**
         * Wave 2
         */
        TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2 = hH.loadSubsetLookupTo(W1);
        TreeMap<Short, Short> CASEW2ToCASEW1 = hH.loadSubsetLookupFrom(W1);
        mergePersonAndHouseholdDataIntoCollectionsW2(data, type, pH, hH, nOC,
                CASEW1ToCID, CIDToCASEW1, CASEW1ToCASEW2, CASEW2ToCASEW1);
        /**
         * Wave 3
         */
        TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3 = hH.loadSubsetLookupTo(W2);
        TreeMap<Short, Short> CASEW3ToCASEW2 = hH.loadSubsetLookupFrom(W2);
        mergePersonAndHouseholdDataIntoCollectionsW3(data, type, pH, hH, nOC,
                CASEW1ToCID, CIDToCASEW1, CASEW1ToCASEW2, CASEW2ToCASEW1,
                CASEW2ToCASEW3, CASEW3ToCASEW2);
        /**
         * Wave 4
         */
        TreeMap<Short, HashSet<Short>> CASEW3ToCASEW4 = hH.loadSubsetLookupTo(W3);
        TreeMap<Short, Short> CASEW4ToCASEW3 = hH.loadSubsetLookupFrom(W3);
        mergePersonAndHouseholdDataIntoCollectionsW4(data, type, pH, hH, nOC,
                CASEW1ToCID, CIDToCASEW1, CASEW1ToCASEW2, CASEW2ToCASEW1,
                CASEW2ToCASEW3, CASEW3ToCASEW2, CASEW3ToCASEW4, CASEW4ToCASEW3);
        /**
         * Wave 5
         */
        TreeMap<Short, HashSet<Short>> CASEW4ToCASEW5 = hH.loadSubsetLookupTo(W4);
        TreeMap<Short, Short> CASEW5ToCASEW4 = hH.loadSubsetLookupFrom(W4);
        mergePersonAndHouseholdDataIntoCollectionsW5(data, type, pH, hH, nOC,
                CASEW1ToCID, CIDToCASEW1, CASEW1ToCASEW2, CASEW2ToCASEW1,
                CASEW2ToCASEW3, CASEW3ToCASEW2, CASEW3ToCASEW4, CASEW4ToCASEW3,
                CASEW4ToCASEW5, CASEW5ToCASEW4);
        env.log("data.lookup.size() " + data.CASEW1ToCID.size());
        env.log("data.data.size() " + data.data.size());
        env.cacheData();
        env.logEndTag(m);
    }

    /**
     * Merge Person and Household Data for Wave 1.
     *
     * @param data
     * @param type
     * @param pH personHandler
     * @param hH hholdHandler
     * @param chunkSize
     * @return
     */
    public Object[] mergePersonAndHouseholdDataIntoCollectionsW1(WaAS_Data data,
            String type, WaAS_PERSON_Handler pH, WaAS_HHOLD_Handler hH,
            int chunkSize) {
        // Wave 1
        String m0 = "mergePersonAndHouseholdDataIntoCollectionsW1";
        env.logStartTag(m0);
        Object[] r = new Object[3];
        TreeMap<Short, WaAS_Wave1_HHOLD_Record> hs;
        if (type.equalsIgnoreCase(WaAS_Strings.s_InW1W2W3W4W5)) {
            hs = hH.loadCachedSubsetW1(type);
        } else if (type.equalsIgnoreCase(WaAS_Strings.s_Paired)) {
            hs = hH.loadCachedSubset2W1(WaAS_Strings.s_InW2);
        } else {
            env.log("Unrecognised type " + type);
            hs = null;
        }
        TreeSet<Short> CASEW1IDs = new TreeSet<>();
        CASEW1IDs.addAll(hs.keySet());
        int nOC;
        nOC = (int) Math.ceil((double) CASEW1IDs.size() / (double) chunkSize);
        r[0] = nOC;
        Object[] ps = pH.loadSubsetWave1(CASEW1IDs, nOC, W1);
        TreeMap<Short, HashSet<Short>> CIDToCASEW1;
        CIDToCASEW1 = (TreeMap<Short, HashSet<Short>>) ps[0];
        TreeMap<Short, File> cFs = (TreeMap<Short, File>) ps[2];
        r[1] = ps[0];
        r[2] = ps[1];
        CIDToCASEW1.keySet().stream().forEach(cID -> {
            String m1 = "Collection ID " + cID;
            env.logStartTag(m1);
            WaAS_Collection c = new WaAS_Collection(cID);
            data.data.put(cID, c);
            // Add hhold records.
            String m2 = "Add hhold records";
            env.logStartTag(m2);
            HashSet<Short> s = CIDToCASEW1.get(cID);
            s.stream().forEach(CASEW1 -> {
                data.CASEW1ToCID.put(CASEW1, cID);
                HashMap<Short, WaAS_Combined_Record> m = c.getData();
                WaAS_Combined_Record cr = m.get(CASEW1);
                if (cr == null) {
                    cr = new WaAS_Combined_Record(CASEW1);
                    m.put(CASEW1, cr);
                }
                cr.w1Record.setHhold(hs.get(CASEW1));
            });
            env.logEndTag(m2);
            // Add person records.
            m2 = "Add person records";
            env.logStartTag(m2);
            File f = cFs.get(cID);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(line -> {
                WaAS_Wave1_PERSON_Record p = new WaAS_Wave1_PERSON_Record(line);
                short CASEW1 = p.getCASEW1();
                HashMap<Short, WaAS_Combined_Record> m = c.getData();
                WaAS_Combined_Record cr = m.get(CASEW1);
                cr.w1Record.getPeople().add(p);
            });
            env.logEndTag(m2);
            // Close br
            Generic_IO.closeBufferedReader(br);
            // Cache and clear collection
            data.cacheSubsetCollection(cID, c);
            data.clearCollection(cID);
            env.logEndTag(m1);
        });
        env.logEndTag(m0);
        return r;
    }

    /**
     * Merge Person and Household Data for Wave 2.
     *
     * @param data
     * @param type
     * @param pH
     * @param hH
     * @param nOC
     * @param CASEW1ToCID
     * @param CIDToCASEW1
     * @param CASEW1ToCASEW2
     * @param CASEW2ToCASEW1
     */
    public void mergePersonAndHouseholdDataIntoCollectionsW2(WaAS_Data data,
            String type, WaAS_PERSON_Handler pH, WaAS_HHOLD_Handler hH, int nOC,
            HashMap<Short, Short> CASEW1ToCID,
            TreeMap<Short, HashSet<Short>> CIDToCASEW1,
            TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2,
            TreeMap<Short, Short> CASEW2ToCASEW1) {
        // Wave 2
        String m0 = "mergePersonAndHouseholdDataIntoCollectionsW2";
        env.logStartTag(m0);
        TreeMap<Short, WaAS_Wave2_HHOLD_Record> hs;
        if (type.equalsIgnoreCase(WaAS_Strings.s_InW1W2W3W4W5)) {
            hs = hH.loadCachedSubsetW2(type);
        } else if (type.equalsIgnoreCase(WaAS_Strings.s_Paired)) {
            hs = hH.loadCachedSubset2W2(WaAS_Strings.s_InW3);
        } else {
            env.log("Unrecognised type " + type);
            hs = null;
        }
        TreeMap<Short, File> cFs = pH.loadSubsetWave2(nOC, CASEW1ToCID, W2,
                CASEW2ToCASEW1);
        cFs.keySet().stream().forEach(cID -> {
            String m1 = "Collection ID " + cID;
            env.logStartTag(m1);
            WaAS_Collection c = data.getCollection(cID);
            // Add hhold records.
            String m2 = "Add hhold records";
            env.logStartTag(m2);
            HashSet<Short> s = CIDToCASEW1.get(cID);
            s.stream().forEach(CASEW1 -> {
                data.CASEW1ToCID.put(CASEW1, cID);
                HashMap<Short, WaAS_Combined_Record> m = c.getData();
                WaAS_Combined_Record cr = m.get(CASEW1);
                if (cr == null) {
                    env.log("No combined record for CASEW1 " + CASEW1 + "! "
                            + "This may be a data error?");
                } else {
                    HashSet<Short> CASEW2s;
                    CASEW2s = CASEW1ToCASEW2.get(CASEW1);
                    CASEW2s.stream().forEach(CASEW2 -> {
                        WaAS_Wave2_Record w2rec = new WaAS_Wave2_Record(CASEW2);
                        w2rec.setHhold(hs.get(CASEW2));
                        cr.w2Records.put(CASEW2, w2rec);
                    });
                }
            });
            env.logEndTag(m2);
            // Add person records.
            m2 = "Add person records";
            env.logStartTag(m2);
            File f = cFs.get(cID);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(line -> {
                WaAS_Wave2_PERSON_Record p = new WaAS_Wave2_PERSON_Record(line);
                short CASEW1Check = p.getCASEW1();
                short CASEW2 = p.getCASEW2();
                short CASEW1 = CASEW2ToCASEW1.get(CASEW2);
                printCheck(W2, CASEW1Check, CASEW1, CASEW1ToCASEW2);
                HashMap<Short, WaAS_Combined_Record> m = c.getData();
                WaAS_Combined_Record cr = m.get(CASEW1);
                if (cr == null) {
                    env.log("No combined record for CASEW1 " + CASEW1 + "! "
                            + "This may be a data error, or this person may "
                            + "have moved from one hhold to another?");
                } else {
                    HashSet<Short> CASEW2s = CASEW1ToCASEW2.get(CASEW1);
                    CASEW2s.stream().forEach(k2 -> {
                        WaAS_Wave2_Record w2rec = cr.w2Records.get(k2);
                        w2rec.getPeople().add(p);
                    });
                }
            });
            env.logEndTag(m2);
            // Close br
            Generic_IO.closeBufferedReader(br);
            // Cache and clear collection
            data.cacheSubsetCollection(cID, c);
            data.clearCollection(cID);
            env.logEndTag(m1);
        });
        env.logEndTag(m0);
    }

    protected void printCheck(byte wave, short CASEWXCheck, short CASEWX,
            TreeMap<Short, HashSet<Short>> lookup) {
        if (CASEWXCheck != CASEWX) {
            env.log("Person in Wave " + wave + " record given by CASEW" + wave
                    + " " + CASEWX + " has a CASEW" + (wave - 1) + " as "
                    + CASEWXCheck + ", but in the CASEW" + wave + "ToCASEW"
                    + (wave - 1) + " lookup this is " + CASEWX + " - this may "
                    + "mean the person is new to the household/data.");
            if (lookup.get(CASEWXCheck) == null) {
                env.log("CASEW" + (wave - 1) + "ToCASEW" + wave + ".get(CASEW"
                        + (wave - 1) + "Check) == null");
            } else {
                env.log("CASEW" + (wave - 1) + "ToCASEW" + wave + ".get(CASEW"
                        + (wave - 1) + "Check).size() "
                        + lookup.get(CASEWXCheck).size());
            }
        }
    }

    /**
     * Merge Person and Household Data for Wave 3.
     *
     * @param data
     * @param type
     * @param pH personHandler
     * @param hH hholdHandler
     * @param nOC
     * @param CASEW1ToCID
     * @param CIDToCASEW1
     * @param CASEW1ToCASEW2
     * @param CASEW2ToCASEW1
     * @param CASEW2ToCASEW3
     * @param CASEW3ToCASEW2
     */
    public void mergePersonAndHouseholdDataIntoCollectionsW3(WaAS_Data data,
            String type, WaAS_PERSON_Handler pH, WaAS_HHOLD_Handler hH, int nOC,
            HashMap<Short, Short> CASEW1ToCID,
            TreeMap<Short, HashSet<Short>> CIDToCASEW1,
            TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2,
            TreeMap<Short, Short> CASEW2ToCASEW1,
            TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3,
            TreeMap<Short, Short> CASEW3ToCASEW2) {
        String m0 = "mergePersonAndHouseholdDataIntoCollectionsW3";
        env.logStartTag(m0);
        TreeMap<Short, WaAS_Wave3_HHOLD_Record> hs;
        if (type.equalsIgnoreCase(WaAS_Strings.s_InW1W2W3W4W5)) {
            hs = hH.loadCachedSubsetW3(type);
        } else if (type.equalsIgnoreCase(WaAS_Strings.s_Paired)) {
            hs = hH.loadCachedSubset2W3(WaAS_Strings.s_InW4);
        } else {
            env.log("Unrecognised type " + type);
            hs = null;
        }
        TreeMap<Short, File> cFs = pH.loadSubsetWave3(nOC, CASEW1ToCID,
                W3, CASEW2ToCASEW1, CASEW3ToCASEW2);
        cFs.keySet().stream().forEach(cID -> {
            String m1 = "Collection ID " + cID;
            env.logStartTag(m1);
            WaAS_Collection c = data.getCollection(cID);
            // Add hhold records.
            String m2 = "Add hhold records";
            env.logStartTag(m2);
            HashSet<Short> s = CIDToCASEW1.get(cID);
            s.stream().forEach(CASEW1 -> {
                data.CASEW1ToCID.put(CASEW1, cID);
                HashMap<Short, WaAS_Combined_Record> m = c.getData();
                WaAS_Combined_Record cr = m.get(CASEW1);
                if (cr == null) {
                    env.log("No combined record for CASEW1 " + CASEW1 + "! "
                            + "This may be a data error?");
                } else {
                    HashSet<Short> CASEW2s = CASEW1ToCASEW2.get(CASEW1);
                    CASEW2s.stream().forEach(CASEW2 -> {
                        HashMap<Short, WaAS_Wave3_Record> w3_2 = new HashMap<>();
                        cr.w3Records.put(CASEW2, w3_2);
                        HashSet<Short> CASEW3s = CASEW2ToCASEW3.get(CASEW2);
                        CASEW3s.stream().forEach(CASEW3 -> {
                            WaAS_Wave3_Record w3rec = new WaAS_Wave3_Record(CASEW3);
                            w3rec.setHhold(hs.get(CASEW3));
                            w3_2.put(CASEW3, w3rec);
                        });
                    });
                }
            });
            env.logEndTag(m2);
            // Add person records.
            m2 = "Add person records";
            env.logStartTag(m2);
            File f = cFs.get(cID);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(line -> {
                WaAS_Wave3_PERSON_Record p = new WaAS_Wave3_PERSON_Record(line);
                short CASEW1Check = p.getCASEW1();
                short CASEW2Check = p.getCASEW2();
                short CASEW3 = p.getCASEW3();
                short CASEW2 = CASEW3ToCASEW2.get(CASEW3);
                short CASEW1 = CASEW2ToCASEW1.get(CASEW2);
                //printCheck(W2, CASEW1Check, CASEW1, CASEW1ToCASEW2);
                printCheck(W3, CASEW2Check, CASEW2, CASEW2ToCASEW3);
                HashMap<Short, WaAS_Combined_Record> m = c.getData();
                WaAS_Combined_Record cr = m.get(CASEW1);
                if (cr == null) {
                    env.log("No combined record for CASEW1 " + CASEW1 + "! "
                            + "This may be a data error, or this person may "
                            + "have moved from one hhold to another?");
                } else {
                    HashSet<Short> CASEW2s = CASEW1ToCASEW2.get(CASEW1);
                    CASEW2s.stream().forEach(k2 -> {
                        HashSet<Short> CASEW3s = CASEW2ToCASEW3.get(CASEW2);
                        CASEW3s.stream().forEach(k3 -> {
                            WaAS_Wave3_Record w3rec;
                            w3rec = cr.w3Records.get(k2).get(k3);
                            if (w3rec == null) {
                                w3rec = new WaAS_Wave3_Record(k3);
                                env.log("Adding people, but there is no hhold "
                                        + "record for CASEW3 " + CASEW3 + "!");
                            }
                            w3rec.getPeople().add(p);
                        });
                    });
                }
            });
            env.logEndTag(m2);
            // Close br
            Generic_IO.closeBufferedReader(br);
            // Cache and clear collection
            data.cacheSubsetCollection(cID, c);
            data.clearCollection(cID);
            env.logEndTag(m1);
        });
        env.logEndTag(m0);
    }

    /**
     * Merge Person and Household Data for Wave 4.
     *
     * @param data
     * @param type
     * @param pH personHandler
     * @param hH hholdHandler
     * @param nOC
     * @param CASEW1ToCID
     * @param CIDToCASEW1
     * @param CASEW1ToCASEW2
     * @param CASEW2ToCASEW1
     * @param CASEW2ToCASEW3
     * @param CASEW3ToCASEW2
     * @param CASEW3ToCASEW4
     * @param CASEW4ToCASEW3
     */
    public void mergePersonAndHouseholdDataIntoCollectionsW4(WaAS_Data data,
            String type, WaAS_PERSON_Handler pH, WaAS_HHOLD_Handler hH, int nOC,
            HashMap<Short, Short> CASEW1ToCID,
            TreeMap<Short, HashSet<Short>> CIDToCASEW1,
            TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2,
            TreeMap<Short, Short> CASEW2ToCASEW1,
            TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3,
            TreeMap<Short, Short> CASEW3ToCASEW2,
            TreeMap<Short, HashSet<Short>> CASEW3ToCASEW4,
            TreeMap<Short, Short> CASEW4ToCASEW3) {
        String m0 = "mergePersonAndHouseholdDataIntoCollectionsW4";
        env.logStartTag(m0);
        TreeMap<Short, WaAS_Wave4_HHOLD_Record> hs;
        if (type.equalsIgnoreCase(WaAS_Strings.s_InW1W2W3W4W5)) {
            hs = hH.loadCachedSubsetW4(type);
        } else if (type.equalsIgnoreCase(WaAS_Strings.s_Paired)) {
            hs = hH.loadCachedSubset2W4(WaAS_Strings.s_InW5);
        } else {
            env.log("Unrecognised type " + type);
            hs = null;
        }
        TreeMap<Short, File> cFs = pH.loadSubsetWave4(nOC, CASEW1ToCID, W4,
                CASEW2ToCASEW1, CASEW3ToCASEW2, CASEW4ToCASEW3);
        cFs.keySet().stream().forEach(cID -> {
            String m1 = "Collection ID " + cID;
            env.logStartTag(m1);
            WaAS_Collection c = data.getCollection(cID);
            // Add hhold records.
            String m2 = "Add hhold records";
            env.logStartTag(m2);
            HashSet<Short> s = CIDToCASEW1.get(cID);
            s.stream().forEach(CASEW1 -> {
                data.CASEW1ToCID.put(CASEW1, cID);
                HashMap<Short, WaAS_Combined_Record> m = c.getData();
                WaAS_Combined_Record cr = m.get(CASEW1);
                if (cr == null) {
                    env.log("No combined record for CASEW1 " + CASEW1 + "! "
                            + "This may be a data error?");
                } else {
                    HashSet<Short> CASEW2s = CASEW1ToCASEW2.get(CASEW1);
                    CASEW2s.stream().forEach(CASEW2 -> {
                        HashMap<Short, HashMap<Short, WaAS_Wave4_Record>> w4_2;
                        w4_2 = new HashMap<>();
                        cr.w4Records.put(CASEW2, w4_2);
                        HashSet<Short> CASEW3s = CASEW2ToCASEW3.get(CASEW2);
                        CASEW3s.stream().forEach(CASEW3 -> {
                            HashMap<Short, WaAS_Wave4_Record> w4_3;
                            w4_3 = new HashMap<>();
                            w4_2.put(CASEW3, w4_3);
                            HashSet<Short> CASEW4s = CASEW3ToCASEW4.get(CASEW3);
                            CASEW4s.stream().forEach(CASEW4 -> {
                                WaAS_Wave4_Record w4rec;
                                w4rec = new WaAS_Wave4_Record(CASEW4);
                                w4rec.setHhold(hs.get(CASEW4));
                                w4_3.put(CASEW4, w4rec);
                            });
                        });
                    });
                }
            });
            env.logEndTag(m2);
            // Add person records.
            m2 = "Add person records";
            env.logStartTag(m2);
            File f = cFs.get(cID);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(line -> {
                WaAS_Wave4_PERSON_Record p = new WaAS_Wave4_PERSON_Record(line);
                short CASEW1Check = p.getCASEW1();
                short CASEW2Check = p.getCASEW2();
                short CASEW3Check = p.getCASEW3();
                short CASEW4 = p.getCASEW4();
                short CASEW3 = CASEW4ToCASEW3.get(CASEW4);
                short CASEW2 = CASEW3ToCASEW2.get(CASEW3);
                short CASEW1 = CASEW2ToCASEW1.get(CASEW2);
                //printCheck(W2, CASEW1Check, CASEW1, CASEW1ToCASEW2);
                //printCheck(W3, CASEW2Check, CASEW2, CASEW2ToCASEW3);
                printCheck(W4, CASEW3Check, CASEW3, CASEW3ToCASEW4);
                HashMap<Short, WaAS_Combined_Record> m = c.getData();
                WaAS_Combined_Record cr = m.get(CASEW1);
                if (cr == null) {
                    env.log("No combined record for CASEW1 " + CASEW1 + "! "
                            + "This may be a data error, or this person may "
                            + "have moved from one hhold to another?");
                } else {
                    HashSet<Short> CASEW2s = CASEW1ToCASEW2.get(CASEW1);
                    CASEW2s.stream().forEach(k2 -> {
                        HashSet<Short> CASEW3s = CASEW2ToCASEW3.get(CASEW2);
                        CASEW3s.stream().forEach(k3 -> {
                            HashSet<Short> CASEW4s = CASEW3ToCASEW4.get(CASEW3);
                            CASEW4s.stream().forEach(k4 -> {
                                HashMap<Short, HashMap<Short, WaAS_Wave4_Record>> w4_2;
                                w4_2 = cr.w4Records.get(k2);
                                if (w4_2 == null) {
                                    w4_2 = new HashMap<>();
                                    cr.w4Records.put(k2, w4_2);
                                }
                                HashMap<Short, WaAS_Wave4_Record> w4_3;
                                w4_3 = w4_2.get(k3);
                                if (w4_3 == null) {
                                    w4_3 = new HashMap<>();
                                    w4_2.put(k3, w4_3);
                                }
                                WaAS_Wave4_Record w4rec;
                                w4rec = w4_3.get(k4);
                                if (w4rec == null) {
                                    w4rec = new WaAS_Wave4_Record(k4);
                                    env.log("Adding people, but there is no "
                                            + "hhold record for CASEW4 "
                                            + CASEW4 + "!");
                                }
                                w4rec.getPeople().add(p);
                            });
                        });
                    });
                }
            });
            env.logEndTag(m2);
            // Close br
            Generic_IO.closeBufferedReader(br);
            // Cache and clear collection
            data.cacheSubsetCollection(cID, c);
            data.clearCollection(cID);
            env.logEndTag(m1);
        });
        env.logEndTag(m0);
    }

    /**
     * Merge Person and Household Data for Wave 5.
     *
     * @param data
     * @param type
     * @param pH
     * @param hH
     * @param nOC
     * @param CASEW1ToCID
     * @param CIDToCASEW1
     * @param CASEW1ToCASEW2
     * @param CASEW2ToCASEW1
     * @param CASEW2ToCASEW3
     * @param CASEW3ToCASEW2
     * @param CASEW3ToCASEW4
     * @param CASEW4ToCASEW3
     * @param CASEW4ToCASEW5
     * @param CASEW5ToCASEW4
     */
    public void mergePersonAndHouseholdDataIntoCollectionsW5(WaAS_Data data,
            String type, WaAS_PERSON_Handler pH, WaAS_HHOLD_Handler hH, int nOC,
            HashMap<Short, Short> CASEW1ToCID,
            TreeMap<Short, HashSet<Short>> CIDToCASEW1,
            TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2,
            TreeMap<Short, Short> CASEW2ToCASEW1,
            TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3,
            TreeMap<Short, Short> CASEW3ToCASEW2,
            TreeMap<Short, HashSet<Short>> CASEW3ToCASEW4,
            TreeMap<Short, Short> CASEW4ToCASEW3,
            TreeMap<Short, HashSet<Short>> CASEW4ToCASEW5,
            TreeMap<Short, Short> CASEW5ToCASEW4) {
        // Wave 5
        String m0 = "mergePersonAndHouseholdDataIntoCollectionsW5";
        env.logStartTag(m0);
        TreeMap<Short, WaAS_Wave5_HHOLD_Record> hs;
        if (type.equalsIgnoreCase(WaAS_Strings.s_InW1W2W3W4W5)) {
            hs = hH.loadCachedSubsetW5(type);
        } else if (type.equalsIgnoreCase(WaAS_Strings.s_Paired)) {
            hs = hH.loadCachedSubset2W5(WaAS_Strings.s_InW4); // It may seem eems wierd to be W4 not W5, but probably right!?
            if (hs == null) {
                env.log("WTF1");
            }
        } else {
            env.log("Unrecognised type " + type);
            hs = null;
        }
        if (hs == null) {
            env.log("WTF2");
        }
        TreeMap<Short, File> cFs;
        cFs = pH.loadSubsetWave5(nOC, CASEW1ToCID, W5, CASEW2ToCASEW1,
                CASEW3ToCASEW2, CASEW4ToCASEW3, CASEW5ToCASEW4);
        cFs.keySet().stream().forEach(cID -> {
            String m1 = "Collection ID " + cID;
            env.logStartTag(m1);
            WaAS_Collection c = data.getCollection(cID);
            // Add hhold records.
            String m2 = "Add hhold records";
            env.logStartTag(m2);
            HashSet<Short> s = CIDToCASEW1.get(cID);
            s.stream().forEach(CASEW1 -> {
                data.CASEW1ToCID.put(CASEW1, cID);
                HashMap<Short, WaAS_Combined_Record> m = c.getData();
                WaAS_Combined_Record cr = m.get(CASEW1);
                if (cr == null) {
                    env.log("No combined record for CASEW1 " + CASEW1 + "! "
                            + "This may be a data error?");
                } else {
                    HashSet<Short> CASEW2s = CASEW1ToCASEW2.get(CASEW1);
                    CASEW2s.stream().forEach(CASEW2 -> {
                        HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>> w5_2;
                        w5_2 = new HashMap<>();
                        cr.w5Records.put(CASEW2, w5_2);
                        HashSet<Short> CASEW3s = CASEW2ToCASEW3.get(CASEW2);
                        CASEW3s.stream().forEach(CASEW3 -> {
                            HashMap<Short, HashMap<Short, WaAS_Wave5_Record>> w5_3;
                            w5_3 = new HashMap<>();
                            w5_2.put(CASEW3, w5_3);
                            HashSet<Short> CASEW4s = CASEW3ToCASEW4.get(CASEW3);
                            CASEW4s.stream().forEach(CASEW4 -> {
                                HashMap<Short, WaAS_Wave5_Record> w5_4;
                                w5_4 = new HashMap<>();
                                w5_3.put(CASEW4, w5_4);
                                HashSet<Short> CASEW5s;
                                CASEW5s = CASEW4ToCASEW5.get(CASEW4);
                                CASEW5s.stream().forEach(CASEW5 -> {
                                    if (CASEW5 != null) {
                                        WaAS_Wave5_Record w5rec;
                                        w5rec = new WaAS_Wave5_Record(CASEW5);
                                        w5rec.setHhold(hs.get(CASEW5));
                                        w5_4.put(CASEW5, w5rec);
                                    }
                                });
                            });
                        });
                    });
                }
            });
            env.logEndTag(m2);
            // Add person records.
            m2 = "Add person records";
            env.logStartTag(m2);
            File f = cFs.get(cID);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach(line -> {
                WaAS_Wave5_PERSON_Record p = new WaAS_Wave5_PERSON_Record(line);
                short CASEW1Check = p.getCASEW1();
                short CASEW2Check = p.getCASEW2();
                short CASEW3Check = p.getCASEW3();
                short CASEW4Check = p.getCASEW4();
                short CASEW5 = p.getCASEW5();
                Short o = CASEW5ToCASEW4.get(CASEW5);
                if (o == null) {
                    env.log("CASEW5 " + CASEW5 + " is not in CASEW5ToCASEW4 lookup");
                } else {
                    short CASEW4 = o;
                    short CASEW3 = CASEW4ToCASEW3.get(CASEW4);
                    short CASEW2 = CASEW3ToCASEW2.get(CASEW3);
                    short CASEW1 = CASEW2ToCASEW1.get(CASEW2);
                    //printCheck(W2, CASEW1Check, CASEW1, CASEW1ToCASEW2);
                    //printCheck(W3, CASEW2Check, CASEW2, CASEW2ToCASEW3);
                    //printCheck(W4, CASEW3Check, CASEW3, CASEW3ToCASEW4);
                    printCheck(W5, CASEW4Check, CASEW4, CASEW4ToCASEW5);
                    HashMap<Short, WaAS_Combined_Record> m = c.getData();
                    WaAS_Combined_Record cr = m.get(CASEW1);
                    if (cr == null) {
                        env.log("No combined record for CASEW1 " + CASEW1 + "! "
                                + "This may be a data error, or this person may "
                                + "have moved from one hhold to another?");
                    } else {
                        HashSet<Short> CASEW2s = CASEW1ToCASEW2.get(CASEW1);
                        CASEW2s.stream().forEach(k2 -> {
                            HashSet<Short> CASEW3s = CASEW2ToCASEW3.get(CASEW2);
                            CASEW3s.stream().forEach(k3 -> {
                                HashSet<Short> CASEW4s = CASEW3ToCASEW4.get(CASEW3);
                                CASEW4s.stream().forEach(k4 -> {
                                    HashSet<Short> CASEW5s = CASEW4ToCASEW5.get(CASEW4);
                                    CASEW5s.stream().forEach(k5 -> {
                                        HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>> w5_2;
                                        w5_2 = cr.w5Records.get(k2);
                                        if (w5_2 == null) {
                                            w5_2 = new HashMap<>();
                                            cr.w5Records.put(k2, w5_2);
                                        }
                                        HashMap<Short, HashMap<Short, WaAS_Wave5_Record>> w5_3;
                                        w5_3 = w5_2.get(k3);
                                        if (w5_3 == null) {
                                            w5_3 = new HashMap<>();
                                            w5_2.put(k3, w5_3);
                                        }
                                        HashMap<Short, WaAS_Wave5_Record> w5_4;
                                        w5_4 = w5_3.get(k4);
                                        if (w5_4 == null) {
                                            w5_4 = new HashMap<>();
                                            w5_3.put(k4, w5_4);
                                        }
                                        WaAS_Wave5_Record w5rec;
                                        w5rec = cr.w5Records.get(k2).get(k3).get(k4).get(k5);
                                        if (w5rec == null) {
                                            w5rec = new WaAS_Wave5_Record(k5);
                                            env.log("Adding people, but there is "
                                                    + "no hhold record for CASEW5 "
                                                    + CASEW5 + "!");
                                        }
                                        w5rec.getPeople().add(p);
                                    });
                                });
                            });
                        });
                    }
                }
            });
            env.logEndTag(m2);
            // Close br
            Generic_IO.closeBufferedReader(br);
            // Cache and clear collection
            data.cacheSubsetCollection(cID, c);
            data.clearCollection(cID);
            env.logEndTag(m1);
        }
        );
        env.logEndTag(m0);
    }

    /**
     * Method for running JavaCodeGeneration
     */
    public void runJavaCodeGeneration() {
        WaAS_JavaCodeGenerator.main(null);
    }

    /**
     * Read input data and create subsets. The subsets are for records in all
     * waves. Organise for person records that each subset is split into
     * separate files such that these files neatly aggregate into those
     * corresponding to household collections. The collections will be merged
     * one by one in doDataProcessingStep2.
     *
     * @param hH hholdHandler
     */
    public void loadHouseholdsInAllWaves(WaAS_HHOLD_Handler hH) {
        String m0 = "loadHouseholdsInAllWaves";
        env.logStartTag(m0);
        /**
         * Step 1: Load hhold data into cache and memory.
         */
        Object[] hholdData = hH.loadHouseholdsInAllWaves(WaAS_Strings.s_InW1W2W3W4W5);
        /**
         * Step 2: Unpack hholdData. hholdData is an Object[] r length size 5.
         * Each element is an Object[] r containing the data from loading each
         * wave.
         * <ul>
         * <li>r[0] is a TreeMap with Integer keys which are the CASE id for the
         * wave and the values are WaAS_Wave1Or2Or3Or4Or5_HHOLD_Record>.</li>
         * <li>r[1] is an array of TreeSets where:
         * <ul>
         * <li>For Wave 5;
         * <ul>
         * <li>r[1][0] is a list of CASEW5 values,</li>
         * <li>r[1][1] is a list of CASEW4 values,</li>
         * <li>r[1][2] is a list of CASEW3 values,</li>
         * <li>r[1][3] is a list of CASEW2 values,</li>
         * <li>r[1][4] is a list of CASEW1 values.</li>
         * </ul></li>
         * <li>For Wave 4;
         * <ul>
         * <li>r[1][0] is a list of CASEW4 values,</li>
         * <li>r[1][1] is a list of CASEW3 values,</li>
         * <li>r[1][2] is a list of CASEW2 values,</li>
         * <li>r[1][3] is a list of CASEW1 values.</li>
         * </ul></li>
         * <li>For Wave 3;
         * <ul>
         * <li>r[1][0] is a list of CASEW3 values,</li>
         * <li>r[1][1] is a list of CASEW2 values,</li>
         * <li>r[1][2] is a list of CASEW1 values.</li>
         * </ul></li>
         * <li>For Wave 2;
         * <ul>
         * <li>r[1][0] is a list of CASEW2 values,</li>
         * <li>r[1][1] is a list of CASEW1 values.</li>
         * </ul></li>
         * <li>For Wave1:
         * <ul>
         * <li>r[1][0] is a list of CASEW1 values.</li>
         * </ul></li>
         * </ul></li>
         * </ul>
         */
        HashMap<Byte, TreeSet<Short>[]> iDLists = new HashMap<>();
        // W1
        Object[] hholdDataW1 = (Object[]) hholdData[0];
        iDLists.put(W1, (TreeSet<Short>[]) hholdDataW1[1]);
        // W2
        Object[] hholdDataW2 = (Object[]) hholdData[1];
        iDLists.put(W2, (TreeSet<Short>[]) hholdDataW2[1]);
        // W3
        Object[] hholdDataW3 = (Object[]) hholdData[2];
        iDLists.put(W3, (TreeSet<Short>[]) hholdDataW3[1]);
        // W4
        Object[] hholdDataW4 = (Object[]) hholdData[3];
        iDLists.put(W4, (TreeSet<Short>[]) hholdDataW4[1]);
        // W5
        Object[] hholdDataW5 = (Object[]) hholdData[4];
        iDLists.put(W5, (TreeSet<Short>[]) hholdDataW5[1]);
        /**
         * Step 3: Print out the Number of Households in each wave.
         *
         * @return r - an Object[] of length 2. r[0] is a TreeMap with keys as
         * CASEW5 and values as WaAS_Wave5_HHOLD_Records. r[1] is an array of
         * TreeSets where: r[1][0] is a list of CASEW1 values, r[1][1] is a list
         * of CASEW2 values, r[1][2] is a list of CASEW3 values, r[1][3] is a
         * list of CASEW4 values.
         */
        for (byte w = NWAVES; w > 0; w--) {
            TreeSet<Short>[] iDList = iDLists.get(w);
            for (int i = w; i > 0; i--) {
                String m;
                if (i == w) {
                    if (w > 2) {
                        m = "" + iDList[i].size() + "\tNumber of HHOLD IDs in "
                                + "Wave " + w + " reported as being in ";
                        for (int j = w - 1; j > 0; j--) {
                            m += "Wave " + j + ", ";
                        }
                        env.log(m);
                    }
                } else {
                    m = "" + iDList[i].size()
                            + "\tNumber of HHOLD IDs in Wave " + w
                            + " reported as being in Wave " + i;
                    env.log(m);
                }
            }
        }
        env.logEndTag(m0);
    }

    /**
     *
     * @param subset Contains a subset of CASEW1 IDs.
     */
    public void initDataSimple(HashSet<Short> subset) {
        String m = "getCombineRecordsSimple";
        env.logStartTag(m);
        Iterator<Short> ite = data.data.keySet().iterator();
        while (ite.hasNext()) {
            short cID = ite.next();
            WaAS_Collection c = data.getCollection(cID);
            WaAS_CollectionSimple cs = new WaAS_CollectionSimple(cID);
            data.dataSimple.put(cID, cs);
            HashMap<Short, WaAS_Combined_Record_Simple> csData = cs.getData();
            HashMap<Short, WaAS_Combined_Record> cData = c.getData();
            Iterator<Short> ite2 = cData.keySet().iterator();
            while (ite2.hasNext()) {
                short CASEW1 = ite2.next();
                if (subset.contains(CASEW1)) {
                    WaAS_Combined_Record cr = cData.get(CASEW1);
                    WaAS_Combined_Record_Simple wcrs;
                    wcrs = new WaAS_Combined_Record_Simple(CASEW1);
                    wcrs.w1Record = cr.w1Record;
                    wcrs.w2Record = cr.w2Records.values().stream().findFirst().get();
                    wcrs.w3Record = cr.w3Records.values().stream().findFirst().get()
                            .values().stream().findFirst().get();
                    wcrs.w4Record = cr.w4Records.values().stream().findFirst().get()
                            .values().stream().findFirst().get()
                            .values().stream().findFirst().get();
                    wcrs.w5Record = cr.w5Records.values().stream().findFirst().get()
                            .values().stream().findFirst().get()
                            .values().stream().findFirst().get()
                            .values().stream().findFirst().get();
                    csData.put(CASEW1, wcrs);
                }
            }
            data.clearCollection(cID);
            data.cacheSubsetCollectionSimple(cID, cs);
            data.clearCollectionSimple(cID);
        }
        env.logEndTag(m);
    }

    boolean doJavaCodeGeneration = false;
    boolean doLoadHouseholdsAndIndividualsInAllWaves = false;
    boolean doLoadHouseholdsInPairedWaves = false;
    boolean doLoadAllHouseholdsRecords = false;

}
