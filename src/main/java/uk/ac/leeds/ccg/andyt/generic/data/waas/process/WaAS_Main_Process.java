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
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_CollectionID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_CollectionSimple;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_CombinedRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_CombinedRecordSimple;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Data;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_HHOLD_Handler;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_HHOLD_Handler.WaAS_W1Data;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_HHOLD_Handler.WaAS_W2Data;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_HHOLD_Handler.WaAS_W3Data;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_HHOLD_Handler.WaAS_W4Data;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_HHOLD_Handler.WaAS_W5Data;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_W4ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_PERSON_Handler;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_W1ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_W2ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_W2Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_W3ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_W3Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_W4Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_W5ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_W5Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W1HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W2HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W3HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W4HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W5HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_W1PRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_W2PRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_W3PRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_W4PRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_W5PRecord;

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
//        p.doJavaCodeGeneration = true;
        p.doLoadAllHouseholdsRecords = true;
        p.doLoadHouseholdsAndIndividualsInAllWaves = true;
        p.doLoadHouseholdsInPairedWaves = true;
        p.run();
    }

    public void run() {
        String m = "run";
        env.logStartTag(m);
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
                WaAS_W2Data W2InW1 = hH.loadW2InW1();
                //Object[] W2InW1 = hH.loadHouseholdsInPreviousWave(W2);
                //TreeMap<Short, WaAS_W2HRecord> W2InW1Recs;
                //W2InW1Recs = (TreeMap<Short, WaAS_W2HRecord>) W2InW1[0];
                //TreeSet<Short>[] W2InW1Sets = (TreeSet<Short>[]) W2InW1[1];
                //TreeSet<Short> W2InW1CASEW1 = W2InW1Sets[0];
                //Object[] W1InW2 = hH.loadW1(W2InW1CASEW1, WaAS_Strings.s_InW2);
                WaAS_W1Data W1InW2 = hH.loadW1(W2InW1.W1InW2, WaAS_Strings.s_InW2);
                //TreeMap<Short, WaAS_W1HRecord> W1InW2Recs;
                //W1InW2Recs = (TreeMap<Short, WaAS_W1HRecord>) W1InW2[0];
                //env.log(W2InW1Recs.size() + "\t W2InW1Recs.size()");
                //env.log(W1InW2Recs.size() + "\t W1InW2Recs.size()");
                env.log(W2InW1.lookup.size() + "\t W2InW1.lookup.size()");
                env.log(W1InW2.lookup.size() + "\t W1InW2.lookup.size()");
            }
            if (true) {
                WaAS_W3Data W3InW2 = hH.loadW3();
                //Object[] W3InW2 = hH.loadHouseholdsInPreviousWave(W3);
                //TreeMap<Short, WaAS_W3HRecord> W3InW2Recs;
                //W3InW2Recs = (TreeMap<Short, WaAS_W3HRecord>) W3InW2[0];
                //TreeSet<Short>[] W3InW2Sets = (TreeSet<Short>[]) W3InW2[1];
                //TreeSet<Short> W3InW2CASEW2 = W3InW2Sets[1];
                WaAS_W2Data W2InW3 = hH.loadW2InS(W3InW2.W2InW3, WaAS_Strings.s_InW3);
                //Object[] W2InW3 = hH.loadW2(W3InW2CASEW2, WaAS_Strings.s_InW3);
                //TreeMap<Short, WaAS_W1HRecord> W2InW3Recs;
                //W2InW3Recs = (TreeMap<Short, WaAS_W1HRecord>) W2InW3[0];
                env.log(W3InW2.lookup.size() + "\t W3InW2.lookup.size()");
                env.log(W2InW3.lookup.size() + "\t W2InW3.lookup.size()");
            }
            if (true) {
                WaAS_W4Data W4InW3 = hH.loadW4();
                //Object[] W4InW3 = hH.loadHouseholdsInPreviousWave(W4);
                //TreeMap<WaAS_W4ID, WaAS_W4HRecord> W4InW3Recs;
                //W4InW3Recs = (TreeMap<WaAS_W4ID, WaAS_W4HRecord>) W4InW3[0];
                //TreeSet<Short>[] W4InW3Sets = (TreeSet<Short>[]) W4InW3[1];
                //TreeSet<Short> W4InW3CASEW3 = W4InW3Sets[2];
                //Object[] W3InW4 = hH.loadW3(W4InW3CASEW3, WaAS_Strings.s_InW4);
                //W3Data W3InW4 = hH.loadW3(W4InW3CASEW3, WaAS_Strings.s_InW4);
                WaAS_W3Data W3InW4 = hH.loadW3InS(W4InW3.W3InW4, WaAS_Strings.s_InW4);
                //TreeMap<Short, WaAS_W1HRecord> W3InW4Recs;
                //W3InW4Recs = (TreeMap<Short, WaAS_W1HRecord>) W3InW4[0];
                //env.log(W4InW3Recs.size() + "\t W4InW3Recs.size()");
                //env.log(W3InW4.lookup.size() + "\t W3InW4Recs.size()");
                env.log(W4InW3.lookup.size() + "\t W4InW3.lookup.size()");
                env.log(W3InW4.lookup.size() + "\t W3InW4.lookup.size()");
            }
            if (true) {
                WaAS_W5Data W5InW4 = hH.loadW5();
                //Object[] W5InW4 = hH.loadHouseholdsInPreviousWave(W5);
                //TreeMap<WaAS_W5ID, WaAS_W5HRecord> W5InW4Recs;
                //W5InW4Recs = (TreeMap<WaAS_W5ID, WaAS_W5HRecord>) W5InW4[0];
                //TreeSet<Short>[] W5InW4Sets = (TreeSet<Short>[]) W5InW4[1];
                //TreeSet<Short> W5InW4CASEW4 = W5InW4Sets[3];
                WaAS_W4Data W4InW5 = hH.loadW4InS(W5InW4.W4InW5, WaAS_Strings.s_InW5);
                //Object[] W4InW5 = hH.loadW4(W5InW4CASEW4, WaAS_Strings.s_InW5);
                //TreeMap<Short, WaAS_W1HRecord> W4InW5Recs;
                //W4InW5Recs = (TreeMap<Short, WaAS_W1HRecord>) W4InW5[0];
                //env.log(W5InW4Recs.size() + "\t W5InW4Recs.size()");
                //env.log(W4InW5Recs.size() + "\t W4InW5Recs.size()");
                env.log(W5InW4.lookup.size() + "\t W5InW4.lookup.size()");
                env.log(W4InW5.lookup.size() + "\t W4InW5.lookup.size()");
            }
            mergePersonAndHouseholdDataIntoCollections(
                    //indir, outdir,
                    WaAS_Strings.s_Paired, hH, chunkSize);
        }
        HashSet<WaAS_W1ID> subset = hH.getSubset(data, 4);
        initDataSimple(subset);
        env.cacheData();
        env.logEndTag(m);
        env.ge.closeLog(env.logID);
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
        TreeMap<WaAS_W1ID, WaAS_W1HRecord> allW1 = hH.loadAllW1();
        TreeMap<WaAS_W2ID, WaAS_W2HRecord> allW2 = hH.loadAllW2();
        TreeMap<WaAS_W3ID, WaAS_W3HRecord> allW3 = hH.loadAllW3();
        TreeMap<WaAS_W4ID, WaAS_W4HRecord> allW4 = hH.loadAllW4();
        TreeMap<WaAS_W5ID, WaAS_W5HRecord> allW5 = hH.loadAllW5();
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
        TreeMap<WaAS_CollectionID, HashSet<WaAS_W1ID>> cIDToW1ID;
        cIDToW1ID = (TreeMap<WaAS_CollectionID, HashSet<WaAS_W1ID>>) o[1];
        HashMap<WaAS_W1ID, WaAS_CollectionID> w1IDToCID = (HashMap<WaAS_W1ID, WaAS_CollectionID>) o[2];
        /**
         * Wave 2
         */
        TreeMap<WaAS_W1ID, HashSet<WaAS_W2ID>> w1IDToW2ID = hH.loadSubsetLookupToW1();
        TreeMap<WaAS_W2ID, WaAS_W1ID> w2IDToW1ID = hH.loadSubsetLookupFromW1();
        mergePersonAndHouseholdDataIntoCollectionsW2(data, type, pH, hH, nOC,
                w1IDToCID, cIDToW1ID, w1IDToW2ID, w2IDToW1ID);
        /**
         * Wave 3
         */
        TreeMap<WaAS_W2ID, HashSet<WaAS_W3ID>> w2IDToW3ID = hH.loadSubsetLookupToW2();
        TreeMap<WaAS_W3ID, WaAS_W2ID> w3IDToW2ID = hH.loadSubsetLookupFromW2();
        mergePersonAndHouseholdDataIntoCollectionsW3(data, type, pH, hH, nOC,
                w1IDToCID, cIDToW1ID, w1IDToW2ID, w2IDToW1ID, w2IDToW3ID,
                w3IDToW2ID);
        /**
         * Wave 4
         */
        TreeMap<WaAS_W3ID, HashSet<WaAS_W4ID>> W3IDToW4ID = hH.loadSubsetLookupToW3();
        TreeMap<WaAS_W4ID, WaAS_W3ID> W4IDToW3ID = hH.loadSubsetLookupFromW3();
        mergePersonAndHouseholdDataIntoCollectionsW4(data, type, pH, hH, nOC,
                w1IDToCID, cIDToW1ID, w1IDToW2ID, w2IDToW1ID, w2IDToW3ID,
                w3IDToW2ID, W3IDToW4ID, W4IDToW3ID);
        /**
         * Wave 5
         */
        TreeMap<WaAS_W4ID, HashSet<WaAS_W5ID>> W4IDToW5ID = hH.loadSubsetLookupToW4();
        TreeMap<WaAS_W5ID, WaAS_W4ID> W5IDToW4ID = hH.loadSubsetLookupFromW4();
        mergePersonAndHouseholdDataIntoCollectionsW5(data, type, pH, hH, nOC,
                w1IDToCID, cIDToW1ID, w1IDToW2ID, w2IDToW1ID, w2IDToW3ID,
                w3IDToW2ID, W3IDToW4ID, W4IDToW3ID, W4IDToW5ID, W5IDToW4ID);
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
        TreeMap<WaAS_W1ID, WaAS_W1HRecord> hs;
        if (type.equalsIgnoreCase(WaAS_Strings.s_InW1W2W3W4W5)) {
            hs = hH.loadCachedSubsetW1(type);
        } else if (type.equalsIgnoreCase(WaAS_Strings.s_Paired)) {
            hs = hH.loadCachedSubset2W1(WaAS_Strings.s_InW2);
        } else {
            env.log("Unrecognised type " + type);
            hs = null;
        }
        TreeSet<WaAS_W1ID> w1IDs = new TreeSet<>();
        w1IDs.addAll(hs.keySet());
        int nOC = (int) Math.ceil((double) w1IDs.size() / (double) chunkSize);
        r[0] = nOC;
        Object[] ps = pH.loadSubsetWave1(w1IDs, nOC, W1);
        TreeMap<WaAS_CollectionID, HashSet<WaAS_W1ID>> CIDToCASEW1;
        CIDToCASEW1 = (TreeMap<WaAS_CollectionID, HashSet<WaAS_W1ID>>) ps[0];
        TreeMap<WaAS_CollectionID, File> cFs = (TreeMap<WaAS_CollectionID, File>) ps[2];
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
            HashSet<WaAS_W1ID> s = CIDToCASEW1.get(cID);
            s.stream().forEach(CASEW1 -> {
                data.CASEW1ToCID.put(CASEW1, cID);
                HashMap<WaAS_W1ID, WaAS_CombinedRecord> m = c.getData();
                WaAS_CombinedRecord cr = m.get(CASEW1);
                if (cr == null) {
                    cr = new WaAS_CombinedRecord(CASEW1);
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
                WaAS_W1PRecord p = new WaAS_W1PRecord(line);
                WaAS_W1ID w1ID = new WaAS_W1ID(p.getCASEW1());
                HashMap<WaAS_W1ID, WaAS_CombinedRecord> m = c.getData();
                WaAS_CombinedRecord cr = m.get(w1ID);
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
     * @param W1IDToCID
     * @param CIDToW1ID
     * @param w1IDToW2ID
     * @param W2IDToW1ID
     */
    public void mergePersonAndHouseholdDataIntoCollectionsW2(WaAS_Data data,
            String type, WaAS_PERSON_Handler pH, WaAS_HHOLD_Handler hH, int nOC,
            HashMap<WaAS_W1ID, WaAS_CollectionID> W1IDToCID,
            TreeMap<WaAS_CollectionID, HashSet<WaAS_W1ID>> CIDToW1ID,
            TreeMap<WaAS_W1ID, HashSet<WaAS_W2ID>> w1IDToW2ID,
            TreeMap<WaAS_W2ID, WaAS_W1ID> W2IDToW1ID) {
        // Wave 2
        String m0 = "mergePersonAndHouseholdDataIntoCollectionsW2";
        env.logStartTag(m0);
        TreeMap<WaAS_W2ID, WaAS_W2HRecord> hs;
        if (type.equalsIgnoreCase(WaAS_Strings.s_InW1W2W3W4W5)) {
            hs = hH.loadCachedSubsetW2(type);
        } else if (type.equalsIgnoreCase(WaAS_Strings.s_Paired)) {
            hs = hH.loadCachedSubset2W2(WaAS_Strings.s_InW3);
        } else {
            env.log("Unrecognised type " + type);
            hs = null;
        }
        TreeMap<WaAS_CollectionID, File> cFs = pH.loadSubsetWave2(nOC,
                W1IDToCID, W2, W2IDToW1ID);
        cFs.keySet().stream().forEach(cID -> {
            String m1 = "Collection ID " + cID;
            env.logStartTag(m1);
            WaAS_Collection c = data.getCollection(cID);
            // Add hhold records.
            String m2 = "Add hhold records";
            env.logStartTag(m2);
            HashSet<WaAS_W1ID> s = CIDToW1ID.get(cID);
            s.stream().forEach(w1ID -> {
                data.CASEW1ToCID.put(w1ID, cID);
                HashMap<WaAS_W1ID, WaAS_CombinedRecord> m = c.getData();
                WaAS_CombinedRecord cr = m.get(w1ID);
                if (cr == null) {
                    env.log("No combined record for CASEW1 " + w1ID + "! "
                            + "This may be a data error?");
                } else {
                    HashSet<WaAS_W2ID> w2IDs = w1IDToW2ID.get(w1ID);
                    w2IDs.stream().forEach(w2ID -> {
                        WaAS_W2Record w2rec = new WaAS_W2Record(w2ID.getID());
                        w2rec.setHhold(hs.get(w2ID));
                        cr.w2Records.put(w2ID, w2rec);
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
                WaAS_W2PRecord p = new WaAS_W2PRecord(line);
                WaAS_W1ID w1IDCheck = new WaAS_W1ID(p.getCASEW1());
                WaAS_W2ID w2ID = new WaAS_W2ID(p.getCASEW2());
                WaAS_W1ID w1ID = W2IDToW1ID.get(w2ID);
                printCheck(W2, w1IDCheck, w1ID, w1IDToW2ID);
                HashMap<WaAS_W1ID, WaAS_CombinedRecord> m = c.getData();
                WaAS_CombinedRecord cr = m.get(w1ID);
                if (cr == null) {
                    env.log("No combined record for CASEW1 " + w1ID + "! "
                            + "This may be a data error, or this person may "
                            + "have moved from one hhold to another?");
                } else {
                    HashSet<WaAS_W2ID> w2IDs = w1IDToW2ID.get(w1ID);
                    w2IDs.stream().forEach(k2 -> {
                        WaAS_W2Record w2rec = cr.w2Records.get(k2);
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

    protected <K, V> void printCheck(byte wave, K wIDCheck, K wID,
            TreeMap<K, HashSet<V>> lookup) {
        if (!wIDCheck.equals(wID)) {
            env.log("Person in Wave " + wave + " record given by " + wID 
                    + " has a CASEW" + (wave - 1) + " as " + wIDCheck 
                    + " - this may mean the person is new to the household/data.");
            if (lookup.get(wIDCheck) == null) {
                env.log("CASEW" + (wave - 1) + "ToCASEW" + wave + ".get(CASEW"
                        + (wave - 1) + "Check) == null");
            } else {
                env.log("CASEW" + (wave - 1) + "ToCASEW" + wave + ".get(CASEW"
                        + (wave - 1) + "Check).size() "
                        + lookup.get(wIDCheck).size());
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
     * @param w1ToCID
     * @param cIDToW1ID
     * @param w1IDToW2ID
     * @param w2IDToW1ID
     * @param w2IDToW3ID
     * @param w3IDToW2ID
     */
    public void mergePersonAndHouseholdDataIntoCollectionsW3(WaAS_Data data,
            String type, WaAS_PERSON_Handler pH, WaAS_HHOLD_Handler hH, int nOC,
            HashMap<WaAS_W1ID, WaAS_CollectionID> w1ToCID,
            TreeMap<WaAS_CollectionID, HashSet<WaAS_W1ID>> cIDToW1ID,
            TreeMap<WaAS_W1ID, HashSet<WaAS_W2ID>> w1IDToW2ID,
            TreeMap<WaAS_W2ID, WaAS_W1ID> w2IDToW1ID,
            TreeMap<WaAS_W2ID, HashSet<WaAS_W3ID>> w2IDToW3ID,
            TreeMap<WaAS_W3ID, WaAS_W2ID> w3IDToW2ID) {
        String m0 = "mergePersonAndHouseholdDataIntoCollectionsW3";
        env.logStartTag(m0);
        TreeMap<WaAS_W3ID, WaAS_W3HRecord> hs;
        if (type.equalsIgnoreCase(WaAS_Strings.s_InW1W2W3W4W5)) {
            hs = hH.loadCachedSubsetW3(type);
        } else if (type.equalsIgnoreCase(WaAS_Strings.s_Paired)) {
            hs = hH.loadCachedSubset2W3(WaAS_Strings.s_InW4);
        } else {
            env.log("Unrecognised type " + type);
            hs = null;
        }
        TreeMap<WaAS_CollectionID, File> cFs = pH.loadSubsetWave3(nOC, w1ToCID,
                W3, w2IDToW1ID, w3IDToW2ID);
        cFs.keySet().stream().forEach(cID -> {
            String m1 = "Collection ID " + cID;
            env.logStartTag(m1);
            WaAS_Collection c = data.getCollection(cID);
            // Add hhold records.
            String m2 = "Add hhold records";
            env.logStartTag(m2);
            HashSet<WaAS_W1ID> s = cIDToW1ID.get(cID);
            s.stream().forEach(w1ID -> {
                data.CASEW1ToCID.put(w1ID, cID);
                HashMap<WaAS_W1ID, WaAS_CombinedRecord> m = c.getData();
                WaAS_CombinedRecord cr = m.get(w1ID);
                if (cr == null) {
                    env.log("No combined record for CASEW1 " + w1ID + "! "
                            + "This may be a data error?");
                } else {
                    HashSet<WaAS_W2ID> w2IDs = w1IDToW2ID.get(w1ID);
                    w2IDs.stream().forEach(w2ID -> {
                        HashMap<WaAS_W3ID, WaAS_W3Record> w3_2 = new HashMap<>();
                        cr.w3Records.put(w2ID, w3_2);
                        HashSet<WaAS_W3ID> CASEW3s = w2IDToW3ID.get(w2ID);
                        CASEW3s.stream().forEach(w3ID -> {
                            WaAS_W3Record w3rec = new WaAS_W3Record(w3ID.getID());
                            w3rec.setHhold(hs.get(w3ID));
                            w3_2.put(w3ID, w3rec);
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
                WaAS_W3PRecord p = new WaAS_W3PRecord(line);
                WaAS_W1ID w1IDCheck = new WaAS_W1ID(p.getCASEW1());
                WaAS_W2ID w2IDCheck = new WaAS_W2ID(p.getCASEW2());
                WaAS_W3ID w3ID = new WaAS_W3ID(p.getCASEW3());
                WaAS_W2ID w2ID = w3IDToW2ID.get(w3ID);
                WaAS_W1ID w1ID = w2IDToW1ID.get(w2ID);
                printCheck(W3, w2IDCheck, w2ID, w2IDToW3ID);
                HashMap<WaAS_W1ID, WaAS_CombinedRecord> m = c.getData();
                WaAS_CombinedRecord cr = m.get(w1ID);
                if (cr == null) {
                    env.log("No combined record for CASEW1 " + w1ID + "! "
                            + "This may be a data error, or this person may "
                            + "have moved from one hhold to another?");
                } else {
                    HashSet<WaAS_W2ID> w2IDs = w1IDToW2ID.get(w1ID);
                    w2IDs.stream().forEach(k2 -> {
                        HashSet<WaAS_W3ID> w3IDs = w2IDToW3ID.get(w2ID);
                        w3IDs.stream().forEach(k3 -> {
                            WaAS_W3Record w3rec = cr.w3Records.get(k2).get(k3);
                            if (w3rec == null) {
                                w3rec = new WaAS_W3Record(k3.getID());
                                env.log("Adding people, but there is no hhold "
                                        + "record for CASEW3 " + w3ID + "!");
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
     * @param w1IDToCID
     * @param cIDToW1ID
     * @param w1IDToW2ID
     * @param w2IDToW1ID
     * @param w2IDToW3ID
     * @param w3IDToW2ID
     * @param w3IDToW4ID
     * @param w4IDToW3ID
     */
    public void mergePersonAndHouseholdDataIntoCollectionsW4(WaAS_Data data,
            String type, WaAS_PERSON_Handler pH, WaAS_HHOLD_Handler hH, int nOC,
            HashMap<WaAS_W1ID, WaAS_CollectionID> w1IDToCID,
            TreeMap<WaAS_CollectionID, HashSet<WaAS_W1ID>> cIDToW1ID,
            TreeMap<WaAS_W1ID, HashSet<WaAS_W2ID>> w1IDToW2ID,
            TreeMap<WaAS_W2ID, WaAS_W1ID> w2IDToW1ID,
            TreeMap<WaAS_W2ID, HashSet<WaAS_W3ID>> w2IDToW3ID,
            TreeMap<WaAS_W3ID, WaAS_W2ID> w3IDToW2ID,
            TreeMap<WaAS_W3ID, HashSet<WaAS_W4ID>> w3IDToW4ID,
            TreeMap<WaAS_W4ID, WaAS_W3ID> w4IDToW3ID) {
        String m0 = "mergePersonAndHouseholdDataIntoCollectionsW4";
        env.logStartTag(m0);
        TreeMap<WaAS_W4ID, WaAS_W4HRecord> hs;
        if (type.equalsIgnoreCase(WaAS_Strings.s_InW1W2W3W4W5)) {
            hs = hH.loadCachedSubsetW4(type);
        } else if (type.equalsIgnoreCase(WaAS_Strings.s_Paired)) {
            hs = hH.loadCachedSubset2W4(WaAS_Strings.s_InW5);
        } else {
            env.log("Unrecognised type " + type);
            hs = null;
        }
        TreeMap<WaAS_CollectionID, File> cFs = pH.loadSubsetWave4(nOC, w1IDToCID, W4,
                w2IDToW1ID, w3IDToW2ID, w4IDToW3ID);
        cFs.keySet().stream().forEach(cID -> {
            String m1 = "Collection ID " + cID;
            env.logStartTag(m1);
            WaAS_Collection c = data.getCollection(cID);
            // Add hhold records.
            String m2 = "Add hhold records";
            env.logStartTag(m2);
            HashSet<WaAS_W1ID> w1IDs = cIDToW1ID.get(cID);
            w1IDs.stream().forEach(w1ID -> {
                data.CASEW1ToCID.put(w1ID, cID);
                HashMap<WaAS_W1ID, WaAS_CombinedRecord> m = c.getData();
                WaAS_CombinedRecord cr = m.get(w1ID);
                if (cr == null) {
                    env.log("No combined record for CASEW1 " + w1ID + "! "
                            + "This may be a data error?");
                } else {
                    HashSet<WaAS_W2ID> w2IDs = w1IDToW2ID.get(w1ID);
                    w2IDs.stream().forEach(w2ID -> {
                        HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, WaAS_W4Record>> w4_2 = new HashMap<>();
                        cr.w4Records.put(w2ID, w4_2);
                        HashSet<WaAS_W3ID> w3IDs = w2IDToW3ID.get(w2ID);
                        w3IDs.stream().forEach(w3ID -> {
                            HashMap<WaAS_W4ID, WaAS_W4Record> w4_3 = new HashMap<>();
                            w4_2.put(w3ID, w4_3);
                            HashSet<WaAS_W4ID> w4IDs = w3IDToW4ID.get(w3ID);
                            w4IDs.stream().forEach(w4ID -> {
                                WaAS_W4Record w4rec  = new WaAS_W4Record(w4ID.getID());
                                w4rec.setHhold(hs.get(w4ID));
                                w4_3.put(w4ID, w4rec);
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
                WaAS_W4PRecord p = new WaAS_W4PRecord(line);
                WaAS_W1ID w1IDCheck = new WaAS_W1ID(p.getCASEW1());
                WaAS_W2ID w2IDCheck = new WaAS_W2ID(p.getCASEW2());
                WaAS_W3ID w3IDCheck = new WaAS_W3ID(p.getCASEW3());
                WaAS_W4ID w4ID = new WaAS_W4ID(p.getCASEW4());
                WaAS_W3ID w3ID = w4IDToW3ID.get(w4ID);
                WaAS_W2ID w2ID = w3IDToW2ID.get(w3ID);
                WaAS_W1ID w1ID = w2IDToW1ID.get(w2ID);
                //printCheck(W2, w1IDCheck, w1ID, w1IDToW2ID);
                //printCheck(W3, w2IDCheck, w2ID, w2IDToW3ID);
                printCheck(W4, w3IDCheck, w3ID, w3IDToW4ID);
                HashMap<WaAS_W1ID, WaAS_CombinedRecord> m = c.getData();
                WaAS_CombinedRecord cr = m.get(w1ID);
                if (cr == null) {
                    env.log("No combined record for CASEW1 " + w1ID + "! "
                            + "This may be a data error, or this person may "
                            + "have moved from one hhold to another?");
                } else {
                    HashSet<WaAS_W2ID> w2IDs = w1IDToW2ID.get(w1ID);
                    w2IDs.stream().forEach(k2 -> {
                        HashSet<WaAS_W3ID> w3IDs = w2IDToW3ID.get(w2ID);
                        w3IDs.stream().forEach(k3 -> {
                            HashSet<WaAS_W4ID> w4IDs = w3IDToW4ID.get(w3ID);
                            w4IDs.stream().forEach(k4 -> {
                                HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, WaAS_W4Record>> w4_2;
                                w4_2 = cr.w4Records.get(k2);
                                if (w4_2 == null) {
                                    w4_2 = new HashMap<>();
                                    cr.w4Records.put(k2, w4_2);
                                }
                                HashMap<WaAS_W4ID, WaAS_W4Record> w4_3  = w4_2.get(k3);
                                if (w4_3 == null) {
                                    w4_3 = new HashMap<>();
                                    w4_2.put(k3, w4_3);
                                }
                                WaAS_W4Record w4rec  = w4_3.get(k4);
                                if (w4rec == null) {
                                    w4rec = new WaAS_W4Record(k4.getID());
                                    env.log("Adding people, but there is no "
                                            + "hhold record for CASEW4 "
                                            + w4ID + "!");
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
     * @param w1IDToCID
     * @param cIDToW1ID
     * @param w1IDToW2IDs
     * @param w2IDToW1ID
     * @param w2IDToW3IDs
     * @param w3IDToW2ID
     * @param w3IDToW4IDs
     * @param w4IDToW3ID
     * @param w4IDToW5IDs
     * @param w5IDToW4ID
     */
    public void mergePersonAndHouseholdDataIntoCollectionsW5(WaAS_Data data,
            String type, WaAS_PERSON_Handler pH, WaAS_HHOLD_Handler hH, int nOC,
            HashMap<WaAS_W1ID, WaAS_CollectionID> w1IDToCID,
            TreeMap<WaAS_CollectionID, HashSet<WaAS_W1ID>> cIDToW1ID,
            TreeMap<WaAS_W1ID, HashSet<WaAS_W2ID>> w1IDToW2IDs,
            TreeMap<WaAS_W2ID, WaAS_W1ID> w2IDToW1ID,
            TreeMap<WaAS_W2ID, HashSet<WaAS_W3ID>> w2IDToW3IDs,
            TreeMap<WaAS_W3ID, WaAS_W2ID> w3IDToW2ID,
            TreeMap<WaAS_W3ID, HashSet<WaAS_W4ID>> w3IDToW4IDs,
            TreeMap<WaAS_W4ID, WaAS_W3ID> w4IDToW3ID,
            TreeMap<WaAS_W4ID, HashSet<WaAS_W5ID>> w4IDToW5IDs,
            TreeMap<WaAS_W5ID, WaAS_W4ID> w5IDToW4ID) {
        // Wave 5
        String m0 = "mergePersonAndHouseholdDataIntoCollectionsW5";
        env.logStartTag(m0);
        TreeMap<WaAS_W5ID, WaAS_W5HRecord> hs;
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
        TreeMap<WaAS_CollectionID, File> cFs = pH.loadSubsetWave5(nOC,
                w1IDToCID, W5, w2IDToW1ID, w3IDToW2ID, w4IDToW3ID, w5IDToW4ID);
        cFs.keySet().stream().forEach(cID -> {
            String m1 = "Collection ID " + cID;
            env.logStartTag(m1);
            WaAS_Collection c = data.getCollection(cID);
            // Add hhold records.
            String m2 = "Add hhold records";
            env.logStartTag(m2);
            HashSet<WaAS_W1ID> s = cIDToW1ID.get(cID);
            s.stream().forEach(w1ID -> {
                data.CASEW1ToCID.put(w1ID, cID);
                HashMap<WaAS_W1ID, WaAS_CombinedRecord> m = c.getData();
                WaAS_CombinedRecord cr = m.get(w1ID);
                if (cr == null) {
                    env.log("No combined record for CASEW1 " + w1ID + "! "
                            + "This may be a data error?");
                } else {
                    HashSet<WaAS_W2ID> CASEW2s = w1IDToW2IDs.get(w1ID);
                    CASEW2s.stream().forEach(w2ID -> {
                        HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>>> w5_2;
                        w5_2 = new HashMap<>();
                        cr.w5Records.put(w2ID, w5_2);
                        HashSet<WaAS_W3ID> CASEW3s = w2IDToW3IDs.get(w2ID);
                        CASEW3s.stream().forEach(w3ID -> {
                            HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>> w5_3 = new HashMap<>();
                            w5_2.put(w3ID, w5_3);
                            HashSet<WaAS_W4ID> w4IDs = w3IDToW4IDs.get(w3ID);
                            w4IDs.stream().forEach(w4ID -> {
                                HashMap<WaAS_W5ID, WaAS_W5Record> w5_4 = new HashMap<>();
                                w5_3.put(w4ID, w5_4);
                                HashSet<WaAS_W5ID> w5IDs;
                                w5IDs = w4IDToW5IDs.get(w4ID);
                                w5IDs.stream().forEach(w5ID -> {
                                    if (w5ID != null) {
                                        WaAS_W5Record w5rec = new WaAS_W5Record(w5ID.getID());
                                        w5rec.setHhold(hs.get(w5ID));
                                        w5_4.put(w5ID, w5rec);
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
                WaAS_W5PRecord p = new WaAS_W5PRecord(line);
                WaAS_W1ID CASEW1Check = new WaAS_W1ID(p.getCASEW1());
                WaAS_W2ID w2IDCheck = new WaAS_W2ID(p.getCASEW2());
                WaAS_W3ID w3IDCheck = new WaAS_W3ID(p.getCASEW3());
                WaAS_W4ID w4IDCheck = new WaAS_W4ID(p.getCASEW4());
                WaAS_W5ID w5ID = new WaAS_W5ID(p.getCASEW5());
                WaAS_W4ID w4ID = w5IDToW4ID.get(w5ID);
                if (w4ID == null) {
                    env.log("CASEW5 " + w5ID + " is not in CASEW5ToCASEW4 lookup");
                } else {
                    WaAS_W3ID w3ID = w4IDToW3ID.get(w4ID);
                    WaAS_W2ID w2ID = w3IDToW2ID.get(w3ID);
                    WaAS_W1ID w1ID = w2IDToW1ID.get(w2ID);
                    //printCheck(W2, CASEW1Check, CASEW1, CASEW1ToCASEW2);
                    //printCheck(W3, CASEW2Check, CASEW2, CASEW2ToCASEW3);
                    //printCheck(W4, CASEW3Check, CASEW3, CASEW3ToCASEW4);
                    printCheck(W5, w4IDCheck, w4ID, w4IDToW5IDs);
                    HashMap<WaAS_W1ID, WaAS_CombinedRecord> m = c.getData();
                    WaAS_CombinedRecord cr = m.get(w1ID);
                    if (cr == null) {
                        env.log("No combined record for CASEW1 " + w1ID + "! "
                                + "This may be a data error, or this person may "
                                + "have moved from one hhold to another?");
                    } else {
                        HashSet<WaAS_W2ID> w2IDs = w1IDToW2IDs.get(w1ID);
                        w2IDs.stream().forEach(k2 -> {
                            HashSet<WaAS_W3ID> w3IDs = w2IDToW3IDs.get(w2ID);
                            w3IDs.stream().forEach(k3 -> {
                                HashSet<WaAS_W4ID> w4IDs = w3IDToW4IDs.get(w3ID);
                                w4IDs.stream().forEach(k4 -> {
                                    HashSet<WaAS_W5ID> w5IDs = w4IDToW5IDs.get(w4ID);
                                    w5IDs.stream().forEach(k5 -> {
                                        HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>>> w5_2;
                                        w5_2 = cr.w5Records.get(k2);
                                        if (w5_2 == null) {
                                            w5_2 = new HashMap<>();
                                            cr.w5Records.put(k2, w5_2);
                                        }
                                        HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>> w5_3 = w5_2.get(k3);
                                        if (w5_3 == null) {
                                            w5_3 = new HashMap<>();
                                            w5_2.put(k3, w5_3);
                                        }
                                        HashMap<WaAS_W5ID, WaAS_W5Record> w5_4  = w5_3.get(k4);
                                        if (w5_4 == null) {
                                            w5_4 = new HashMap<>();
                                            w5_3.put(k4, w5_4);
                                        }
                                        WaAS_W5Record w5rec;
                                        w5rec = cr.w5Records.get(k2).get(k3).get(k4).get(k5);
                                        if (w5rec == null) {
                                            w5rec = new WaAS_W5Record(k5.getID());
                                            env.log("Adding people, but there is "
                                                    + "no hhold record for CASEW5 "
                                                    + w5ID + "!");
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
    public void initDataSimple(HashSet<WaAS_W1ID> subset) {
        String m = "initDataSimple";
        env.logStartTag(m);
        Iterator<WaAS_CollectionID> ite = data.data.keySet().iterator();
        while (ite.hasNext()) {
            WaAS_CollectionID cID = ite.next();
            WaAS_Collection c = data.getCollection(cID);
            WaAS_CollectionSimple cs = new WaAS_CollectionSimple(cID);
            data.dataSimple.put(cID, cs);
            HashMap<WaAS_W1ID, WaAS_CombinedRecordSimple> csData = cs.getData();
            HashMap<WaAS_W1ID, WaAS_CombinedRecord> cData = c.getData();
            Iterator<WaAS_W1ID> ite2 = cData.keySet().iterator();
            while (ite2.hasNext()) {
                WaAS_W1ID w1ID = ite2.next();
                if (subset.contains(w1ID)) {
                    WaAS_CombinedRecord cr = cData.get(w1ID);
                    WaAS_CombinedRecordSimple wcrs;
                    wcrs = new WaAS_CombinedRecordSimple(w1ID);
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
                    csData.put(w1ID, wcrs);
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
