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
package uk.ac.leeds.ccg.data.waas.process;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import uk.ac.leeds.ccg.data.core.Data_Environment;
import uk.ac.leeds.ccg.data.id.Data_CollectionID;
import uk.ac.leeds.ccg.data.id.Data_RecordID;
import uk.ac.leeds.ccg.generic.core.Generic_Environment;
import uk.ac.leeds.ccg.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.data.waas.core.WaAS_Object;
import uk.ac.leeds.ccg.data.waas.core.WaAS_Strings;
import uk.ac.leeds.ccg.data.waas.data.WaAS_Collection;
import uk.ac.leeds.ccg.data.waas.data.records.WaAS_CombinedRecord;
import uk.ac.leeds.ccg.data.waas.data.records.WaAS_CombinedRecordSimple;
import uk.ac.leeds.ccg.data.waas.data.WaAS_DataInAllWaves;
import uk.ac.leeds.ccg.data.waas.data.subset.WaAS_DataSubsetW1;
import uk.ac.leeds.ccg.data.waas.data.subset.WaAS_DataSubsetW2;
import uk.ac.leeds.ccg.data.waas.data.subset.WaAS_DataSubsetW3;
import uk.ac.leeds.ccg.data.waas.data.subset.WaAS_DataSubsetW4;
import uk.ac.leeds.ccg.data.waas.data.subset.WaAS_DataSubsetW5;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_W4ID;
import uk.ac.leeds.ccg.data.waas.data.handlers.WaAS_PERSON_Handler;
import uk.ac.leeds.ccg.data.waas.data.WaAS_W1Data;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_W1ID;
import uk.ac.leeds.ccg.data.waas.data.records.WaAS_W1Record;
import uk.ac.leeds.ccg.data.waas.data.WaAS_W2Data;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_W2ID;
import uk.ac.leeds.ccg.data.waas.data.records.WaAS_W2Record;
import uk.ac.leeds.ccg.data.waas.data.WaAS_W3Data;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_W3ID;
import uk.ac.leeds.ccg.data.waas.data.records.WaAS_W3Record;
import uk.ac.leeds.ccg.data.waas.data.WaAS_W4Data;
import uk.ac.leeds.ccg.data.waas.data.records.WaAS_W4Record;
import uk.ac.leeds.ccg.data.waas.data.WaAS_W5Data;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_CollectionID;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_RecordID;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_W5ID;
import uk.ac.leeds.ccg.data.waas.data.records.WaAS_W5Record;
import uk.ac.leeds.ccg.data.waas.data.person.WaAS_W1PRecord;
import uk.ac.leeds.ccg.data.waas.data.person.WaAS_W2PRecord;
import uk.ac.leeds.ccg.data.waas.data.person.WaAS_W3PRecord;
import uk.ac.leeds.ccg.data.waas.data.person.WaAS_W4PRecord;
import uk.ac.leeds.ccg.data.waas.data.person.WaAS_W5PRecord;
import uk.ac.leeds.ccg.generic.io.Generic_Defaults;
import uk.ac.leeds.ccg.generic.io.Generic_IO;

/**
 * WaAS_Main
 *
 * @author Andy Turner
 * @version 1.0.0
 */
public class WaAS_Main extends WaAS_Object {

    // For convenience
    //protected WaAS_Data data;
    public WaAS_Main(WaAS_Environment env) {
        super(env);
    }

    public static void main(String[] args) {
        try {
            Data_Environment de = new Data_Environment(
                    new Generic_Environment(new Generic_Defaults()));
            Path dataDir = Paths.get(de.files.getDir().toString(),
                    WaAS_Strings.s_data, WaAS_Strings.s_WaAS);
            WaAS_Environment we = new WaAS_Environment(de, dataDir);
            WaAS_Main p = new WaAS_Main(we);
            // Main switches
            p.doLoadAllHouseholdsRecords = true;
            p.doLoadHouseholdsAndIndividualsInAllWaves = true;
            p.doLoadHouseholdsInPairedWaves = true;
            p.doSubset = true;
            p.run();
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
        }
    }

    public void run() throws FileNotFoundException, IOException, ClassNotFoundException {
        String m = "run";
        env.logStartTag(m);
        int chunkSize = 512;//1024;//2048;//512;//256;//1024;// 512; 256;
        if (doLoadAllHouseholdsRecords) {
            loadAllHouseholdRecords();
        }
        if (doLoadHouseholdsAndIndividualsInAllWaves) {
            WaAS_DataInAllWaves dataInAllWaves = new WaAS_DataInAllWaves(we);
            String type = WaAS_Strings.s__In_w1w2w3w4w5;
            dataInAllWaves.loadDataInAllWaves(type);
            mergePersonAndHouseholdDataIntoCollections(type, chunkSize);
        }
        if (doLoadHouseholdsInPairedWaves) {
            if (true) {
                WaAS_W2Data W2InW1 = we.hh.loadW2InW1();
                //Object[] w2_In_w1 = hH.loadHouseholdsInPreviousWave(W2);
                //Map<Short, WaAS_W2HRecord> W2InW1Recs;
                //W2InW1Recs = (Map<Short, WaAS_W2HRecord>) w2_In_w1[0];
                //Set<Short>[] W2InW1Sets = (Set<Short>[]) w2_In_w1[1];
                //Set<Short> W2InW1CASEW1 = W2InW1Sets[0];
                //Object[] w1_In_w2 = hH.loadW1(W2InW1CASEW1, WaAS_Strings.s_InW2);
                WaAS_W1Data W1InW2 = we.hh.loadW1(W2InW1.w1_In_w2, WaAS_Strings.s__In_ + WaAS_Strings.s_w2);
                //Map<Short, WaAS_W1HRecord> W1InW2Recs;
                //W1InW2Recs = (Map<Short, WaAS_W1HRecord>) w1_In_w2[0];
                //env.log(W2InW1Recs.size() + "\t W2InW1Recs.size()");
                //env.log(W1InW2Recs.size() + "\t W1InW2Recs.size()");
                env.log(W2InW1.lookup.size() + "\t W2InW1.lookup.size()");
                env.log(W1InW2.lookup.size() + "\t W1InW2.lookup.size()");
            }
            if (true) {
                WaAS_W3Data W3InW2 = we.hh.loadW3InW2();
                //Object[] W3InW2 = hH.loadHouseholdsInPreviousWave(W3);
                //Map<Short, WaAS_W3HRecord> W3InW2Recs;
                //W3InW2Recs = (Map<Short, WaAS_W3HRecord>) W3InW2[0];
                //Set<Short>[] W3InW2Sets = (Set<Short>[]) W3InW2[1];
                //Set<Short> W3InW2CASEW2 = W3InW2Sets[1];
                WaAS_W2Data W2InW3 = we.hh.loadW2InS(W3InW2.w2_In_w3, WaAS_Strings.s__In_ + WaAS_Strings.s_w3);
                //Object[] w2_In_w3 = hH.loadW2(W3InW2CASEW2, WaAS_Strings.s_InW3);
                //Map<Short, WaAS_W1HRecord> W2InW3Recs;
                //W2InW3Recs = (Map<Short, WaAS_W1HRecord>) w2_In_w3[0];
                env.log(W3InW2.lookup.size() + "\t W3InW2.lookup.size()");
                env.log(W2InW3.lookup.size() + "\t W2InW3.lookup.size()");
            }
            if (true) {
                WaAS_W4Data W4InW3 = we.hh.loadW4InW3();
                //Object[] W4InW3 = hH.loadHouseholdsInPreviousWave(W4);
                //Map<WaAS_W4ID, WaAS_W4HRecord> W4InW3Recs;
                //W4InW3Recs = (Map<WaAS_W4ID, WaAS_W4HRecord>) W4InW3[0];
                //Set<Short>[] W4InW3Sets = (Set<Short>[]) W4InW3[1];
                //Set<Short> W4InW3CASEW3 = W4InW3Sets[2];
                //Object[] w3_In_w4 = hH.loadW3InW2(W4InW3CASEW3, WaAS_Strings.s_InW4);
                //W3Data w3_In_w4 = hH.loadW3InW2(W4InW3CASEW3, WaAS_Strings.s_InW4);
                WaAS_W3Data W3InW4 = we.hh.loadW3InS(W4InW3.w3_In_w4, WaAS_Strings.s__In_ + WaAS_Strings.s_w4);
                //Map<Short, WaAS_W1HRecord> W3InW4Recs;
                //W3InW4Recs = (Map<Short, WaAS_W1HRecord>) w3_In_w4[0];
                //env.log(W4InW3Recs.size() + "\t W4InW3Recs.size()");
                //env.log(w3_In_w4.lookup.size() + "\t W3InW4Recs.size()");
                env.log(W4InW3.lookup.size() + "\t W4InW3.lookup.size()");
                env.log(W3InW4.lookup.size() + "\t W3InW4.lookup.size()");
            }
            if (true) {
                WaAS_W5Data W5InW4 = we.hh.loadW5InW4();
                //Object[] W5InW4 = hH.loadHouseholdsInPreviousWave(W5);
                //Map<WaAS_W5ID, WaAS_W5HRecord> W5InW4Recs;
                //W5InW4Recs = (Map<WaAS_W5ID, WaAS_W5HRecord>) W5InW4[0];
                //Set<Short>[] W5InW4Sets = (Set<Short>[]) W5InW4[1];
                //Set<Short> W5InW4CASEW4 = W5InW4Sets[3];
                WaAS_W4Data W4InW5 = we.hh.loadW4InS(W5InW4.w4_In_w5, WaAS_Strings.s__In_ + WaAS_Strings.s_w5);
                //Object[] w4_In_w5 = hH.loadW4InW3(W5InW4CASEW4, WaAS_Strings.s_InW5);
                //Map<Short, WaAS_W1HRecord> W4InW5Recs;
                //W4InW5Recs = (Map<Short, WaAS_W1HRecord>) w4_In_w5[0];
                //env.log(W5InW4Recs.size() + "\t W5InW4Recs.size()");
                //env.log(W4InW5Recs.size() + "\t W4InW5Recs.size()");
                env.log(W5InW4.lookup.size() + "\t W5InW4.lookup.size()");
                env.log(W4InW5.lookup.size() + "\t W4InW5.lookup.size()");
            }
            mergePersonAndHouseholdDataIntoCollections(WaAS_Strings.s_Paired, chunkSize);
        }
        if (doSubset) {
            Set<WaAS_W1ID> subset = we.hh.getSubset(4);
            initDataSimple(subset);
        }
        we.swapData();
        env.logEndTag(m);
        env.closeLog(we.de.logID);
    }

    /**
     * Read input data and create household all sets.
     *
     * @throws java.io.FileNotFoundException If encountered.
     * @throws java.io.IOException If encountered.
     * @throws java.lang.ClassNotFoundException If encountered.
     */
    public void loadAllHouseholdRecords() throws IOException, FileNotFoundException, ClassNotFoundException {
        String m0 = "loadAllHouseholdRecords";
        env.logStartTag(m0);
        we.hh.loadAllW1();
        we.hh.loadAllW2();
        we.hh.loadAllW3();
        we.hh.loadAllW4();
        we.hh.loadAllW5();
        env.logEndTag(m0);
    }

    /**
     * Merge Person and Household Data into data.
     *
     * @param type s__In_w1w2w3w4w5
     * @param chunkSize
     * @throws java.io.IOException If encountered.
     * @throws java.lang.ClassNotFoundException If encountered.
     */
    public void mergePersonAndHouseholdDataIntoCollections(String type,
            int chunkSize) throws IOException, ClassNotFoundException {
        String m = "mergePersonAndHouseholdDataIntoCollections(" + type + ", ...)";
        env.logStartTag(m);
        WaAS_PERSON_Handler pH = new WaAS_PERSON_Handler(we);
        /**
         * Wave 1
         */
        WaAS_DataSubsetW1 dsW1 = mergePersonAndHouseholdDataIntoCollectionsW1(type, pH, chunkSize);

        /**
         * Wave 2
         */
        Map<WaAS_W1ID, Set<WaAS_W2ID>> w1_To_w2 = pH.loadSubsetLookupToW1();
        Map<WaAS_W2ID, WaAS_W1ID> w2_To_w1 = pH.loadSubsetLookupFromW1();
        WaAS_DataSubsetW2 dsW2 = mergePersonAndHouseholdDataIntoCollectionsW2(
                type, pH, dsW1, w1_To_w2, w2_To_w1);
        /**
         * Wave 3
         */
        Map<WaAS_W2ID, Set<WaAS_W3ID>> w2_To_w3 = pH.loadSubsetLookupToW2();
        Map<WaAS_W3ID, WaAS_W2ID> w3_To_w2 = pH.loadSubsetLookupFromW2();
        WaAS_DataSubsetW3 dsW3 = mergePersonAndHouseholdDataIntoCollectionsW3(
                type, pH, dsW2, dsW1, w1_To_w2, w2_To_w1, w2_To_w3, w3_To_w2);
        /**
         * Wave 4
         */
        Map<WaAS_W3ID, Set<WaAS_W4ID>> w3_To_w4 = pH.loadSubsetLookupToW3();
        Map<WaAS_W4ID, WaAS_W3ID> w4_To_w3 = pH.loadSubsetLookupFromW3();
        WaAS_DataSubsetW4 dsW4 = mergePersonAndHouseholdDataIntoCollectionsW4(
                type, pH, dsW3, dsW1, w1_To_w2, w2_To_w1, w2_To_w3, w3_To_w2,
                w3_To_w4, w4_To_w3);
        /**
         * Wave 5
         */
        Map<WaAS_W4ID, Set<WaAS_W5ID>> w4_To_w5 = pH.loadSubsetLookupToW4();
        Map<WaAS_W5ID, WaAS_W4ID> w5_To_w4 = pH.loadSubsetLookupFromW4();
        WaAS_DataSubsetW5 dsW5 = mergePersonAndHouseholdDataIntoCollectionsW5(
                type, pH, dsW4, dsW1, w1_To_w2, w2_To_w1, w2_To_w3, w3_To_w2,
                w3_To_w4, w4_To_w3, w4_To_w5, w5_To_w4);
        env.log("data.lookup.size() " + dsW1.w1_To_c.size());
        env.log("data.data.size() " + we.data.getData().size());
        we.swapData();
        env.logEndTag(m);
    }

    public class MergedDataW1 {

        Map<WaAS_W1ID, WaAS_W1Record> hs;
        /**
         * Number of data
         */
        int nOC;
    }

    /**
     * Merge Person and Household Data for Wave 1.
     *
     * @param type
     * @param pH personHandler
     * @param chunkSize
     * @return
     * @throws java.io.IOException If encountered.
     * @throws java.lang.ClassNotFoundException If encountered.
     */
    public WaAS_DataSubsetW1 mergePersonAndHouseholdDataIntoCollectionsW1(
            String type, WaAS_PERSON_Handler pH, int chunkSize) 
            throws IOException, ClassNotFoundException {
        // Wave 1
        String m0 = "mergePersonAndHouseholdDataIntoCollectionsW1";
        we.de.logStartTagMem(m0);
        //Object[] r = new Object[3];
        Map<WaAS_W1ID, WaAS_W1Record> hs;
        if (type.equalsIgnoreCase(WaAS_Strings.s__In_w1w2w3w4w5)) {
            hs = we.hh.loadCachedSubsetW1(type);
        } else if (type.equalsIgnoreCase(WaAS_Strings.s_Paired)) {
            hs = we.hh.loadCachedSubset2W1(WaAS_Strings.s__In_ + WaAS_Strings.s_w2);
        } else {
            env.log("Unrecognised type " + type);
            hs = null;
        }
        Set<WaAS_W1ID> w1IDs = new TreeSet<>();
        if (hs != null) {
            w1IDs.addAll(hs.keySet());
        }
        we.data.nOC = (int) Math.ceil((double) w1IDs.size() / (double) chunkSize);
        WaAS_DataSubsetW1 sW1 = pH.loadDataSubsetW1(w1IDs);
        //sW1.c_To_w1.keySet().stream().forEach(cID -> {
        env.log("sW1.c_To_w1.keySet().size() " + sW1.c_To_w1.keySet().size());
        Iterator<WaAS_CollectionID> ite2 = sW1.c_To_w1.keySet().iterator();
        while (ite2.hasNext()) {
            WaAS_CollectionID cID = ite2.next();
            String m1 = "Collection ID " + cID;
            we.de.logStartTagMem(m1);
            WaAS_Collection c = new WaAS_Collection(cID);
            //Map<WaAS_CollectionID, WaAS_Collection> d = we.data.getData();

            we.data.getData().put(cID, c);
            // Add hhold records.
            String m2 = "Add hhold records";
            we.de.logStartTagMem(m2);
            Set<WaAS_W1ID> s = sW1.c_To_w1.get(cID);
            int count = 0;
            // The following way not using streams works!
            Iterator<WaAS_W1ID> ite = s.iterator();
            while (ite.hasNext()) {
                WaAS_W1ID w1ID = ite.next();
                //env.data.w1_To_c.put(w1ID, cID);
                WaAS_CombinedRecord cr = (WaAS_CombinedRecord) c.data.get(w1ID);
                if (cr == null) {
                    WaAS_W1Record w1Rec = hs.get(w1ID);
                    cr = new WaAS_CombinedRecord(w1Rec);
                    c.data.put(w1ID, cr);
                    count++;
                }
            }

// The problem with using streams is that it is not possible that way to set c to null and if that is not done there is a memory leak!
//            s.stream().forEach(w1ID -> { 
//                we.data.w1_To_c.put(w1ID, cID);
//                Map<WaAS_W1ID, WaAS_CombinedRecord> m = c.getData();
//                WaAS_CombinedRecord cr = m.get(w1ID);
//                if (cr == null) {
//                    WaAS_W1Record w1Rec = hs.get(w1ID);
//                    cr = new WaAS_CombinedRecord(we, w1Rec);
//                    m.put(w1ID, cr);
//                }
//                //cr.w1Rec.setHhold(hs.get(CASEW1));
//            });
            env.log("Number of records added " + count);
            we.de.logEndTagMem(m2);
            // Add person records.
            m2 = "Add person records";
            we.de.logStartTagMem(m2);
            count = 0;
            Path f = sW1.cFs.get(cID); // This should be DataSubset1_0.tab
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.readLine(); // skip header
            String line = br.readLine();
            int ln = 0;
            int i = 0;
            while (line != null) {
                try {
                    WaAS_W1PRecord rec = new WaAS_W1PRecord(new WaAS_RecordID(i), line);
                    i++;
                    line = br.readLine();
                    WaAS_W1ID w1ID = we.data.CASEW1_To_w1.get(rec.getCASEW1());
                    if (s.contains(w1ID)) {
                        WaAS_CombinedRecord cr = (WaAS_CombinedRecord) c.data.get(w1ID);
                        cr.w1Rec.getPrs().add(rec);
                        count++;
                    }
                } catch (Exception ex) {
                    we.logLineNotLoading(ex, ln);
                }
                ln++;
            }

//// The problem with using streams is that it is not possible that way to set c to null and if that is not done there is a memory leak!
////            br.lines().skip(1).forEach(line -> {
////                WaAS_W1PRecord p = new WaAS_W1PRecord(line);
////                //WaAS_W1ID w1ID = new WaAS_W1ID(p.getCASEW1());
////                WaAS_W1ID w1ID = we.data.CASEW1_To_w1.get(p.getCASEW1());
////                Map<WaAS_W1ID, WaAS_CombinedRecord> m = c.getData();
////                WaAS_CombinedRecord cr = m.get(w1ID);
////                cr.w1Rec.getPrs().add(p);
////            });
            env.log("Number of records added " + count);
            we.de.logEndTagMem(m2);
            // Cache and clear collection
            we.data.cacheSubsetCollection(cID, c);
            we.data.clearCollection(cID);
            env.log("Set c = null and call the garbage collector.");
            c = null; // Free memory!
            System.gc();
            we.de.logEndTagMem(m1);
            //});
        }
        we.de.logEndTagMem(m0);
        return sW1;
    }

    /**
     * Merge Person and Household Data for Wave 2.
     *
     * @param type
     * @param pH
     * @param sW1
     * @param w1_To_w2
     * @param w2_To_w1
     * @return
     * @throws java.io.IOException If encountered.
     * @throws java.lang.ClassNotFoundException If encountered.
     */
    public WaAS_DataSubsetW2 mergePersonAndHouseholdDataIntoCollectionsW2(
            String type, WaAS_PERSON_Handler pH, WaAS_DataSubsetW1 sW1,
            Map<WaAS_W1ID, Set<WaAS_W2ID>> w1_To_w2,
            Map<WaAS_W2ID, WaAS_W1ID> w2_To_w1) throws IOException, ClassNotFoundException {
        // Wave 2
        String m0 = "mergePersonAndHouseholdDataIntoCollectionsW2";
        we.de.logStartTagMem(m0);
        Map<WaAS_W2ID, WaAS_W2Record> hs;
        if (type.equalsIgnoreCase(WaAS_Strings.s__In_w1w2w3w4w5)) {
            hs = we.hh.loadCachedSubsetW2(type);
        } else if (type.equalsIgnoreCase(WaAS_Strings.s_Paired)) {
            hs = we.hh.loadCachedSubset2W2(WaAS_Strings.s__In_ + WaAS_Strings.s_w3);

            // Debugging code.
            if (hs == null) {
                int debug = 1;
                env.log("hs == null, type = " + type);
            }

        } else {
            env.log("Unrecognised type " + type);
            hs = null;
        }
        WaAS_DataSubsetW2 sW2 = pH.loadDataSubsetW2(sW1, w2_To_w1);
        //sW2.cFs.keySet().stream().forEach(cID -> {
        Iterator<WaAS_CollectionID> ite2 = sW2.cFs.keySet().iterator();
        while (ite2.hasNext()) {
            WaAS_CollectionID cID = ite2.next();
            String m1 = "Collection ID " + cID;
            we.de.logStartTagMem(m1);
            WaAS_Collection c = we.data.getCollection(cID);
            // Add hhold records.
            String m2 = "Add hhold records";
            env.logStartTag(m2);
            Set<WaAS_W1ID> s = sW1.c_To_w1.get(cID);
            // The following way not using streams works!
            Iterator<WaAS_W1ID> ite = s.iterator();
            while (ite.hasNext()) {
                WaAS_W1ID w1ID = ite.next();

                if (w1ID == null) {
                    env.log("w1ID " + w1ID);
                }

                we.data.w1_To_c.put(w1ID, cID);
                WaAS_CombinedRecord cr = (WaAS_CombinedRecord) c.data.get(w1ID);
                if (cr == null) {
                    env.log("No combined record for " + w1ID + "! Data error?");
                } else {
                    Set<WaAS_W2ID> w2IDs = w1_To_w2.get(w1ID);
                    if (w2IDs == null) {
                        env.log("w2IDs = null for w1ID " + w1ID);
                    } else {

                        w2IDs.stream().forEach(w2ID -> {
                            if (w2ID == null) {
                                env.log("w2ID = null");
                            } else {
                                WaAS_W2Record w2rec = hs.get(w2ID);
                                cr.w2Recs.put(w2ID, w2rec);
                            }
                        });
                    }
                }
            }
// The problem with using streams is that it is not possible that way to set c to null and if that is not done there is a memory leak!
//            s.stream().forEach(w1ID -> {
//                we.data.w1_To_c.put(w1ID, cID);
//                Map<WaAS_W1ID, WaAS_CombinedRecord> m = c.getData();
//                WaAS_CombinedRecord cr = m.get(w1ID);
//                if (cr == null) {
//                    env.log("No combined record for " + w1ID + "! Data error?");
//                } else {
//                    Set<WaAS_W2ID> w2IDs = w1_To_w2.get(w1ID);
//                    w2IDs.stream().forEach(w2ID -> {
//                        WaAS_W2Record w2rec = hs.get(w2ID);
//                        cr.w2Recs.put(w2ID, w2rec);
//                    });
//                }
//            });
            we.de.logEndTagMem(m2);
            // Add person records.
            m2 = "Add person records";
            env.logStartTag(m2);
            Path f = sW2.cFs.get(cID);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            // The following way not using streams works!
            String line;
            br.readLine(); // Skip header
            br = Generic_IO.getBufferedReader(f);
            br.readLine(); // skip header
            line = br.readLine();
            int ln = 0;
            int i = 0;
            while (line != null) {
                try {
                    WaAS_W2PRecord p = new WaAS_W2PRecord(new WaAS_RecordID(i), line);
                    i++;
                    line = br.readLine();
                    //WaAS_W1ID w1IDCheck = new WaAS_W1ID(p.getCASEW1());
                    //WaAS_W1ID w1IDCheck = we.data.CASEW1_To_w1.get(p.getCASEW1());
                    //WaAS_W2ID w2ID = new WaAS_W2ID(p.getCASEW2());
                    WaAS_W2ID w2ID = we.data.CASEW2_To_w2.get(p.getCASEW2());
                    WaAS_W1ID w1ID = w2_To_w1.get(w2ID);
                    //printCheck(we.W2, w1IDCheck, w1ID, w1_To_w2);
                    if (s.contains(w1ID)) {
                        WaAS_CombinedRecord cr = (WaAS_CombinedRecord) c.data.get(w1ID);
                        if (cr == null) {
                            env.log("No combined record for " + w1ID + "! Data error, "
                                    + "or this person may have moved household?");
                        } else {
                            Set<WaAS_W2ID> w2IDs = w1_To_w2.get(w1ID);
                            w2IDs.stream().forEach(k2 -> {
                                WaAS_W2Record w2rec = cr.w2Recs.get(k2);
                                if (w2rec == null) {
                                    w2rec = new WaAS_W2Record(k2);
                                    env.log("Adding people, but there is no hhold "
                                            + "record for " + w2ID + "!");
                                }
                                w2rec.getPrs().add(p);
                            });
                        }
                    }
                } catch (Exception ex) {
                    we.logLineNotLoading(ex, ln);
                }
                ln++;
            }
//// The problem with using streams is that it is not possible that way to set c to null and if that is not done there is a memory leak!
////            br.lines().skip(1).forEach(line -> {
////                WaAS_W2PRecord p = new WaAS_W2PRecord(line);
////                //WaAS_W1ID w1IDCheck = new WaAS_W1ID(p.getCASEW1());
////                //WaAS_W1ID w1IDCheck = we.data.CASEW1_To_w1.get(p.getCASEW1());
////                //WaAS_W2ID w2ID = new WaAS_W2ID(p.getCASEW2());
////                WaAS_W2ID w2ID = we.data.CASEW2_To_w2.get(p.getCASEW2());
////                WaAS_W1ID w1ID = w2_To_w1.get(w2ID);
////                //printCheck(we.W2, w1IDCheck, w1ID, w1_To_w2);
////                Map<WaAS_W1ID, WaAS_CombinedRecord> m = c.getData();
////                WaAS_CombinedRecord cr = m.get(w1ID);
////                if (cr == null) {
////                    env.log("No combined record for " + w1ID + "! Data error, "
////                            + "or this person may have moved household?");
////                } else {
////                    Set<WaAS_W2ID> w2IDs = w1_To_w2.get(w1ID);
////                    w2IDs.stream().forEach(k2 -> {
////                        WaAS_W2Record w2rec = cr.w2Recs.get(k2);
////                        if (w2rec == null) {
////                            w2rec = new WaAS_W2Record(we, k2);
////                            env.log("Adding people, but there is no hhold "
////                                    + "record for " + w2ID + "!");
////                        }
////                        w2rec.getPrs().add(p);
////                    });
////                }
////            });
            we.de.logEndTagMem(m2);
            // Close br
            io.closeBufferedReader(br);
            // Cache and clear collection
            we.data.cacheSubsetCollection(cID, c);
            we.data.clearCollection(cID);
            env.log("Set c = null and call the garbage collector.");
            c = null; // Free memory!
            System.gc();
            we.de.logEndTagMem(m1);
            //});
        }
        env.logEndTag(m0);
        return sW2;
    }

    /**
     * Checks to see if wIDCheck.equals(wID). If that is not the case, then
     * lookup is used to
     *
     * @param <K>
     * @param <V>
     * @param wave
     * @param wIDCheck
     * @param wID
     * @param lookup
     */
    protected <K, V> void printCheck(byte wave, K wIDCheck, K wID,
            Map<K, Set<V>> lookup) {
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
     * @param type
     * @param pH personHandler
     * @param sW2
     * @param sW1
     * @param w1_To_w2
     * @param w2_To_w1
     * @param w2_To_w3
     * @param w3_To_w2
     * @return
     * @throws java.io.IOException If encountered.
     * @throws java.lang.ClassNotFoundException If encountered.
     */
    public WaAS_DataSubsetW3 mergePersonAndHouseholdDataIntoCollectionsW3(
            String type, WaAS_PERSON_Handler pH,
            WaAS_DataSubsetW2 sW2, WaAS_DataSubsetW1 sW1,
            Map<WaAS_W1ID, Set<WaAS_W2ID>> w1_To_w2,
            Map<WaAS_W2ID, WaAS_W1ID> w2_To_w1,
            Map<WaAS_W2ID, Set<WaAS_W3ID>> w2_To_w3,
            Map<WaAS_W3ID, WaAS_W2ID> w3_To_w2) throws IOException,
            ClassNotFoundException {
        String m0 = "mergePersonAndHouseholdDataIntoCollectionsW3";
        env.logStartTag(m0);
        Map<WaAS_W3ID, WaAS_W3Record> hs;
        if (type.equalsIgnoreCase(WaAS_Strings.s__In_w1w2w3w4w5)) {
            hs = we.hh.loadCachedSubsetW3(type);
        } else if (type.equalsIgnoreCase(WaAS_Strings.s_Paired)) {
            hs = we.hh.loadCachedSubset2W3(WaAS_Strings.s__In_ + WaAS_Strings.s_w4);
        } else {
            env.log("Unrecognised type " + type);
            hs = null;
        }
        WaAS_DataSubsetW3 sW3 = pH.loadDataSubsetW3(sW2, sW1.w1_To_c, w2_To_w1, w3_To_w2);
        //sW3.cFs.keySet().stream().forEach(cID -> {
        Iterator<WaAS_CollectionID> ite2 = sW3.cFs.keySet().iterator();
        while (ite2.hasNext()) {
            WaAS_CollectionID cID = ite2.next();
            String m1 = "Collection ID " + cID;
            we.de.logStartTagMem(m1);
            WaAS_Collection c = we.data.getCollection(cID);
            // Add hhold records.
            String m2 = "Add hhold records";
            we.de.logStartTagMem(m2);
            Set<WaAS_W1ID> s = sW1.c_To_w1.get(cID);
            // The following way not using streams works!
            Iterator<WaAS_W1ID> ite = s.iterator();
            while (ite.hasNext()) {
                WaAS_W1ID w1ID = ite.next();
                we.data.w1_To_c.put(w1ID, cID);
                WaAS_CombinedRecord cr = (WaAS_CombinedRecord) c.data.get(w1ID);
                if (cr == null) {
                    env.log("No combined record for CASEW1 " + w1ID + "! "
                            + "This may be a data error?");
                } else {
                    Set<WaAS_W2ID> w2IDs = w1_To_w2.get(w1ID);
                    if (w2IDs == null) {
                        env.log("w2IDs = null for w1ID " + w1ID);
                    } else {
                        w2IDs.stream().forEach(w2ID -> {
                            Map<WaAS_W3ID, WaAS_W3Record> w3_2 = new HashMap<>();
                            cr.w3Recs.put(w2ID, w3_2);
                            Set<WaAS_W3ID> w3IDs = w2_To_w3.get(w2ID);
                            if (w3IDs == null) {
                                env.log("w3IDs = null for w2ID " + w2ID);
                            } else {
                                w3IDs.stream().forEach(w3ID -> {
                                    if (w3ID == null) {
                                        env.log("w3ID = null");
                                    } else {
                                        WaAS_W3Record w3rec = hs.get(w3ID);
                                        w3_2.put(w3ID, w3rec);
                                    }
                                });
                            }
                        });
                    }
                }
            }
// The problem with using streams is that it is not possible that way to set c to null and if that is not done there is a memory leak!
//            s.stream().forEach(w1ID -> {
//                we.data.w1_To_c.put(w1ID, cID);
//                Map<WaAS_W1ID, WaAS_CombinedRecord> m = c.getData();
//                WaAS_CombinedRecord cr = m.get(w1ID);
//                if (cr == null) {
//                    env.log("No combined record for CASEW1 " + w1ID + "! "
//                            + "This may be a data error?");
//                } else {
//                    Set<WaAS_W2ID> w2IDs = w1_To_w2.get(w1ID);
//                    w2IDs.stream().forEach(w2ID -> {
//                        Map<WaAS_W3ID, WaAS_W3Record> w3_2 = new HashMap<>();
//                        cr.w3Recs.put(w2ID, w3_2);
//                        Set<WaAS_W3ID> CASEW3s = w2_To_w3.get(w2ID);
//                        CASEW3s.stream().forEach(w3ID -> {
//                            WaAS_W3Record w3rec = hs.get(w3ID);
//                            w3_2.put(w3ID, w3rec);
//                        });
//                    });
//                }
//            });
            env.logEndTag(m2);
            // Add person records.
            m2 = "Add person records";
            env.logStartTag(m2);
            Path f = sW3.cFs.get(cID);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.readLine(); // skip header
            String line = br.readLine();
            int ln = 0;
            int i = 0;
            while (line != null) {
                try {
                    WaAS_W3PRecord p = new WaAS_W3PRecord(new WaAS_RecordID(i), line);
                    i++;
                    line = br.readLine();
                    //WaAS_W1ID w1IDCheck = new WaAS_W1ID(p.getCASEW1());
                    //WaAS_W1ID w1IDCheck = we.data.CASEW1_To_w1.get(p.getCASEW1());
                    //WaAS_W2ID w2IDCheck = new WaAS_W2ID(p.getCASEW2());
                    //WaAS_W2ID w2IDCheck = we.data.CASEW2_To_w2.get(p.getCASEW2());
                    //WaAS_W3ID w3ID = new WaAS_W3ID(rec.getCASEW3());
                    WaAS_W3ID w3ID = we.data.CASEW3_To_w3.get(p.getCASEW3());
                    WaAS_W2ID w2ID = w3_To_w2.get(w3ID);
                    WaAS_W1ID w1ID = w2_To_w1.get(w2ID);
                    //printCheck(we.W3, w2IDCheck, w2ID, w2_To_w3);
                    if (s.contains(w1ID)) {
                        WaAS_CombinedRecord cr = (WaAS_CombinedRecord) c.data.get(w1ID);
                        if (cr == null) {
                            env.log("No combined record for CASEW1 " + w1ID + "! "
                                    + "This may be a data error, or this person may "
                                    + "have moved from one hhold to another?");
                        } else {
                            Set<WaAS_W2ID> w2IDs = w1_To_w2.get(w1ID);
                            w2IDs.stream().forEach(k2 -> {
                                Set<WaAS_W3ID> w3IDs = w2_To_w3.get(w2ID);
                                w3IDs.stream().forEach(k3 -> {
                                    WaAS_W3Record w3rec = cr.w3Recs.get(k2).get(k3);
                                    if (w3rec == null) {
                                        w3rec = new WaAS_W3Record(k3);
                                        env.log("Adding people, but there is no hhold "
                                                + "record for " + w3ID + "!");
                                    }
                                    w3rec.getPrs().add(p);
                                });
                            });
                        }
                    }
                } catch (Exception ex) {
                    we.logLineNotLoading(ex, ln);
                }
                ln++;
            }
//// The problem with using streams is that it is not possible that way to set c to null and if that is not done there is a memory leak!
////            br.lines().skip(1).forEach(line -> {
////                WaAS_W3PRecord p = new WaAS_W3PRecord(line);
////                //WaAS_W1ID w1IDCheck = new WaAS_W1ID(p.getCASEW1());
////                //WaAS_W1ID w1IDCheck = we.data.CASEW1_To_w1.get(p.getCASEW1());
////                //WaAS_W2ID w2IDCheck = new WaAS_W2ID(p.getCASEW2());
////                //WaAS_W2ID w2IDCheck = we.data.CASEW2_To_w2.get(p.getCASEW2());
////                //WaAS_W3ID w3ID = new WaAS_W3ID(rec.getCASEW3());
////                WaAS_W3ID w3ID = we.data.CASEW3_To_w3.get(p.getCASEW3());
////                WaAS_W2ID w2ID = w3_To_w2.get(w3ID);
////                WaAS_W1ID w1ID = w2_To_w1.get(w2ID);
////                //printCheck(we.W3, w2IDCheck, w2ID, w2_To_w3);
////                Map<WaAS_W1ID, WaAS_CombinedRecord> m = c.getData();
////                WaAS_CombinedRecord cr = m.get(w1ID);
////                if (cr == null) {
////                    env.log("No combined record for CASEW1 " + w1ID + "! "
////                            + "This may be a data error, or this person may "
////                            + "have moved from one hhold to another?");
////                } else {
////                    Set<WaAS_W2ID> w2IDs = w1_To_w2.get(w1ID);
////                    w2IDs.stream().forEach(k2 -> {
////                        Set<WaAS_W3ID> w3IDs = w2_To_w3.get(w2ID);
////                        w3IDs.stream().forEach(k3 -> {
////                            WaAS_W3Record w3rec = cr.w3Recs.get(k2).get(k3);
////                            if (w3rec == null) {
////                                w3rec = new WaAS_W3Record(we, k3);
////                                env.log("Adding people, but there is no hhold "
////                                        + "record for " + w3ID + "!");
////                            }
////                            w3rec.getPrs().add(p);
////                        });
////                    });
////                }
////            });
            we.de.logEndTagMem(m2);
            // Close br
            io.closeBufferedReader(br);
            // Cache and clear collection
            we.data.cacheSubsetCollection(cID, c);
            we.data.clearCollection(cID);
            c = null; // Free memory!
            System.gc();
            we.de.logEndTagMem(m1);
            //});
        }
        we.de.logEndTagMem(m0);
        return sW3;
    }

    /**
     * Merge Person and Household Data for Wave 4.
     *
     * @param type
     * @param pH personHandler
     * @param sW3
     * @param sW1
     * @param w1_To_w2
     * @param w2_To_w1
     * @param w2_To_w3
     * @param w3_To_w2
     * @param w3_To_w4
     * @param w4_To_w3
     * @return
     * @throws java.io.IOException If encountered.
     * @throws java.lang.ClassNotFoundException If encountered.
     */
    public WaAS_DataSubsetW4 mergePersonAndHouseholdDataIntoCollectionsW4(
            String type, WaAS_PERSON_Handler pH,
            WaAS_DataSubsetW3 sW3, WaAS_DataSubsetW1 sW1,
            Map<WaAS_W1ID, Set<WaAS_W2ID>> w1_To_w2,
            Map<WaAS_W2ID, WaAS_W1ID> w2_To_w1,
            Map<WaAS_W2ID, Set<WaAS_W3ID>> w2_To_w3,
            Map<WaAS_W3ID, WaAS_W2ID> w3_To_w2,
            Map<WaAS_W3ID, Set<WaAS_W4ID>> w3_To_w4,
            Map<WaAS_W4ID, WaAS_W3ID> w4_To_w3) 
            throws IOException, ClassNotFoundException {
        String m0 = "mergePersonAndHouseholdDataIntoCollectionsW4";
        env.logStartTag(m0);
        Map<WaAS_W4ID, WaAS_W4Record> hs;
        if (type.equalsIgnoreCase(WaAS_Strings.s__In_w1w2w3w4w5)) {
            hs = we.hh.loadCachedSubsetW4(type);
        } else if (type.equalsIgnoreCase(WaAS_Strings.s_Paired)) {
            hs = we.hh.loadCachedSubset2W4(WaAS_Strings.s__In_ + WaAS_Strings.s_w5);
        } else {
            env.log("Unrecognised type " + type);
            hs = null;
        }
        WaAS_DataSubsetW4 dataSubsetW4 = pH.loadDataSubsetW4(sW3,
                sW1.w1_To_c, w2_To_w1, w3_To_w2, w4_To_w3);
        dataSubsetW4.cFs.keySet().stream().forEach(cID -> {
                String m1 = "Collection ID " + cID;
                env.logStartTag(m1);
                env.log("TotalFreeMemory " + we.getTotalFreeMemory());
                WaAS_Collection c = we.data.getCollection(cID);
                // Add hhold records.
                String m2 = "Add hhold records";
                env.logStartTag(m2);
                Set<WaAS_W1ID> s = sW1.c_To_w1.get(cID);
                // The following way not using streams works!
                Iterator<WaAS_W1ID> ite = s.iterator();
                while (ite.hasNext()) {
                    WaAS_W1ID w1ID = ite.next();
                    we.data.w1_To_c.put(w1ID, cID);
                    WaAS_CombinedRecord cr = (WaAS_CombinedRecord) c.data.get(w1ID);
                    if (cr == null) {
                        env.log("No combined record for CASEW1 " + w1ID + "! "
                                + "This may be a data error?");
                    } else {
                        Set<WaAS_W2ID> w2IDs = w1_To_w2.get(w1ID);
                        if (w2IDs == null) {
                            env.log("w2IDs = null for w1ID " + w1ID);
                        } else {
                            w2IDs.stream().forEach(w2ID -> {
                                Map<WaAS_W3ID, Map<WaAS_W4ID, WaAS_W4Record>> w4_2 = new HashMap<>();
                                cr.w4Recs.put(w2ID, w4_2);
                                Set<WaAS_W3ID> w3IDs = w2_To_w3.get(w2ID);
                                if (w3IDs == null) {
                                    env.log("w3IDs = null for w2ID " + w2ID);
                                } else {
                                    w3IDs.stream().forEach(w3ID -> {
                                        Map<WaAS_W4ID, WaAS_W4Record> w4_3 = new HashMap<>();
                                        w4_2.put(w3ID, w4_3);
                                        Set<WaAS_W4ID> w4IDs = w3_To_w4.get(w3ID);
                                        if (w4IDs == null) {
                                            env.log("w4IDs = null for w3ID " + w3ID);
                                        } else {
                                            w4IDs.stream().forEach(w4ID -> {
                                                if (w4ID == null) {
                                                    env.log("w4ID = null");
                                                } else {
                                                    WaAS_W4Record w4rec = hs.get(w4ID);
                                                    w4_3.put(w4ID, w4rec);
                                                }
                                            });
                                        }
                                    });
                                }
                            });
                        }
                    }
                }
// The problem with using streams is that it is not possible that way to set c to null and if that is not done there is a memory leak!
//            w1IDs.stream().forEach(w1ID -> {
//                we.data.w1_To_c.put(w1ID, cID);
//                Map<WaAS_W1ID, WaAS_CombinedRecord> m = c.getData();
//                WaAS_CombinedRecord cr = m.get(w1ID);
//                if (cr == null) {
//                    env.log("No combined record for CASEW1 " + w1ID + "! "
//                            + "This may be a data error?");
//                } else {
//                    Set<WaAS_W2ID> w2IDs = w1_To_w2.get(w1ID);
//                    w2IDs.stream().forEach(w2ID -> {
//                        Map<WaAS_W3ID, Map<WaAS_W4ID, WaAS_W4Record>> w4_2 = new HashMap<>();
//                        cr.w4Recs.put(w2ID, w4_2);
//                        Set<WaAS_W3ID> w3IDs = w2_To_w3.get(w2ID);
//                        w3IDs.stream().forEach(w3ID -> {
//                            Map<WaAS_W4ID, WaAS_W4Record> w4_3 = new HashMap<>();
//                            w4_2.put(w3ID, w4_3);
//                            Set<WaAS_W4ID> w4IDs = w3_To_w4.get(w3ID);
//                            w4IDs.stream().forEach(w4ID -> {
//                                WaAS_W4Record w4rec = hs.get(w4ID);
//                                w4_3.put(w4ID, w4rec);
//                            });
//                        });
//                    });
//                }
//            });
                env.logEndTag(m2);
// Add person records.
                m2 = "Add person records";
                env.logStartTag(m2);
                Path f = dataSubsetW4.cFs.get(cID);
                try (BufferedReader br = Generic_IO.getBufferedReader(f)) {
                    br.readLine(); // skip header
                    String line = br.readLine();
                    int ln = 0;
                    int i = 0;
                    while (line != null) {
                        try {
                            WaAS_W4PRecord p = new WaAS_W4PRecord(new WaAS_RecordID(i), line);
                            i++;
                            line = br.readLine();
                            //WaAS_W1ID w1IDCheck = new WaAS_W1ID(p.getCASEW1());
                            //WaAS_W1ID w1IDCheck = we.data.CASEW1_To_w1.get(p.getCASEW1());
                            //WaAS_W2ID w2IDCheck = new WaAS_W2ID(p.getCASEW2());
                            //WaAS_W2ID w2IDCheck = we.data.CASEW2_To_w2.get(p.getCASEW2());
                            //WaAS_W3ID w3IDCheck = new WaAS_W3ID(p.getCASEW3());
                            //WaAS_W3ID w3IDCheck = we.data.CASEW3_To_w3.get(p.getCASEW3());
                            //WaAS_W4ID w4ID = new WaAS_W4ID(rec.getCASEW4());
                            WaAS_W4ID w4ID = we.data.CASEW4_To_w4.get(p.getCASEW4());
                            WaAS_W3ID w3ID = w4_To_w3.get(w4ID);
                            WaAS_W2ID w2ID = w3_To_w2.get(w3ID);
                            WaAS_W1ID w1ID = w2_To_w1.get(w2ID);
                            //printCheck(W2, w1IDCheck, w1ID, w1IDToW2ID);
                            //printCheck(W3, w2IDCheck, w2ID, w2IDToW3ID);
                            //printCheck(W4, w3IDCheck, w3ID, w3_To_w4);
                            if (s.contains(w1ID)) {
                                WaAS_CombinedRecord cr = (WaAS_CombinedRecord) c.data.get(w1ID);
                                if (cr == null) {
                                    env.log("No combined record for CASEW1 " + w1ID + "! "
                                            + "This may be a data error, or this person may "
                                            + "have moved from one hhold to another?");
                                } else {
                                    Set<WaAS_W2ID> w2IDs = w1_To_w2.get(w1ID);
                                    w2IDs.stream().forEach(k2 -> {
                                        Set<WaAS_W3ID> w3IDs = w2_To_w3.get(w2ID);
                                        w3IDs.stream().forEach(k3 -> {
                                            Set<WaAS_W4ID> w4IDs = w3_To_w4.get(w3ID);
                                            w4IDs.stream().forEach(k4 -> {
                                                Map<WaAS_W3ID, Map<WaAS_W4ID, WaAS_W4Record>> w4_2;
                                                w4_2 = cr.w4Recs.get(k2);
                                                if (w4_2 == null) {
                                                    w4_2 = new HashMap<>();
                                                    cr.w4Recs.put(k2, w4_2);
                                                }
                                                Map<WaAS_W4ID, WaAS_W4Record> w4_3 = w4_2.get(k3);
                                                if (w4_3 == null) {
                                                    w4_3 = new HashMap<>();
                                                    w4_2.put(k3, w4_3);
                                                }
                                                WaAS_W4Record w4rec = w4_3.get(k4);
                                                if (w4rec == null) {
                                                    w4rec = new WaAS_W4Record(k4);
                                                    env.log("Adding people, but there is no "
                                                            + "hhold record for " + w4ID + "!");
                                                }
                                                w4rec.getPrs().add(p);
                                            });
                                        });
                                    });
                                }
                            }
                        } catch (Exception ex) {
                            we.logLineNotLoading(ex, ln);
                        }
                        ln++;
                    }
                } catch (IOException ex) {
                    ex.printStackTrace(System.err);
                    env.log(ex.getMessage());
                    System.exit(0);
                }
//// The problem with using streams is that it is not possible that way to set c to null and if that is not done there is a memory leak!
////            br.lines().skip(1).forEach(line -> {
////                WaAS_W4PRecord p = new WaAS_W4PRecord(line);
////                //WaAS_W1ID w1IDCheck = new WaAS_W1ID(p.getCASEW1());
////                //WaAS_W1ID w1IDCheck = we.data.CASEW1_To_w1.get(p.getCASEW1());
////                //WaAS_W2ID w2IDCheck = new WaAS_W2ID(p.getCASEW2());
////                //WaAS_W2ID w2IDCheck = we.data.CASEW2_To_w2.get(p.getCASEW2());
////                //WaAS_W3ID w3IDCheck = new WaAS_W3ID(p.getCASEW3());
////                //WaAS_W3ID w3IDCheck = we.data.CASEW3_To_w3.get(p.getCASEW3());
////                //WaAS_W4ID w4ID = new WaAS_W4ID(rec.getCASEW4());
////                WaAS_W4ID w4ID = we.data.CASEW4_To_w4.get(p.getCASEW4());
////                WaAS_W3ID w3ID = w4_To_w3.get(w4ID);
////                WaAS_W2ID w2ID = w3_To_w2.get(w3ID);
////                WaAS_W1ID w1ID = w2_To_w1.get(w2ID);
////                //printCheck(W2, w1IDCheck, w1ID, w1IDToW2ID);
////                //printCheck(W3, w2IDCheck, w2ID, w2IDToW3ID);
////                //printCheck(W4, w3IDCheck, w3ID, w3_To_w4);
////                Map<WaAS_W1ID, WaAS_CombinedRecord> m = c.getData();
////                WaAS_CombinedRecord cr = m.get(w1ID);
////                if (cr == null) {
////                    env.log("No combined record for CASEW1 " + w1ID + "! "
////                            + "This may be a data error, or this person may "
////                            + "have moved from one hhold to another?");
////                } else {
////                    Set<WaAS_W2ID> w2IDs = w1_To_w2.get(w1ID);
////                    w2IDs.stream().forEach(k2 -> {
////                        Set<WaAS_W3ID> w3IDs = w2_To_w3.get(w2ID);
////                        w3IDs.stream().forEach(k3 -> {
////                            Set<WaAS_W4ID> w4IDs = w3_To_w4.get(w3ID);
////                            w4IDs.stream().forEach(k4 -> {
////                                Map<WaAS_W3ID, Map<WaAS_W4ID, WaAS_W4Record>> w4_2;
////                                w4_2 = cr.w4Recs.get(k2);
////                                if (w4_2 == null) {
////                                    w4_2 = new HashMap<>();
////                                    cr.w4Recs.put(k2, w4_2);
////                                }
////                                Map<WaAS_W4ID, WaAS_W4Record> w4_3 = w4_2.get(k3);
////                                if (w4_3 == null) {
////                                    w4_3 = new HashMap<>();
////                                    w4_2.put(k3, w4_3);
////                                }
////                                WaAS_W4Record w4rec = w4_3.get(k4);
////                                if (w4rec == null) {
////                                    w4rec = new WaAS_W4Record(we, k4);
////                                    env.log("Adding people, but there is no "
////                                            + "hhold record for " + w4ID + "!");
////                                }
////                                w4rec.getPrs().add(p);
////                            });
////                        });
////                    });
////                }
////            });
                env.logEndTag(m2);
                try {
                    // Cache and clear collection
                    we.data.cacheSubsetCollection(cID, c);
                } catch (IOException ex) {
                    Logger.getLogger(WaAS_Main.class.getName()).log(Level.SEVERE, null, ex);
                }
                we.data.clearCollection(cID);
                c = null; // Free memory!
                System.gc();
                env.logEndTag(m1);
        });
        env.logEndTag(m0);
        return dataSubsetW4;
    }

    /**
     * Merge Person and Household Data for Wave 5.
     *
     * @param type
     * @param pH
     * @param sW4
     * @param sW1
     * @param w1_To_w2
     * @param w2_To_w1
     * @param w2_To_w3
     * @param w3_To_w2
     * @param w3_To_w4
     * @param w4_To_w3
     * @param w4_To_w5
     * @param w5_To_w4
     * @return
     * @throws java.io.IOException If encountered.
     * @throws java.lang.ClassNotFoundException If encountered.
     */
    public WaAS_DataSubsetW5 mergePersonAndHouseholdDataIntoCollectionsW5(
            String type, WaAS_PERSON_Handler pH,
            WaAS_DataSubsetW4 sW4, WaAS_DataSubsetW1 sW1,
            Map<WaAS_W1ID, Set<WaAS_W2ID>> w1_To_w2,
            Map<WaAS_W2ID, WaAS_W1ID> w2_To_w1,
            Map<WaAS_W2ID, Set<WaAS_W3ID>> w2_To_w3,
            Map<WaAS_W3ID, WaAS_W2ID> w3_To_w2,
            Map<WaAS_W3ID, Set<WaAS_W4ID>> w3_To_w4,
            Map<WaAS_W4ID, WaAS_W3ID> w4_To_w3,
            Map<WaAS_W4ID, Set<WaAS_W5ID>> w4_To_w5,
            Map<WaAS_W5ID, WaAS_W4ID> w5_To_w4) throws IOException, 
            ClassNotFoundException {
        // Wave 5
        String m0 = "mergePersonAndHouseholdDataIntoCollectionsW5";
        env.logStartTag(m0);
        Map<WaAS_W5ID, WaAS_W5Record> hs;
        if (type.equalsIgnoreCase(WaAS_Strings.s__In_w1w2w3w4w5)) {
            hs = we.hh.loadCachedSubsetW5(type);
        } else if (type.equalsIgnoreCase(WaAS_Strings.s_Paired)) {
            hs = we.hh.loadCachedSubset2W5(WaAS_Strings.s__In_ + WaAS_Strings.s_w4); // It may seem eems wierd to be W4 not W5, but probably right!?
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
        WaAS_DataSubsetW5 sW5 = pH.loadDataSubsetW5(sW4, sW1.w1_To_c, w2_To_w1, w3_To_w2, w4_To_w3, w5_To_w4);
        sW5.cFs.keySet().stream().forEach(cID -> {
            try {
                String m1 = "Collection ID " + cID;
                env.logStartTag(m1);
                env.log("TotalFreeMemory " + we.getTotalFreeMemory());
                WaAS_Collection c = we.data.getCollection(cID);
                // Add hhold records.
                String m2 = "Add hhold records";
                env.logStartTag(m2);
                Set<WaAS_W1ID> s = sW1.c_To_w1.get(cID);
                // The following way not using streams works!
                Iterator<WaAS_W1ID> ite = s.iterator();
                while (ite.hasNext()) {
                    WaAS_W1ID w1ID = ite.next();
                    we.data.w1_To_c.put(w1ID, cID);
                    WaAS_CombinedRecord cr = (WaAS_CombinedRecord) c.data.get(w1ID);
                    if (cr == null) {
                        env.log("No combined record for CASEW1 " + w1ID + "! "
                                + "This may be a data error?");
                    } else {
                        Set<WaAS_W2ID> w2IDs = w1_To_w2.get(w1ID);
                        if (w2IDs == null) {
                            env.log("w2IDs = null for w1ID " + w1ID);
                        } else {
                            w2IDs.stream().forEach(w2ID -> {
                                Map<WaAS_W3ID, Map<WaAS_W4ID, Map<WaAS_W5ID, WaAS_W5Record>>> w5_2;
                                w5_2 = new HashMap<>();
                                cr.w5Recs.put(w2ID, w5_2);
                                Set<WaAS_W3ID> w3IDs = w2_To_w3.get(w2ID);
                                if (w3IDs == null) {
                                    env.log("w3IDs = null for w2ID " + w2ID);
                                } else {
                                    w3IDs.stream().forEach(w3ID -> {
                                        Map<WaAS_W4ID, Map<WaAS_W5ID, WaAS_W5Record>> w5_3 = new HashMap<>();
                                        w5_2.put(w3ID, w5_3);
                                        Set<WaAS_W4ID> w4IDs = w3_To_w4.get(w3ID);
                                        if (w4IDs == null) {
                                            env.log("w4IDs = null for w3ID " + w3ID);
                                        } else {
                                            w4IDs.stream().forEach(w4ID -> {
                                                Map<WaAS_W5ID, WaAS_W5Record> w5_4 = new HashMap<>();
                                                w5_3.put(w4ID, w5_4);
                                                Set<WaAS_W5ID> w5IDs = w4_To_w5.get(w4ID);
                                                if (w5IDs == null) {
                                                    env.log("w5IDs = null for w4ID " + w4ID);
                                                } else {
                                                    w5IDs.stream().forEach(w5ID -> {
                                                        if (w5ID == null) {
                                                            env.log("w5ID = null");
                                                        } else {
                                                            WaAS_W5Record w5rec = hs.get(w5ID);
                                                            w5_4.put(w5ID, w5rec);
                                                        }
                                                    });
                                                }
                                            });
                                        }
                                    });
                                }
                            });
                        }
                    }
                }
// The problem with using streams is that it is not possible that way to set c to null and if that is not done there is a memory leak!
//            s.stream().forEach(w1ID -> {
//                we.data.w1_To_c.put(w1ID, cID);
//                Map<WaAS_W1ID, WaAS_CombinedRecord> m = c.getData();
//                WaAS_CombinedRecord cr = m.get(w1ID);
//                if (cr == null) {
//                    env.log("No combined record for CASEW1 " + w1ID + "! "
//                            + "This may be a data error?");
//                } else {
//                    Set<WaAS_W2ID> w2IDs = w1_To_w2.get(w1ID);
//                    w2IDs.stream().forEach(w2ID -> {
//                        Map<WaAS_W3ID, Map<WaAS_W4ID, Map<WaAS_W5ID, WaAS_W5Record>>> w5_2;
//                        w5_2 = new HashMap<>();
//                        cr.w5Recs.put(w2ID, w5_2);
//                        Set<WaAS_W3ID> w3IDs = w2_To_w3.get(w2ID);
//                        w3IDs.stream().forEach(w3ID -> {
//                            Map<WaAS_W4ID, Map<WaAS_W5ID, WaAS_W5Record>> w5_3 = new HashMap<>();
//                            w5_2.put(w3ID, w5_3);
//                            Set<WaAS_W4ID> w4IDs = w3_To_w4.get(w3ID);
//                            w4IDs.stream().forEach(w4ID -> {
//                                Map<WaAS_W5ID, WaAS_W5Record> w5_4 = new HashMap<>();
//                                w5_3.put(w4ID, w5_4);
//                                Set<WaAS_W5ID> w5IDs = w4_To_w5.get(w4ID);
//                                w5IDs.stream().forEach(w5ID -> {
//                                    if (w5ID != null) {
//                                        WaAS_W5Record w5rec = hs.get(w5ID);
//                                        w5_4.put(w5ID, w5rec);
//                                    }
//                                });
//                            });
//                        });
//                    });
//                }
//            });
                env.logEndTag(m2);
// Add person records.
                m2 = "Add person records";
                env.logStartTag(m2);
                Path f = sW5.cFs.get(cID);
                try (BufferedReader br = Generic_IO.getBufferedReader(f)) {
                    br.readLine(); // skip header
                    String line = br.readLine();
                    int ln = 0;
                    int i = 0;
                    while (line != null) {
                        try {
                            WaAS_W5PRecord p = new WaAS_W5PRecord(new WaAS_RecordID(i), line);
                            i++;
                            line = br.readLine();
//                WaAS_W1ID w1IDCheck = we.data.CASEW1_To_w1.get(p.getCASEW1());
//                WaAS_W2ID w2IDCheck = we.data.CASEW2_To_w2.get(p.getCASEW2());
//                WaAS_W3ID w3IDCheck = we.data.CASEW3_To_w3.get(p.getCASEW3());
//                WaAS_W4ID w4IDCheck = we.data.CASEW4_To_w4.get(p.getCASEW4());
                            WaAS_W5ID w5ID = we.data.CASEW5_To_w5.get(p.getCASEW5());
                            WaAS_W4ID w4ID = w5_To_w4.get(w5ID);
                            if (w4ID == null) {
                                env.log("CASEW5 " + w5ID + " is not in CASEW5ToCASEW4 lookup");
                            } else {
                                WaAS_W3ID w3ID = w4_To_w3.get(w4ID);
                                WaAS_W2ID w2ID = w3_To_w2.get(w3ID);
                                WaAS_W1ID w1ID = w2_To_w1.get(w2ID);
//                    printCheck(we.W2, w1IDCheck, w1ID, w1_To_w2);
//                    printCheck(we.W3, w2IDCheck, w2ID, w2_To_w3);
//                    printCheck(we.W4, w3IDCheck, w3ID, w3_To_w4);
//                    printCheck(we.W5, w4IDCheck, w4ID, w4_To_w5);
                                if (s.contains(w1ID)) {
                                    WaAS_CombinedRecord cr = (WaAS_CombinedRecord) c.data.get(w1ID);
                                    if (cr == null) {
                                        env.log("No combined record for CASEW1 " + w1ID + "! "
                                                + "This may be a data error, or this person may "
                                                + "have moved from one hhold to another?");
                                    } else {
                                        Set<WaAS_W2ID> w2IDs = w1_To_w2.get(w1ID);
                                        w2IDs.stream().forEach(k2 -> {
                                            Set<WaAS_W3ID> w3IDs = w2_To_w3.get(w2ID);
                                            w3IDs.stream().forEach(k3 -> {
                                                Set<WaAS_W4ID> w4IDs = w3_To_w4.get(w3ID);
                                                w4IDs.stream().forEach(k4 -> {
                                                    Set<WaAS_W5ID> w5IDs = w4_To_w5.get(w4ID);
                                                    w5IDs.stream().forEach(k5 -> {
                                                        Map<WaAS_W3ID, Map<WaAS_W4ID, Map<WaAS_W5ID, WaAS_W5Record>>> w5_2;
                                                        w5_2 = cr.w5Recs.get(k2);
                                                        if (w5_2 == null) {
                                                            w5_2 = new HashMap<>();
                                                            cr.w5Recs.put(k2, w5_2);
                                                        }
                                                        Map<WaAS_W4ID, Map<WaAS_W5ID, WaAS_W5Record>> w5_3 = w5_2.get(k3);
                                                        if (w5_3 == null) {
                                                            w5_3 = new HashMap<>();
                                                            w5_2.put(k3, w5_3);
                                                        }
                                                        Map<WaAS_W5ID, WaAS_W5Record> w5_4 = w5_3.get(k4);
                                                        if (w5_4 == null) {
                                                            w5_4 = new HashMap<>();
                                                            w5_3.put(k4, w5_4);
                                                        }
                                                        WaAS_W5Record w5rec;
                                                        w5rec = cr.w5Recs.get(k2).get(k3).get(k4).get(k5);
                                                        if (w5rec == null) {
                                                            w5rec = new WaAS_W5Record(k5);
                                                            env.log("Adding people, but there "
                                                                    + "is no hhold record for "
                                                                    + w5ID + "!");
                                                        }
                                                        w5rec.getPrs().add(p);
                                                    });
                                                });
                                            });
                                        });
                                    }
                                }
                            }
                        } catch (Exception ex) {
                            we.logLineNotLoading(ex, ln);
                        }
                        ln++;
                    }
                } catch (IOException ex) {
                    ex.printStackTrace(System.err);
                    env.log(ex.getMessage());
                    System.exit(0);
                }
//// The problem with using streams is that it is not possible that way to set c to null and if that is not done there is a memory leak!
////            br.lines().skip(1).forEach(line -> {
////                WaAS_W5PRecord p = new WaAS_W5PRecord(line);
//////                WaAS_W1ID w1IDCheck = we.data.CASEW1_To_w1.get(p.getCASEW1());
//////                WaAS_W2ID w2IDCheck = we.data.CASEW2_To_w2.get(p.getCASEW2());
//////                WaAS_W3ID w3IDCheck = we.data.CASEW3_To_w3.get(p.getCASEW3());
//////                WaAS_W4ID w4IDCheck = we.data.CASEW4_To_w4.get(p.getCASEW4());
////                WaAS_W5ID w5ID = we.data.CASEW5_To_w5.get(p.getCASEW5());
////                WaAS_W4ID w4ID = w5_To_w4.get(w5ID);
////                if (w4ID == null) {
////                    env.log("CASEW5 " + w5ID + " is not in CASEW5ToCASEW4 lookup");
////                } else {
////                    WaAS_W3ID w3ID = w4_To_w3.get(w4ID);
////                    WaAS_W2ID w2ID = w3_To_w2.get(w3ID);
////                    WaAS_W1ID w1ID = w2_To_w1.get(w2ID);
//////                    printCheck(we.W2, w1IDCheck, w1ID, w1_To_w2);
//////                    printCheck(we.W3, w2IDCheck, w2ID, w2_To_w3);
//////                    printCheck(we.W4, w3IDCheck, w3ID, w3_To_w4);
//////                    printCheck(we.W5, w4IDCheck, w4ID, w4_To_w5);
////                    Map<WaAS_W1ID, WaAS_CombinedRecord> m = c.getData();
////                    WaAS_CombinedRecord cr = m.get(w1ID);
////                    if (cr == null) {
////                        env.log("No combined record for CASEW1 " + w1ID + "! "
////                                + "This may be a data error, or this person may "
////                                + "have moved from one hhold to another?");
////                    } else {
////                        Set<WaAS_W2ID> w2IDs = w1_To_w2.get(w1ID);
////                        w2IDs.stream().forEach(k2 -> {
////                            Set<WaAS_W3ID> w3IDs = w2_To_w3.get(w2ID);
////                            w3IDs.stream().forEach(k3 -> {
////                                Set<WaAS_W4ID> w4IDs = w3_To_w4.get(w3ID);
////                                w4IDs.stream().forEach(k4 -> {
////                                    Set<WaAS_W5ID> w5IDs = w4_To_w5.get(w4ID);
////                                    w5IDs.stream().forEach(k5 -> {
////                                        Map<WaAS_W3ID, Map<WaAS_W4ID, Map<WaAS_W5ID, WaAS_W5Record>>> w5_2;
////                                        w5_2 = cr.w5Recs.get(k2);
////                                        if (w5_2 == null) {
////                                            w5_2 = new HashMap<>();
////                                            cr.w5Recs.put(k2, w5_2);
////                                        }
////                                        Map<WaAS_W4ID, Map<WaAS_W5ID, WaAS_W5Record>> w5_3 = w5_2.get(k3);
////                                        if (w5_3 == null) {
////                                            w5_3 = new HashMap<>();
////                                            w5_2.put(k3, w5_3);
////                                        }
////                                        Map<WaAS_W5ID, WaAS_W5Record> w5_4 = w5_3.get(k4);
////                                        if (w5_4 == null) {
////                                            w5_4 = new HashMap<>();
////                                            w5_3.put(k4, w5_4);
////                                        }
////                                        WaAS_W5Record w5rec;
////                                        w5rec = cr.w5Recs.get(k2).get(k3).get(k4).get(k5);
////                                        if (w5rec == null) {
////                                            w5rec = new WaAS_W5Record(we, k5);
////                                            env.log("Adding people, but there "
////                                                    + "is no hhold record for "
////                                                    + w5ID + "!");
////                                        }
////                                        w5rec.getPrs().add(p);
////                                    });
////                                });
////                            });
////                        });
////                    }
////                }
////            });
                env.logEndTag(m2);
// Cache and clear collection
                we.data.cacheSubsetCollection(cID, c);
                we.data.clearCollection(cID);
                c = null; // Free memory!
                System.gc();
                env.logEndTag(m1);
            } catch (IOException ex) {
                Logger.getLogger(WaAS_Main.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        );
        env.logEndTag(m0);
        return sW5;
    }

    /**
     *
     * @param subset Contains a subset of CASEW1 IDs.
     */
    public void initDataSimple(Set<WaAS_W1ID> subset) {
        String m = "initDataSimple";
        env.logStartTag(m);
        //Iterator<? extends Data_CollectionID> ite = we.data.getData().keySet().iterator();
        Iterator<? super Data_CollectionID> ite = we.data.getData().keySet().iterator();
        while (ite.hasNext()) {
            try {
                WaAS_CollectionID cID = (WaAS_CollectionID) ite.next();
                WaAS_Collection c = we.data.getCollection(cID);
                WaAS_Collection cs = new WaAS_Collection(cID);
                we.data.dataSimple.put(cID, cs);
                Iterator<? super Data_RecordID> ite2 = c.data.keySet().iterator();
                while (ite2.hasNext()) {
                    WaAS_W1ID w1ID = (WaAS_W1ID) ite2.next();
                    if (subset.contains(w1ID)) {
                        WaAS_CombinedRecord cr = (WaAS_CombinedRecord) c.data.get(w1ID);
                        WaAS_CombinedRecordSimple crs = new WaAS_CombinedRecordSimple(cr.w1Rec);
                        crs.w2Rec = cr.w2Recs.values().stream().findFirst().get();
                        crs.w3Rec = cr.w3Recs.values().stream().findFirst().get()
                                .values().stream().findFirst().get();
                        crs.w4Rec = cr.w4Recs.values().stream().findFirst().get()
                                .values().stream().findFirst().get()
                                .values().stream().findFirst().get();
                        crs.w5Rec = cr.w5Recs.values().stream().findFirst().get()
                                .values().stream().findFirst().get()
                                .values().stream().findFirst().get()
                                .values().stream().findFirst().get();
                        cs.data.put(w1ID, crs);
                    }
                }
                we.data.clearCollection(cID);
                we.data.cacheSubsetCollectionSimple(cID, cs);
                we.data.clearCollectionSimple(cID);
            } catch (IOException ex) {
                Logger.getLogger(WaAS_Main.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        env.logEndTag(m);
    }

    boolean doLoadHouseholdsAndIndividualsInAllWaves = false;
    boolean doLoadHouseholdsInPairedWaves = false;
    boolean doLoadAllHouseholdsRecords = false;
    boolean doSubset = false;
}
