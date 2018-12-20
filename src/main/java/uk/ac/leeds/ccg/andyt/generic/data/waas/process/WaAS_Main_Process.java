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
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.TreeSet;
import uk.ac.leeds.ccg.andyt.generic.io.Generic_IO;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Strings;
import uk.ac.leeds.ccg.andyt.generic.data.waas.io.WaAS_Files;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Object;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Collection;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Combined_Record;
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
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave1Or2Or3Or4Or5_PERSON_Record;
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
    protected final WaAS_Strings Strings;
    protected final WaAS_Files Files;

    // For logging.
    File logF;
    public static transient PrintWriter logPW;
    File logF0;
    public static transient PrintWriter logPW0;

    public WaAS_Main_Process(WaAS_Environment env) {
        super(env);
        data = env.data;
        Strings = env.Strings;
        Files = env.Files;
    }

    public static void main(String[] args) {
        WaAS_Main_Process p;
        WaAS_Environment env;
        env = new WaAS_Environment();
        p = new WaAS_Main_Process(env);
        p.Files.setDataDirectory(new File(System.getProperty("user.dir"), "data"));
        // Main switches
        //p.doJavaCodeGeneration = true;
        p.doLoadDataIntoCaches = true; // rename/reuse just left here for convenience...
        p.run();
    }

    public void run() {
        logF0 = new File(Files.getOutputDataDir(Strings), "log0.txt");
        logPW0 = Generic_IO.getPrintWriter(logF0, false); // Overwrite log file.

        if (doJavaCodeGeneration) {
            runJavaCodeGeneration();
        }

        File indir;
        File outdir;
        File generateddir;
        WaAS_HHOLD_Handler hholdHandler;

        indir = Files.getWaASInputDir();
        generateddir = Files.getGeneratedWaASDir();
        outdir = new File(generateddir, "Subsets");
        outdir.mkdirs();
        hholdHandler = new WaAS_HHOLD_Handler(Env.Files, Env.Strings, indir);

        int chunkSize;
        chunkSize = 256; //1024; 512; 256;
        //doDataProcessingStep1New(indir, outdir, hholdHandler);
        //doDataProcessingStep2(indir, outdir, hholdHandler, chunkSize);
        doDataProcessingStep3(outdir);

        logPW.close();
    }

    /**
     * Go through hholds for all waves and figure which ones have not
     * significantly changed in terms of hhold composition. Having children and
     * children leaving home is fine. Anything else is perhaps an issue.
     *
     * @param outdir
     */
    public void doDataProcessingStep3(File outdir) {
        initlog(3);
        log("Number of combined records " + data.CASEW1ToCID.size());
        log("Number of collections of combined records " + data.data.size());
//        /**
//         * Calculate the number of hholds that have the same number of
//         * adults throughout.
//         */
//        long n = data.data.keySet().stream()
//                .mapToLong(cID -> {
//                    WaAS_Collection c;
//                    c = data.getCollection(cID);
//                    long nc = c.getData().keySet().stream()
//                            .mapToLong(CASEW1 -> {
//                                WaAS_Combined_Record cr;
//                                cr = c.getData().get(CASEW1);
//                                c.getData().get(CASEW1);
//                                try {
//                                    byte w1 = cr.w1Record.getHhold().getNUMADULT();
//                                    byte w2 = cr.w2Record.getHhold().getNUMADULT();
//                                    byte w3 = cr.w3Record.getHhold().getNUMADULT();
//                                    byte w4 = cr.w4Record.getHhold().getNUMADULT();
//                                    byte w5 = cr.w5Record.getHhold().getNUMADULT();
//                                    if (w1 == w2 && w2 == w3 && w3 == w4 && w4 == w5) {
//                                        return 1;
//                                    } else {
//                                        return 0;
//                                    }
//                                } catch (NullPointerException e) {
//                                    return 0;
//                                }
//                            }).sum();
//                    data.clearCollection(cID);
//                    return nc;
//                }).sum();
//        log("There are " + n + " hholds that contain the "
//                + "same number of adults throughout.");

        // For brevity/convenience.
        byte W1 = WaAS_Data.W1;
        byte W2 = WaAS_Data.W2;
        byte W3 = WaAS_Data.W3;
        byte W4 = WaAS_Data.W4;
        byte W5 = WaAS_Data.W5;
        WaAS_Wave1_HHOLD_Record w1h;
        WaAS_Wave2_HHOLD_Record w2h;
        WaAS_Wave3_HHOLD_Record w3h;
        WaAS_Wave4_HHOLD_Record w4h;
        WaAS_Wave5_HHOLD_Record w5h;

        //HashSet<Short> s = new HashSet<>();
        Iterator<Short> ite;
        Iterator<Short> ite2;
        short cID;
        short CASEW1;
        WaAS_Collection c;
        WaAS_Combined_Record cr;
        HashMap<Short, WaAS_Combined_Record> cData;
        String m;
        // Check For Household Records
        m = "Check For Household Records";
        boolean check;
        int count0;
        count0 = 0;
        int count1;
        count1 = 0;
        int count2;
        count2 = 0;
        int count3;
        count3 = 0;
        log("<" + m + ">");
        ite = data.data.keySet().iterator();
        while (ite.hasNext()) {
            cID = ite.next();
            c = data.getCollection(cID);
            cData = c.getData();
            ite2 = cData.keySet().iterator();
            while (ite2.hasNext()) {
                CASEW1 = ite2.next();
                cr = cData.get(CASEW1);
                check = process0(CASEW1, cr);
                if (check) {
                    count0++;
                }
                check = process1(CASEW1, cr);
                if (check) {
                    count1++;
                }
                check = process2(CASEW1, cr);
                if (check) {
                    count2++;
                }
                check = process3(CASEW1, cr);
                if (check) {
                    count3++;
                }
            }
            data.clearCollection(cID);
        }
        log("" + count0 + " Total hholds in all 5 waves.");
        log("" + count1 + " Total hholds that are a single hhold over all 5 waves.");
        log("" + count2 + " Total hholds that are a single hhold over all 5 waves and have same number of adults in all 5 waves.");
        log("" + count3 + " Total hholds that are a single hhold over all 5 waves and have the same basic adult household composition over all 5 waves.");
        log("</" + m + ">");

//        /**
//         * Get the IDs of hholds that have the same number of adults
//         * throughout.
//         */
//        ite = data.data.keySet().iterator();
//        while (ite.hasNext()) {
//            cID = ite.next();
//            log1("Processing collection " + cID);
//            c = data.getCollection(cID);
//            ite2 = c.getData().keySet().iterator();
//            while (ite2.hasNext()) {
//                CASEW1 = ite2.next();
//                cr = c.getData().get(CASEW1);
//                c.getData().get(CASEW1);
//                w1h = cr.w1Record.getHhold();
//                w2h = cr.w2Record.getHhold();
//                w3h = cr.w3Record.getHhold();
//                w4h = cr.w4Record.getHhold();
//                w5h = cr.w5Record.getHhold();
//                if (process0(W1, CASEW1, w1h)) {
//                    if (process0(W2, CASEW1, w2h)) {
//                        if (process0(W3, CASEW1, w3h)) {
//                            if (process0(W4, CASEW1, w4h)) {
//                                if (process0(W5, CASEW1, w5h)) {
//                                    s.add(CASEW1);
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//            data.clearCollection(cID);
//        }
//        log("Number of combined records with all hhold records "
//                + "for each wave " + s.size() + ".");
//        /**
//         * Stream through the data and calculate the total value of UK Land in
//         * Wave 1 hholds. This value is an aggregate of numerical class
//         * values.
//         */
//        long tDVLUKVAL = data.data.keySet().stream()
//                .mapToLong(cID -> {
//                    WaAS_Collection c;
//                    c = data.getCollection(cID);
//                    long cDVLUKVAL = c.getData().keySet().stream()
//                            .mapToLong(CASEW1 -> {
//                                WaAS_Combined_Record cr;
//                                cr = c.getData().get(CASEW1);
//                                int DVLUKVAL;
//                                DVLUKVAL = cr.w1Record.getHhold().getDVLUKVAL();
//                                if (DVLUKVAL == Integer.MIN_VALUE) {
//                                    DVLUKVAL = 0;
//                                }
//                                return DVLUKVAL;
//                            }).sum();
//                    data.clearCollection(cID);
//                    return cDVLUKVAL;  // Total value of UK Land in c.
//                }).sum();
//        log("Total value of UK Land in Wave 1 " + tDVLUKVAL);
        //
//        /**
//         * Stream through the data and calculate the total income in the last 12
//         * months of all individual people. This value is an aggregate of
//         * numerical class values.
//         */
//        long tFINCVB = data.data.keySet().stream()
//                .mapToLong(cID -> {
//                    WaAS_Collection c;
//                    c = data.getCollection(cID);
//                    long cFINCVB = c.getData().keySet().stream()
//                            .mapToLong(CASEW1 -> {
//                                WaAS_Combined_Record cr;
//                                cr = c.getData().get(CASEW1);
//                                long hFINCVB = cr.w1Record.getPeople().stream()
//                                        .mapToLong(p -> {
//                                            byte FINCVB;
//                                            FINCVB = p.getFINCVB();
//                                            //if (FINCVB == Byte.MIN_VALUE) {
//                                            if (FINCVB < 0) {
//                                                FINCVB = 0;
//                                            }
//                                            return FINCVB;
//                                        }).sum();
//                                return hFINCVB;
//                            }).sum();
//                    data.clearCollection(cID);
//                    return cFINCVB;  // Total income in the last 12 months.
//                }).sum();
//        log("Total income in the last 12 months " + tFINCVB);
        /**
         * The main WaAS data store. Keys are Collection IDs.
         */
        TreeMap<Integer, String> vIDToVName;
        TreeMap<String, Integer> vNameToVID;
        vIDToVName = new TreeMap<>();
        vNameToVID = new TreeMap<>();
        String sDVLUKDEBT = "DVLUKDEBT";
        addVariable(sDVLUKDEBT, vIDToVName, vNameToVID);
        String sDVLUKVAL = "DVLUKVAL";
        addVariable(sDVLUKVAL, vIDToVName, vNameToVID);

//                    WaAS_Combined_Record cr;
//                    cr = c.getData().get(CASEW1);
//                    //cr.w1Record.getHhold().getDVLUKDEBT(); // Debt on UK Land
//                    tDVLUKVAL += cr.w1Record.getHhold().getDVLUKVAL();  // Value of UK Land
//                    //cr.w1Record.getHhold().getDVOPRDEBT(); // Debt on other property
//                    //cr.w1Record.getHhold().getDVOPRVAL();  // Value of other property
//                    //cr.w1Record.getHhold().getHPROPW(); // Total property wealth
//                    //cr.w1Record.getHhold().getGOR(); // Government Office Region Code
//                    cr.w2Record.getHhold().getDVLUKVAL();  // Value of UK Land
//                    cr.w3Record.getHhold().getDVLUKVAL_SUM();  // Value of UK Land
//                    cr.w4Record.getHhold().getDVLUKVAL_SUM();  // Value of UK Land
//                    cr.w5Record.getHhold().getDVLUKVAL_SUM();  // Value of UK Land
//                    cr.w1Record.getPeople().stream()
//                            .forEach(w1person -> {
//                                w1person.getDVLUKDEBT(); // Derived - Total land uk debt
//                                w1person.getDVLUKV(); // Derived - Total land uk value
//                            });
//                });
        logPW.close();
    }

    /**
     * Checks if cr has the same basic adult household composition in each wave
     * for those hholds that have only 1 record for each wave. Iff this is the
     * case then true is returned. The number of adults in a household is
     * allowed to decrease. If the number of adults increases, then a further
     * check is done: If the number of householders is the same and the number
     * of children has decreased (it might be assumed that children have become
     * non-dependents). But, if that is not the case, then if the number of
     * dependents increases for any wave then false is returned.
     *
     * @param CASEW1
     * @param cr
     * @return true iff cr has only 1 record for each wave.
     */
    protected boolean process3(short CASEW1, WaAS_Combined_Record cr) {
        boolean r;
        r = true;
        if (cr.w2Records.size() > 1) {
            log("There are multiple Wave 2 records for CASEW1 " + CASEW1);
            r = false;
        }
        Short CASEW2;
        Iterator<Short> ite2;
        ite2 = cr.w2Records.keySet().iterator();
        while (ite2.hasNext()) {
            CASEW2 = ite2.next();
            WaAS_Wave2_Record w2rec;
            w2rec = cr.w2Records.get(CASEW2);
            String m3;
            m3 = "There are multiple Wave 3 records for "
                    + "CASEW2 " + CASEW2 + " in CASEW1 " + CASEW1;
            if (cr.w3Records.containsKey(CASEW2)) {
                HashMap<Short, WaAS_Wave3_Record> w3_2;
                w3_2 = cr.w3Records.get(CASEW2);
                if (w3_2.size() > 1) {
                    log(m3);
                    r = false;
                } else {
                    Short CASEW3;
                    Iterator<Short> ite3;
                    ite3 = w3_2.keySet().iterator();
                    while (ite3.hasNext()) {
                        CASEW3 = ite3.next();
                        WaAS_Wave3_Record w3rec;
                        w3rec = w3_2.get(CASEW3);
                        String m4;
                        m4 = "There are multiple Wave 4 records for "
                                + "CASEW3 " + CASEW3
                                + " in CASEW2 " + CASEW2
                                + " in CASEW1 " + CASEW1;
                        if (cr.w4Records.containsKey(CASEW2)) {
                            HashMap<Short, HashMap<Short, WaAS_Wave4_Record>> w4_2;
                            w4_2 = cr.w4Records.get(CASEW2);
                            if (w4_2.containsKey(CASEW3)) {
                                HashMap<Short, WaAS_Wave4_Record> w4_3;
                                w4_3 = w4_2.get(CASEW3);
                                if (w4_3.size() > 1) {
                                    log(m4);
                                    r = false;
                                } else {
                                    Iterator<Short> ite4;
                                    ite4 = w4_3.keySet().iterator();
                                    while (ite4.hasNext()) {
                                        Short CASEW4;
                                        CASEW4 = ite4.next();
                                        WaAS_Wave4_Record w4rec;
                                        w4rec = w4_3.get(CASEW4);
                                        String m5;
                                        m5 = "There are multiple Wave 5 records for "
                                                + "CASEW4 " + CASEW4
                                                + " in CASEW3 " + CASEW3
                                                + " in CASEW2 " + CASEW2
                                                + " in CASEW1 " + CASEW1;
                                        if (cr.w5Records.containsKey(CASEW2)) {
                                            HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>> w5_2;
                                            w5_2 = cr.w5Records.get(CASEW2);
                                            if (w5_2.containsKey(CASEW3)) {
                                                HashMap<Short, HashMap<Short, WaAS_Wave5_Record>> w5_3;
                                                w5_3 = w5_2.get(CASEW3);
                                                if (w5_3.containsKey(CASEW4)) {
                                                    HashMap<Short, WaAS_Wave5_Record> w5_4;
                                                    w5_4 = w5_3.get(CASEW4);
                                                    if (w5_4.size() > 1) {
                                                        log(m5);
                                                        r = false;
                                                    } else {
                                                        Iterator<Short> ite5;
                                                        ite5 = w5_4.keySet().iterator();
                                                        while (ite5.hasNext()) {
                                                            Short CASEW5;
                                                            CASEW5 = ite5.next();
                                                            WaAS_Wave5_Record w5rec;
                                                            w5rec = w5_4.get(CASEW5);
                                                            // Wave 1
                                                            WaAS_Wave1_HHOLD_Record w1hhold;
                                                            w1hhold = cr.w1Record.getHhold();
                                                            ArrayList<WaAS_Wave1_PERSON_Record> w1people;
                                                            w1people = cr.w1Record.getPeople();
                                                            // Wave 2
                                                            WaAS_Wave2_HHOLD_Record w2hhold;
                                                            w2hhold = w2rec.getHhold();
                                                            ArrayList<WaAS_Wave2_PERSON_Record> w2people;
                                                            w2people = w2rec.getPeople();
                                                            // Wave 3
                                                            WaAS_Wave3_HHOLD_Record w3hhold;
                                                            w3hhold = w3rec.getHhold();
                                                            ArrayList<WaAS_Wave3_PERSON_Record> w3people;
                                                            w3people = w3rec.getPeople();
                                                            // Wave 4
                                                            WaAS_Wave4_HHOLD_Record w4hhold;
                                                            w4hhold = w4rec.getHhold();
                                                            ArrayList<WaAS_Wave4_PERSON_Record> w4people;
                                                            w4people = w4rec.getPeople();
                                                            // Wave 5
                                                            WaAS_Wave5_HHOLD_Record w5hhold;
                                                            w5hhold = w5rec.getHhold();
                                                            ArrayList<WaAS_Wave5_PERSON_Record> w5people;
                                                            w5people = w5rec.getPeople();
                                                            byte w1NUMADULT = w1hhold.getNUMADULT();
                                                            byte w1NUMCHILD = w1hhold.getNUMCHILD();
                                                            //byte w1NUMDEPCH = w1hhold.getNUMDEPCH();
                                                            byte w1NUMHHLDR = w1hhold.getNUMHHLDR();

                                                            byte w2NUMADULT = w2hhold.getNUMADULT();
                                                            byte w2NUMCHILD = w2hhold.getNUMCHILD();
                                                            //byte w2NUMDEPCH = w2hhold.getNUMDEPCH_HH();
                                                            //boolean w2NUMNDEP = w2hhold.getNUMNDEP();
                                                            byte w2NUMHHLDR = w2hhold.getNUMHHLDR();

                                                            byte w3NUMADULT = w3hhold.getNUMADULT();
                                                            byte w3NUMCHILD = w3hhold.getNUMCHILD();
                                                            //byte w3NUMDEPCH = w3hhold.getNUMDEPCH();
                                                            byte w3NUMHHLDR = w3hhold.getNUMHHLDR();

                                                            byte w4NUMADULT = w4hhold.getNUMADULT();
                                                            byte w4NUMCHILD = w4hhold.getNUMCHILD();
                                                            //byte w4NUMDEPCH = w4hhold.getNUMDEPCH();
                                                            byte w4NUMHHLDR = w4hhold.getNUMHHLDR();

                                                            byte w5NUMADULT = w5hhold.getNUMADULT();
                                                            byte w5NUMCHILD = w5hhold.getNUMCHILD();
                                                            //byte w5NUMDEPCH = w5hhold.getNUMDEPCH();
                                                            byte w5NUMHHLDR = w5hhold.getNUMHHLDR();
                                                            // Compare Wave 1 to Wave 2
                                                            if (w1NUMADULT > w2NUMADULT) {
                                                                if (!(w1NUMHHLDR == w2NUMHHLDR
                                                                        && w1NUMCHILD > w2NUMCHILD)) {
                                                                    // Compare Number of Non dependents in Waves 1 and 2
                                                                    int w1NUMNDep = getNUMNDEP(w1people);
                                                                    int w2NUMNDep = getNUMNDEP(w2people);
                                                                    if (w1NUMNDep < w2NUMNDep) {
                                                                        r = false;
                                                                    }
                                                                }
                                                            }
                                                            // Compare Wave 2 to Wave 3
                                                            if (w2NUMADULT > w3NUMADULT) {
                                                                if (!(w2NUMHHLDR == w3NUMHHLDR
                                                                        && w2NUMCHILD > w3NUMCHILD)) {
                                                                    // Compare Number of Non dependents in Waves 2 and 3
                                                                    int w2NUMNDep = getNUMNDEP(w2people);
                                                                    int w3NUMNDep = getNUMNDEP(w3people);
                                                                    if (w2NUMNDep < w3NUMNDep) {
                                                                        r = false;
                                                                    }
                                                                }
                                                            }
                                                            // Compare Wave 3 to Wave 4
                                                            if (w3NUMADULT > w4NUMADULT) {
                                                                if (!(w3NUMHHLDR == w4NUMHHLDR
                                                                        && w3NUMCHILD > w4NUMCHILD)) {
                                                                    // Compare Number of Non dependents in Waves 3 and 4
                                                                    int w3NUMNDep = getNUMNDEP(w3people);
                                                                    int w4NUMNDep = getNUMNDEP(w4people);
                                                                    if (w3NUMNDep < w4NUMNDep) {
                                                                        r = false;
                                                                    }
                                                                }
                                                            }
                                                            // Compare Wave 4 to Wave 5
                                                            if (w4NUMADULT > w5NUMADULT) {
                                                                if (!(w4NUMHHLDR == w5NUMHHLDR
                                                                        && w4NUMCHILD > w5NUMCHILD)) {
                                                                    // Compare Number of Non dependents in Waves 4 and 5
                                                                    int w4NUMNDep = getNUMNDEP(w4people);
                                                                    int w5NUMNDep = getNUMNDEP(w5people);
                                                                    if (w4NUMNDep < w5NUMNDep) {
                                                                        r = false;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                } else {
                                                    log("!w5_3.containsKey(CASEW4) " + m5);
                                                }
                                            } else {
                                                log("!w5_2.containsKey(CASEW3) " + m5);
                                                r = false;
                                            }
                                        } else {
                                            log("!cr.w5Records.containsKey(CASEW2) " + m5);
                                            r = false;
                                        }
                                    }
                                }
                            } else {
                                log("!w4_2.containsKey(CASEW3) " + m4);
                                r = false;
                            }
                        } else {
                            log("!cr.w4Records.containsKey(CASEW2) " + m4);
                            r = false;
                        }
                    }
                }
            } else {
                log(m3);
            }
        }
        return r;
    }

    /**
     * Get number of non child dependents.
     *
     * @param <T>
     * @param people
     * @return
     * @TODO Move to Person Handler
     */
    public <T> int getNUMNDEP(ArrayList<T> people) {
        int r = 0;
        WaAS_Wave1Or2Or3Or4Or5_PERSON_Record p2;
        Iterator<T> ite;
        ite = people.iterator();
        while (ite.hasNext()) {
            p2 = (WaAS_Wave1Or2Or3Or4Or5_PERSON_Record) ite.next();
//            p2.getISHRP();
//            p2.getISHRPPART();
//            p2.getISCHILD();
            if (p2.getISNDEP()) {
                r++;
            }
        }
        return r;
    }

    /**
     * Checks if cr has the same number of adults in each wave for those hholds
     * that have only 1 record for each wave.
     *
     * @param CASEW1
     * @param cr
     * @return true iff cr has only 1 record for each wave.
     */
    protected boolean process2(short CASEW1, WaAS_Combined_Record cr) {
        boolean r;
        r = true;
        if (cr.w2Records.size() > 1) {
            log("There are multiple Wave 2 records for CASEW1 " + CASEW1);
            r = false;
        }
        Short CASEW2;
        Iterator<Short> ite2;
        ite2 = cr.w2Records.keySet().iterator();
        while (ite2.hasNext()) {
            CASEW2 = ite2.next();
            WaAS_Wave2_Record w2rec;
            w2rec = cr.w2Records.get(CASEW2);
            String m3;
            m3 = "There are multiple Wave 3 records for "
                    + "CASEW2 " + CASEW2 + " in CASEW1 " + CASEW1;
            if (cr.w3Records.containsKey(CASEW2)) {
                HashMap<Short, WaAS_Wave3_Record> w3_2;
                w3_2 = cr.w3Records.get(CASEW2);
                if (w3_2.size() > 1) {
                    log(m3);
                    r = false;
                } else {
                    Short CASEW3;
                    Iterator<Short> ite3;
                    ite3 = w3_2.keySet().iterator();
                    while (ite3.hasNext()) {
                        CASEW3 = ite3.next();
                        WaAS_Wave3_Record w3rec;
                        w3rec = w3_2.get(CASEW3);
                        String m4;
                        m4 = "There are multiple Wave 4 records for "
                                + "CASEW3 " + CASEW3
                                + " in CASEW2 " + CASEW2
                                + " in CASEW1 " + CASEW1;
                        if (cr.w4Records.containsKey(CASEW2)) {
                            HashMap<Short, HashMap<Short, WaAS_Wave4_Record>> w4_2;
                            w4_2 = cr.w4Records.get(CASEW2);
                            if (w4_2.containsKey(CASEW3)) {
                                HashMap<Short, WaAS_Wave4_Record> w4_3;
                                w4_3 = w4_2.get(CASEW3);
                                if (w4_3.size() > 1) {
                                    log(m4);
                                    r = false;
                                } else {
                                    Iterator<Short> ite4;
                                    ite4 = w4_3.keySet().iterator();
                                    while (ite4.hasNext()) {
                                        Short CASEW4;
                                        CASEW4 = ite4.next();
                                        WaAS_Wave4_Record w4rec;
                                        w4rec = w4_3.get(CASEW4);
                                        String m5;
                                        m5 = "There are multiple Wave 5 records for "
                                                + "CASEW4 " + CASEW4
                                                + " in CASEW3 " + CASEW3
                                                + " in CASEW2 " + CASEW2
                                                + " in CASEW1 " + CASEW1;
                                        if (cr.w5Records.containsKey(CASEW2)) {
                                            HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>> w5_2;
                                            w5_2 = cr.w5Records.get(CASEW2);
                                            if (w5_2.containsKey(CASEW3)) {
                                                HashMap<Short, HashMap<Short, WaAS_Wave5_Record>> w5_3;
                                                w5_3 = w5_2.get(CASEW3);
                                                if (w5_3.containsKey(CASEW4)) {
                                                    HashMap<Short, WaAS_Wave5_Record> w5_4;
                                                    w5_4 = w5_3.get(CASEW4);
                                                    if (w5_4.size() > 1) {
                                                        log(m5);
                                                        r = false;
                                                    } else {
                                                        Iterator<Short> ite5;
                                                        ite5 = w5_4.keySet().iterator();
                                                        while (ite5.hasNext()) {
                                                            Short CASEW5;
                                                            CASEW5 = ite5.next();
                                                            WaAS_Wave5_Record w5rec;
                                                            w5rec = w5_4.get(CASEW5);
                                                            byte w1 = cr.w1Record.getHhold().getNUMADULT();
                                                            byte w2 = w2rec.getHhold().getNUMADULT();
                                                            byte w3 = w3rec.getHhold().getNUMADULT();
                                                            byte w4 = w4rec.getHhold().getNUMADULT();
                                                            byte w5 = w5rec.getHhold().getNUMADULT();
                                                            if (!(w1 == w2 && w2 == w3 && w3 == w4 && w4 == w5)) {
                                                                r = false;
                                                            }
                                                        }
                                                    }
                                                } else {
                                                    log("!w5_3.containsKey(CASEW4) " + m5);
                                                }
                                            } else {
                                                log("!w5_2.containsKey(CASEW3) " + m5);
                                                r = false;
                                            }
                                        } else {
                                            log("!cr.w5Records.containsKey(CASEW2) " + m5);
                                            r = false;
                                        }
                                    }
                                }
                            } else {
                                log("!w4_2.containsKey(CASEW3) " + m4);
                                r = false;
                            }
                        } else {
                            log("!cr.w4Records.containsKey(CASEW2) " + m4);
                            r = false;
                        }
                    }
                }
            } else {
                log(m3);
            }
        }
        return r;
    }

    /**
     * Checks if cr has only 1 record for each wave.
     *
     * @param CASEW1
     * @param cr
     * @return true iff cr has only 1 record for each wave.
     */
    protected boolean process1(short CASEW1, WaAS_Combined_Record cr) {
        boolean r;
        r = true;
        if (cr.w2Records.size() > 1) {
            log("There are multiple Wave 2 records for CASEW1 " + CASEW1);
            r = false;
        }
        Short CASEW2;
        Iterator<Short> ite2;
        ite2 = cr.w2Records.keySet().iterator();
        while (ite2.hasNext()) {
            CASEW2 = ite2.next();
            //WaAS_Wave2_Record w2rec;
            //w2rec = cr.w2Records.get(CASEW2);
            String m3;
            m3 = "There are multiple Wave 3 records for "
                    + "CASEW2 " + CASEW2 + " in CASEW1 " + CASEW1;
            if (cr.w3Records.containsKey(CASEW2)) {
                HashMap<Short, WaAS_Wave3_Record> w3_2;
                w3_2 = cr.w3Records.get(CASEW2);
                if (w3_2.size() > 1) {
                    log(m3);
                    r = false;
                } else {
                    Short CASEW3;
                    Iterator<Short> ite3;
                    ite3 = w3_2.keySet().iterator();
                    while (ite3.hasNext()) {
                        CASEW3 = ite3.next();
                        //WaAS_Wave3_Record w3rec;
                        //w3rec = w3_2.get(CASEW3);
                        String m4;
                        m4 = "There are multiple Wave 4 records for "
                                + "CASEW3 " + CASEW3
                                + " in CASEW2 " + CASEW2
                                + " in CASEW1 " + CASEW1;
                        if (cr.w4Records.containsKey(CASEW2)) {
                            HashMap<Short, HashMap<Short, WaAS_Wave4_Record>> w4_2;
                            w4_2 = cr.w4Records.get(CASEW2);
                            if (w4_2.containsKey(CASEW3)) {
                                HashMap<Short, WaAS_Wave4_Record> w4_3;
                                w4_3 = w4_2.get(CASEW3);
                                if (w4_3.size() > 1) {
                                    log(m4);
                                    r = false;
                                } else {
                                    Iterator<Short> ite4;
                                    ite4 = w4_3.keySet().iterator();
                                    while (ite4.hasNext()) {
                                        Short CASEW4;
                                        CASEW4 = ite4.next();
                                        //WaAS_Wave4_Record w4rec;
                                        //w4rec = w4_3.get(CASEW4);
                                        String m5;
                                        m5 = "There are multiple Wave 5 records for "
                                                + "CASEW4 " + CASEW4
                                                + " in CASEW3 " + CASEW3
                                                + " in CASEW2 " + CASEW2
                                                + " in CASEW1 " + CASEW1;
                                        if (cr.w5Records.containsKey(CASEW2)) {
                                            HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>> w5_2;
                                            w5_2 = cr.w5Records.get(CASEW2);
                                            if (w5_2.containsKey(CASEW3)) {
                                                HashMap<Short, HashMap<Short, WaAS_Wave5_Record>> w5_3;
                                                w5_3 = w5_2.get(CASEW3);
                                                if (w5_3.containsKey(CASEW4)) {
                                                    HashMap<Short, WaAS_Wave5_Record> w5_4;
                                                    w5_4 = w5_3.get(CASEW4);
                                                    if (w5_4.size() > 1) {
                                                        log(m5);
                                                        r = false;
                                                    } else {
//                                                        Iterator<Short> ite5;
//                                                        ite5 = w5_4.keySet().iterator();
//                                                        while (ite5.hasNext()) {
//                                                            Short CASEW5;
//                                                            CASEW5 = ite5.next();
//                                                            WaAS_Wave5_Record w5rec;
//                                                            w5rec = w5_4.get(CASEW5);
//                                                        }
                                                    }
                                                } else {
                                                    log("!w5_3.containsKey(CASEW4) " + m5);
                                                }
                                            } else {
                                                log("!w5_2.containsKey(CASEW3) " + m5);
                                                r = false;
                                            }
                                        } else {
                                            log("!cr.w5Records.containsKey(CASEW2) " + m5);
                                            r = false;
                                        }
                                    }
                                }
                            } else {
                                log("!w4_2.containsKey(CASEW3) " + m4);
                                r = false;
                            }
                        } else {
                            log("!cr.w4Records.containsKey(CASEW2) " + m4);
                            r = false;
                        }
                    }
                }
            } else {
                log(m3);
            }
        }
        return r;
    }

    /**
     * Checks if cr has records for each wave.
     *
     * @param CASEW1
     * @param cr
     * @return true iff cr has records for each wave.
     */
    protected boolean process0(short CASEW1, WaAS_Combined_Record cr) {
        boolean r;
        r = true;
        if (cr.w1Record == null) {
            log("There is no Wave 1 record for CASEW1 " + CASEW1);
            r = false;
        }
        if (cr.w2Records.isEmpty()) {
            log("There are no Wave 2 records for CASEW1 " + CASEW1);
            r = false;
        }
        Short CASEW2;
        Iterator<Short> ite2;
        ite2 = cr.w2Records.keySet().iterator();
        while (ite2.hasNext()) {
            CASEW2 = ite2.next();
            //WaAS_Wave2_Record w2rec;
            //w2rec = cr.w2Records.get(CASEW2);
            String m3;
            m3 = "There are no Wave 3 records for "
                    + "CASEW2 " + CASEW2 + " in CASEW1 " + CASEW1;
            if (cr.w3Records.containsKey(CASEW2)) {
                HashMap<Short, WaAS_Wave3_Record> w3_2;
                w3_2 = cr.w3Records.get(CASEW2);
                Short CASEW3;
                Iterator<Short> ite3;
                ite3 = w3_2.keySet().iterator();
                while (ite3.hasNext()) {
                    CASEW3 = ite3.next();
                    //WaAS_Wave3_Record w3rec;
                    //w3rec = w3_2.get(CASEW3);
                    String m4;
                    m4 = "There are no Wave 4 records for "
                            + "CASEW3 " + CASEW3
                            + " in CASEW2 " + CASEW2
                            + " in CASEW1 " + CASEW1;
                    if (cr.w4Records.containsKey(CASEW2)) {
                        HashMap<Short, HashMap<Short, WaAS_Wave4_Record>> w4_2;
                        w4_2 = cr.w4Records.get(CASEW2);
                        if (w4_2.containsKey(CASEW3)) {
                            HashMap<Short, WaAS_Wave4_Record> w4_3;
                            w4_3 = w4_2.get(CASEW3);
                            Iterator<Short> ite4;
                            ite4 = w4_3.keySet().iterator();
                            while (ite4.hasNext()) {
                                Short CASEW4;
                                CASEW4 = ite4.next();
                                //WaAS_Wave4_Record w4rec;
                                //w4rec = w4_3.get(CASEW4);
                                String m5;
                                m5 = "There are no Wave 5 records for "
                                        + "CASEW4 " + CASEW4
                                        + " in CASEW3 " + CASEW3
                                        + " in CASEW2 " + CASEW2
                                        + " in CASEW1 " + CASEW1;
                                if (cr.w5Records.containsKey(CASEW2)) {
                                    HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>> w5_2;
                                    w5_2 = cr.w5Records.get(CASEW2);
                                    if (w5_2.containsKey(CASEW3)) {
                                        HashMap<Short, HashMap<Short, WaAS_Wave5_Record>> w5_3;
                                        w5_3 = w5_2.get(CASEW3);
                                        if (w5_3.containsKey(CASEW4)) {
                                            HashMap<Short, WaAS_Wave5_Record> w5_4;
                                            w5_4 = w5_3.get(CASEW4);
//                                            Iterator<Short> ite5;
//                                            ite5 = w5_4.keySet().iterator();
//                                            while (ite5.hasNext()) {
//                                                Short CASEW5;
//                                                CASEW5 = ite5.next();
//                                                WaAS_Wave5_Record w5rec;
//                                                w5rec = w5_4.get(CASEW5);
//                                            }
                                        } else {
                                            log("!w5_3.containsKey(CASEW4) " + m5);
                                        }
                                    } else {
                                        log("!w5_2.containsKey(CASEW3) " + m5);
                                        r = false;
                                    }
                                } else {
                                    log("!cr.w5Records.containsKey(CASEW2) " + m5);
                                    r = false;
                                }
                            }
                        } else {
                            log("!w4_2.containsKey(CASEW3) " + m4);
                            r = false;
                        }
                    } else {
                        log("!cr.w4Records.containsKey(CASEW2) " + m4);
                        r = false;
                    }
                }
            } else {
                log("!cr.w3Records.containsKey(CASEW2) " + m3);
                r = false;
            }
        }
        return r;
    }

    protected void addVariable(String s, TreeMap<Integer, String> vIDToVName,
            TreeMap<String, Integer> vNameToVID) {
        vIDToVName.put(0, s);
        vNameToVID.put(s, 0);
    }

    /**
     * Merge Person and Household Data
     *
     * @param indir
     * @param outdir
     * @param hholdHandler
     * @param chunkSize
     */
    public void doDataProcessingStep2(File indir, File outdir,
            WaAS_HHOLD_Handler hholdHandler, int chunkSize) {
        initlog(2);
        WaAS_PERSON_Handler personHandler;
        personHandler = new WaAS_PERSON_Handler(Files, Strings, indir);
        log("Merge Person and Household Data");
        /**
         * Wave 1
         */
        Object[] o;
        o = doDataProcessingStep2Wave1(
                data, personHandler, indir, outdir, hholdHandler, chunkSize);
        int nOC = (Integer) o[0];
        TreeMap<Short, HashSet<Short>> CIDToCASEW1;
        CIDToCASEW1 = (TreeMap<Short, HashSet<Short>>) o[1];
        HashMap<Short, Short> CASEW1ToCID;
        CASEW1ToCID = (HashMap<Short, Short>) o[2];
        /**
         * Wave 2
         */
        Object[] lookups;
        lookups = hholdHandler.loadSubsetLookups(WaAS_Data.W1);
        TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2;
        CASEW1ToCASEW2 = (TreeMap<Short, HashSet<Short>>) lookups[0];
        TreeMap<Short, Short> CASEW2ToCASEW1;
        CASEW2ToCASEW1 = (TreeMap<Short, Short>) lookups[1];
        doDataProcessingStep2Wave2(data, personHandler, indir, outdir,
                hholdHandler, nOC, CASEW1ToCID, CIDToCASEW1, CASEW1ToCASEW2,
                CASEW2ToCASEW1);
        /**
         * Wave 3
         */
        lookups = hholdHandler.loadSubsetLookups(WaAS_Data.W2);
        TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3;
        CASEW2ToCASEW3 = (TreeMap<Short, HashSet<Short>>) lookups[0];
        TreeMap<Short, Short> CASEW3ToCASEW2;
        CASEW3ToCASEW2 = (TreeMap<Short, Short>) lookups[1];
        doDataProcessingStep2Wave3(data, personHandler, indir, outdir,
                hholdHandler, nOC, CASEW1ToCID, CIDToCASEW1, CASEW1ToCASEW2,
                CASEW2ToCASEW1, CASEW2ToCASEW3, CASEW3ToCASEW2);
        /**
         * Wave 4
         */
        lookups = hholdHandler.loadSubsetLookups(WaAS_Data.W3);
        TreeMap<Short, HashSet<Short>> CASEW3ToCASEW4;
        CASEW3ToCASEW4 = (TreeMap<Short, HashSet<Short>>) lookups[0];
        TreeMap<Short, Short> CASEW4ToCASEW3;
        CASEW4ToCASEW3 = (TreeMap<Short, Short>) lookups[1];
        doDataProcessingStep2Wave4(data, personHandler, indir, outdir,
                hholdHandler, nOC, CASEW1ToCID, CIDToCASEW1, CASEW1ToCASEW2,
                CASEW2ToCASEW1, CASEW2ToCASEW3, CASEW3ToCASEW2, CASEW3ToCASEW4,
                CASEW4ToCASEW3);
        /**
         * Wave 5
         */
        lookups = hholdHandler.loadSubsetLookups(WaAS_Data.W4);
        TreeMap<Short, HashSet<Short>> CASEW4ToCASEW5;
        CASEW4ToCASEW5 = (TreeMap<Short, HashSet<Short>>) lookups[0];
        TreeMap<Short, Short> CASEW5ToCASEW4;
        CASEW5ToCASEW4 = (TreeMap<Short, Short>) lookups[1];
        doDataProcessingStep2Wave5(data, personHandler, indir, outdir,
                hholdHandler, nOC, CASEW1ToCID, CIDToCASEW1, CASEW1ToCASEW2,
                CASEW2ToCASEW1, CASEW2ToCASEW3, CASEW3ToCASEW2, CASEW3ToCASEW4,
                CASEW4ToCASEW3, CASEW4ToCASEW5, CASEW5ToCASEW4);
        log("data.lookup.size() " + data.CASEW1ToCID.size());
        log("data.data.size() " + data.data.size());
        Env.cacheData();
        logPW.close();
    }

    /**
     * Merge Person and Household Data for Wave 1.
     *
     * @param data
     * @param personHandler
     * @param indir
     * @param outdir
     * @param hholdHandler
     * @param chunkSize
     * @return
     */
    public static Object[] doDataProcessingStep2Wave1(
            WaAS_Data data,
            WaAS_PERSON_Handler personHandler,
            File indir, File outdir,
            WaAS_HHOLD_Handler hholdHandler, int chunkSize) {
        // Wave 1
        String m0;
        m0 = "Wave 1";
        logStart(m0);
        Object[] r;
        r = new Object[3];
        TreeMap<Short, WaAS_Wave1_HHOLD_Record> hs;
        hs = hholdHandler.loadCacheSubsetWave1();
        TreeSet<Short> CASEW1IDs;
        CASEW1IDs = new TreeSet<>();
        CASEW1IDs.addAll(hs.keySet());
        int nOC;
        nOC = (int) Math.ceil((double) CASEW1IDs.size() / (double) chunkSize);
        r[0] = nOC;
        Object[] ps;
        ps = personHandler.loadSubsetWave1(CASEW1IDs, nOC, WaAS_Data.W1,
                outdir);
        TreeMap<Short, HashSet<Short>> CIDToCASEW1;
        CIDToCASEW1 = (TreeMap<Short, HashSet<Short>>) ps[0];
        TreeMap<Short, File> cFs;
        cFs = (TreeMap<Short, File>) ps[2];
        r[1] = ps[0];
        r[2] = ps[1];
        CIDToCASEW1.keySet().stream()
                .forEach(cID -> {
                    String m1;
                    m1 = "Collection ID " + cID;
                    logStart(m1);
                    WaAS_Collection c;
                    c = new WaAS_Collection(cID);
                    data.data.put(cID, c);
                    // Add hhold records.
                    String m2;
                    m2 = "Add hhold records";
                    logStart(m2);
                    HashSet<Short> s;
                    s = CIDToCASEW1.get(cID);
                    s.stream()
                            .forEach(CASEW1 -> {
                                data.CASEW1ToCID.put(CASEW1, cID);
                                HashMap<Short, WaAS_Combined_Record> m;
                                m = c.getData();
                                WaAS_Combined_Record cr;
                                cr = m.get(CASEW1);
                                if (cr == null) {
                                    cr = new WaAS_Combined_Record(CASEW1);
                                    m.put(CASEW1, cr);
                                }
                                cr.w1Record.setHhold(hs.get(CASEW1));
                            });
                    logEnd(m2);
                    // Add person records.
                    m2 = "Add person records";
                    logStart(m2);
                    File f;
                    BufferedReader br;
                    f = cFs.get(cID);
                    br = Generic_IO.getBufferedReader(f);
                    br.lines()
                            .skip(1) // Skip header.
                            .forEach(line -> {
                                WaAS_Wave1_PERSON_Record p;
                                p = new WaAS_Wave1_PERSON_Record(line);
                                short CASEW1;
                                CASEW1 = p.getCASEW1();
                                HashMap<Short, WaAS_Combined_Record> m;
                                m = c.getData();
                                WaAS_Combined_Record cr;
                                cr = m.get(CASEW1);
                                cr.w1Record.getPeople().add(p);
                            });
                    logEnd(m2);
                    // Close br
                    Generic_IO.closeBufferedReader(br);
                    // Cache and clear collection
                    data.cacheSubsetCollection(cID, c);
                    data.clearCollection(cID);
                    logEnd(m1);
                });
        logEnd(m0);
        return r;
    }

    /**
     * Merge Person and Household Data for Wave 2.
     *
     * @param data
     * @param personHandler
     * @param indir
     * @param outdir
     * @param hholdHandler
     * @param nOC
     * @param CASEW1ToCID
     *
     * @param CIDToCASEW1 @param CASEW1ToCASEW2
     * @param CASEW2ToCASEW1
     */
    public static void doDataProcessingStep2Wave2(WaAS_Data data,
            WaAS_PERSON_Handler personHandler, File indir, File outdir,
            WaAS_HHOLD_Handler hholdHandler, int nOC,
            HashMap<Short, Short> CASEW1ToCID,
            TreeMap<Short, HashSet<Short>> CIDToCASEW1,
            TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2,
            TreeMap<Short, Short> CASEW2ToCASEW1) {
        // Wave 2
        String m0;
        m0 = "Wave 2";
        logStart(m0);
        TreeMap<Short, WaAS_Wave2_HHOLD_Record> hs;
        hs = hholdHandler.loadCacheSubsetWave2();
        TreeMap<Short, File> cFs;
        cFs = personHandler.loadSubsetWave2(nOC, CASEW1ToCID, WaAS_Data.W2,
                outdir, CASEW2ToCASEW1);
        cFs.keySet().stream()
                .forEach(cID -> {
                    String m1;
                    m1 = "Collection ID " + cID;
                    logStart(m1);
                    WaAS_Collection c;
                    c = data.getCollection(cID);
                    // Add hhold records.
                    String m2;
                    m2 = "Add hhold records";
                    logStart(m2);
                    HashSet<Short> s;
                    s = CIDToCASEW1.get(cID);
                    s.stream()
                            .forEach(CASEW1 -> {
                                data.CASEW1ToCID.put(CASEW1, cID);
                                HashMap<Short, WaAS_Combined_Record> m;
                                m = c.getData();
                                WaAS_Combined_Record cr;
                                cr = m.get(CASEW1);
                                if (cr == null) {
                                    log("No combined record "
                                            + "for CASEW1 " + CASEW1 + "! "
                                            + "This may be a data error?");
                                } else {
                                    HashSet<Short> CASEW2s;
                                    CASEW2s = CASEW1ToCASEW2.get(CASEW1);
                                    CASEW2s.stream().forEach(CASEW2 -> {
                                        WaAS_Wave2_Record w2rec;
                                        w2rec = new WaAS_Wave2_Record(CASEW2);
                                        w2rec.setHhold(hs.get(CASEW2));
                                        cr.w2Records.put(CASEW2, w2rec);
                                    });
                                }
                            });
                    logEnd(m2);
                    // Add person records.
                    m2 = "Add person records";
                    logStart(m2);
                    File f;
                    BufferedReader br;
                    f = cFs.get(cID);
                    br = Generic_IO.getBufferedReader(f);
                    br.lines()
                            .skip(1) // Skip header.
                            .forEach(line -> {
                                WaAS_Wave2_PERSON_Record p;
                                p = new WaAS_Wave2_PERSON_Record(line);
                                short CASEW1Check;
                                CASEW1Check = p.getCASEW1();
                                short CASEW2;
                                CASEW2 = p.getCASEW2();
                                short CASEW1;
                                CASEW1 = CASEW2ToCASEW1.get(CASEW2);
                                printCheck(WaAS_Data.W2, CASEW1Check, CASEW1, CASEW1ToCASEW2);
                                HashMap<Short, WaAS_Combined_Record> m;
                                m = c.getData();
                                WaAS_Combined_Record cr;
                                cr = m.get(CASEW1);
                                if (cr == null) {
                                    log("No combined record "
                                            + "for CASEW1 " + CASEW1 + "! "
                                            + "This may be a data error, "
                                            + "or this person may have "
                                            + "moved from one hhold "
                                            + "to another?");
                                } else {
                                    HashSet<Short> CASEW2s;
                                    CASEW2s = CASEW1ToCASEW2.get(CASEW1);
                                    CASEW2s.stream().forEach(k2 -> {
                                        WaAS_Wave2_Record w2rec;
                                        w2rec = cr.w2Records.get(k2);
                                        w2rec.getPeople().add(p);
                                    });
                                }
                            });
                    logEnd(m2);
                    // Close br
                    Generic_IO.closeBufferedReader(br);
                    // Cache and clear collection
                    data.cacheSubsetCollection(cID, c);
                    data.clearCollection(cID);
                    logEnd(m1);
                });
        logEnd(m0);
    }

    protected static void printCheck(byte wave, short CASEWXCheck,
            short CASEWX, TreeMap<Short, HashSet<Short>> lookup) {
        if (CASEWXCheck != CASEWX) {
            log("Person in Wave " + wave + " record given by "
                    + "CASEW" + wave + " " + CASEWX + " has a "
                    + "CASEW" + (wave - 1) + " as " + CASEWXCheck + ", "
                    + "but in the CASEW" + wave + "ToCASEW" + (wave - 1) + " "
                    + "lookup this is " + CASEWX);
            if (lookup.get(CASEWXCheck) == null) {
                log("CASEW" + (wave - 1) + "ToCASEW" + wave + ".get(CASEW"
                        + (wave - 1) + "Check) == null");
            } else {
                log("CASEW" + (wave - 1) + "ToCASEW" + wave + ".get(CASEW"
                        + (wave - 1) + "Check).size() "
                        + lookup.get(CASEWXCheck).size());
            }
        }
    }

    /**
     * Merge Person and Household Data for Wave 3.
     *
     * @param data
     * @param personHandler
     * @param indir
     * @param outdir
     * @param hholdHandler
     * @param nOC
     * @param CASEW1ToCID
     * @param CIDToCASEW1
     * @param CASEW1ToCASEW2
     * @param CASEW2ToCASEW1
     * @param CASEW2ToCASEW3
     * @param CASEW3ToCASEW2
     */
    public static void doDataProcessingStep2Wave3(WaAS_Data data,
            WaAS_PERSON_Handler personHandler, File indir, File outdir,
            WaAS_HHOLD_Handler hholdHandler, int nOC,
            HashMap<Short, Short> CASEW1ToCID,
            TreeMap<Short, HashSet<Short>> CIDToCASEW1,
            TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2,
            TreeMap<Short, Short> CASEW2ToCASEW1,
            TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3,
            TreeMap<Short, Short> CASEW3ToCASEW2) {
        // Wave 3;
        String m0;
        m0 = "Wave 3";
        logStart(m0);
        TreeMap<Short, WaAS_Wave3_HHOLD_Record> hs;
        hs = hholdHandler.loadCacheSubsetWave3();
        TreeMap<Short, File> cFs;
        cFs = personHandler.loadSubsetWave3(nOC, CASEW1ToCID, WaAS_Data.W3,
                outdir, CASEW2ToCASEW1, CASEW3ToCASEW2);
        cFs.keySet().stream()
                .forEach(cID -> {
                    String m1;
                    m1 = "Collection ID " + cID;
                    logStart(m1);
                    WaAS_Collection c;
                    c = data.getCollection(cID);
                    // Add hhold records.
                    String m2;
                    m2 = "Add hhold records";
                    logStart(m2);
                    HashSet<Short> s;
                    s = CIDToCASEW1.get(cID);
                    s.stream()
                            .forEach(CASEW1 -> {
                                data.CASEW1ToCID.put(CASEW1, cID);
                                HashMap<Short, WaAS_Combined_Record> m;
                                m = c.getData();
                                WaAS_Combined_Record cr;
                                cr = m.get(CASEW1);
                                if (cr == null) {
                                    log("No combined record "
                                            + "for CASEW1 " + CASEW1 + "! "
                                            + "This may be a data error?");
                                } else {
                                    HashSet<Short> CASEW2s;
                                    CASEW2s = CASEW1ToCASEW2.get(CASEW1);
                                    CASEW2s.stream().forEach(CASEW2 -> {
                                        HashMap<Short, WaAS_Wave3_Record> w3_2;
                                        w3_2 = new HashMap<>();
                                        cr.w3Records.put(CASEW2, w3_2);
                                        HashSet<Short> CASEW3s;
                                        CASEW3s = CASEW2ToCASEW3.get(CASEW2);
                                        CASEW3s.stream().forEach(CASEW3 -> {
                                            WaAS_Wave3_Record w3rec;
                                            w3rec = new WaAS_Wave3_Record(CASEW3);
                                            w3rec.setHhold(hs.get(CASEW3));
                                            w3_2.put(CASEW3, w3rec);
                                        });
                                    });
                                }
                            });
                    logEnd(m2);
                    // Add person records.
                    m2 = "Add person records";
                    logStart(m2);
                    File f;
                    BufferedReader br;
                    f = cFs.get(cID);
                    br = Generic_IO.getBufferedReader(f);
                    br.lines()
                            .skip(1) // Skip header.
                            .forEach(line -> {
                                WaAS_Wave3_PERSON_Record p;
                                p = new WaAS_Wave3_PERSON_Record(line);
                                short CASEW1Check;
                                CASEW1Check = p.getCASEW1();
                                short CASEW2Check;
                                CASEW2Check = p.getCASEW2();
                                short CASEW3;
                                CASEW3 = p.getCASEW3();
                                short CASEW2;
                                CASEW2 = CASEW3ToCASEW2.get(CASEW3);
                                short CASEW1;
                                CASEW1 = CASEW2ToCASEW1.get(CASEW2);
                                //printCheck(WaAS_Data.W2, CASEW1Check, CASEW1, CASEW1ToCASEW2);
                                printCheck(WaAS_Data.W3, CASEW2Check, CASEW2, CASEW2ToCASEW3);
                                HashMap<Short, WaAS_Combined_Record> m;
                                m = c.getData();
                                WaAS_Combined_Record cr;
                                cr = m.get(CASEW1);
                                if (cr == null) {
                                    log("No combined record "
                                            + "for CASEW1 " + CASEW1 + "! "
                                            + "This may be a data error, "
                                            + "or this person may have "
                                            + "moved from one hhold "
                                            + "to another?");
                                } else {
                                    HashSet<Short> CASEW2s;
                                    CASEW2s = CASEW1ToCASEW2.get(CASEW1);
                                    CASEW2s.stream().forEach(k2 -> {
                                        HashSet<Short> CASEW3s;
                                        CASEW3s = CASEW2ToCASEW3.get(CASEW2);
                                        CASEW3s.stream().forEach(k3 -> {
                                            WaAS_Wave3_Record w3rec;
                                            w3rec = cr.w3Records.get(k2).get(k3);
                                            if (w3rec == null) {
                                                w3rec = new WaAS_Wave3_Record(k3);
                                                log("Adding people, but there "
                                                        + "is no hhold record "
                                                        + "for CASEW3 "
                                                        + CASEW3 + "!");
                                            }
                                            w3rec.getPeople().add(p);
                                        });
                                    });
                                }
                            });
                    logEnd(m2);
                    // Close br
                    Generic_IO.closeBufferedReader(br);
                    // Cache and clear collection
                    data.cacheSubsetCollection(cID, c);
                    data.clearCollection(cID);
                    logEnd(m1);
                });
        logEnd(m0);
    }

    /**
     * Merge Person and Household Data for Wave 4.
     *
     * @param data
     * @param personHandler
     * @param indir
     * @param outdir
     * @param hholdHandler
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
    public static void doDataProcessingStep2Wave4(WaAS_Data data,
            WaAS_PERSON_Handler personHandler, File indir, File outdir,
            WaAS_HHOLD_Handler hholdHandler, int nOC,
            HashMap<Short, Short> CASEW1ToCID,
            TreeMap<Short, HashSet<Short>> CIDToCASEW1,
            TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2,
            TreeMap<Short, Short> CASEW2ToCASEW1,
            TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3,
            TreeMap<Short, Short> CASEW3ToCASEW2,
            TreeMap<Short, HashSet<Short>> CASEW3ToCASEW4,
            TreeMap<Short, Short> CASEW4ToCASEW3) {
        // Wave 4
        String m0;
        m0 = "Wave 4";
        logStart(m0);
        TreeMap<Short, WaAS_Wave4_HHOLD_Record> hs;
        hs = hholdHandler.loadCacheSubsetWave4();
        TreeMap<Short, File> cFs;
        cFs = personHandler.loadSubsetWave4(nOC, CASEW1ToCID, WaAS_Data.W4,
                outdir, CASEW2ToCASEW1, CASEW3ToCASEW2, CASEW4ToCASEW3);
        cFs.keySet().stream()
                .forEach(cID -> {
                    String m1;
                    m1 = "Collection ID " + cID;
                    logStart(m1);
                    WaAS_Collection c;
                    c = data.getCollection(cID);
                    // Add hhold records.
                    String m2;
                    m2 = "Add hhold records";
                    logStart(m2);
                    HashSet<Short> s;
                    s = CIDToCASEW1.get(cID);
                    s.stream()
                            .forEach(CASEW1 -> {
                                data.CASEW1ToCID.put(CASEW1, cID);
                                HashMap<Short, WaAS_Combined_Record> m;
                                m = c.getData();
                                WaAS_Combined_Record cr;
                                cr = m.get(CASEW1);
                                if (cr == null) {
                                    log("No combined record "
                                            + "for CASEW1 " + CASEW1 + "! "
                                            + "This may be a data error?");
                                } else {
                                    HashSet<Short> CASEW2s;
                                    CASEW2s = CASEW1ToCASEW2.get(CASEW1);
                                    CASEW2s.stream().forEach(CASEW2 -> {
                                        HashMap<Short, HashMap<Short, WaAS_Wave4_Record>> w4_2;
                                        w4_2 = new HashMap<>();
                                        cr.w4Records.put(CASEW2, w4_2);
                                        HashSet<Short> CASEW3s;
                                        CASEW3s = CASEW2ToCASEW3.get(CASEW2);
                                        CASEW3s.stream().forEach(CASEW3 -> {
                                            HashMap<Short, WaAS_Wave4_Record> w4_3;
                                            w4_3 = new HashMap<>();
                                            w4_2.put(CASEW3, w4_3);
                                            HashSet<Short> CASEW4s;
                                            CASEW4s = CASEW3ToCASEW4.get(CASEW3);
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
                    logEnd(m2);
                    // Add person records.
                    m2 = "Add person records";
                    logStart(m2);
                    File f;
                    BufferedReader br;
                    f = cFs.get(cID);
                    br = Generic_IO.getBufferedReader(f);
                    br.lines()
                            .skip(1) // Skip header.
                            .forEach(line -> {
                                WaAS_Wave4_PERSON_Record p;
                                p = new WaAS_Wave4_PERSON_Record(line);
                                short CASEW1Check;
                                CASEW1Check = p.getCASEW1();
                                short CASEW2Check;
                                CASEW2Check = p.getCASEW2();
                                short CASEW3Check;
                                CASEW3Check = p.getCASEW3();
                                short CASEW4;
                                CASEW4 = p.getCASEW4();
                                short CASEW3;
                                CASEW3 = CASEW4ToCASEW3.get(CASEW4);
                                short CASEW2;
                                CASEW2 = CASEW3ToCASEW2.get(CASEW3);
                                short CASEW1;
                                CASEW1 = CASEW2ToCASEW1.get(CASEW2);
                                //printCheck(WaAS_Data.W2, CASEW1Check, CASEW1, CASEW1ToCASEW2);
                                //printCheck(WaAS_Data.W3, CASEW2Check, CASEW2, CASEW2ToCASEW3);
                                printCheck(WaAS_Data.W4, CASEW3Check, CASEW3, CASEW3ToCASEW4);
                                HashMap<Short, WaAS_Combined_Record> m;
                                m = c.getData();
                                WaAS_Combined_Record cr;
                                cr = m.get(CASEW1);
                                if (cr == null) {
                                    log("No combined record "
                                            + "for CASEW1 " + CASEW1 + "! "
                                            + "This may be a data error, "
                                            + "or this person may have "
                                            + "moved from one hhold "
                                            + "to another?");
                                } else {
                                    HashSet<Short> CASEW2s;
                                    CASEW2s = CASEW1ToCASEW2.get(CASEW1);
                                    CASEW2s.stream().forEach(k2 -> {
                                        HashSet<Short> CASEW3s;
                                        CASEW3s = CASEW2ToCASEW3.get(CASEW2);
                                        CASEW3s.stream().forEach(k3 -> {
                                            HashSet<Short> CASEW4s;
                                            CASEW4s = CASEW3ToCASEW4.get(CASEW3);
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
                                                    log("Adding people, but there "
                                                            + "is no hhold record "
                                                            + "for CASEW4 "
                                                            + CASEW4 + "!");
                                                }
                                                w4rec.getPeople().add(p);
                                            });
                                        });
                                    });
                                }
                            });
                    logEnd(m2);
                    // Close br
                    Generic_IO.closeBufferedReader(br);
                    // Cache and clear collection
                    data.cacheSubsetCollection(cID, c);
                    data.clearCollection(cID);
                    logEnd(m1);
                });
        logEnd(m0);
    }

    /**
     * Merge Person and Household Data for Wave 5.
     *
     * @param data
     * @param personHandler
     * @param indir
     * @param outdir
     * @param hholdHandler
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
    public static void doDataProcessingStep2Wave5(WaAS_Data data,
            WaAS_PERSON_Handler personHandler, File indir, File outdir,
            WaAS_HHOLD_Handler hholdHandler, int nOC,
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
        String m0;
        m0 = "Wave 5";
        logStart(m0);
        TreeMap<Short, WaAS_Wave5_HHOLD_Record> hs;
        hs = hholdHandler.loadCacheSubsetWave5();
        TreeMap<Short, File> cFs;
        cFs = personHandler.loadSubsetWave5(nOC, CASEW1ToCID, WaAS_Data.W5,
                outdir, CASEW2ToCASEW1, CASEW3ToCASEW2, CASEW4ToCASEW3,
                CASEW5ToCASEW4);
        cFs.keySet().stream()
                .forEach(cID -> {
                    String m1;
                    m1 = "Collection ID " + cID;
                    logStart(m1);
                    WaAS_Collection c;
                    c = data.getCollection(cID);
                    // Add hhold records.
                    String m2;
                    m2 = "Add hhold records";
                    logStart(m2);
                    HashSet<Short> s;
                    s = CIDToCASEW1.get(cID);
                    s.stream()
                            .forEach(CASEW1 -> {
                                data.CASEW1ToCID.put(CASEW1, cID);
                                HashMap<Short, WaAS_Combined_Record> m;
                                m = c.getData();
                                WaAS_Combined_Record cr;
                                cr = m.get(CASEW1);
                                if (cr == null) {
                                    log("No combined record "
                                            + "for CASEW1 " + CASEW1 + "! "
                                            + "This may be a data error?");
                                } else {
                                    HashSet<Short> CASEW2s;
                                    CASEW2s = CASEW1ToCASEW2.get(CASEW1);
                                    CASEW2s.stream().forEach(CASEW2 -> {
                                        HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>> w5_2;
                                        w5_2 = new HashMap<>();
                                        cr.w5Records.put(CASEW2, w5_2);
                                        HashSet<Short> CASEW3s;
                                        CASEW3s = CASEW2ToCASEW3.get(CASEW2);
                                        CASEW3s.stream().forEach(CASEW3 -> {
                                            HashMap<Short, HashMap<Short, WaAS_Wave5_Record>> w5_3;
                                            w5_3 = new HashMap<>();
                                            w5_2.put(CASEW3, w5_3);
                                            HashSet<Short> CASEW4s;
                                            CASEW4s = CASEW3ToCASEW4.get(CASEW3);
                                            CASEW4s.stream().forEach(CASEW4 -> {
                                                HashMap<Short, WaAS_Wave5_Record> w5_4;
                                                w5_4 = new HashMap<>();
                                                w5_3.put(CASEW4, w5_4);
                                                HashSet<Short> CASEW5s;
                                                CASEW5s = CASEW4ToCASEW5.get(CASEW4);
                                                CASEW5s.stream().forEach(CASEW5 -> {
                                                    WaAS_Wave5_Record w5rec;
                                                    w5rec = new WaAS_Wave5_Record(CASEW5);
                                                    w5rec.setHhold(hs.get(CASEW5));
                                                    w5_4.put(CASEW5, w5rec);
                                                });
                                            });
                                        });
                                    });
                                }
                            });
                    logEnd(m2);
                    // Add person records.
                    m2 = "Add person records";
                    logStart(m2);
                    File f;
                    BufferedReader br;
                    f = cFs.get(cID);
                    br = Generic_IO.getBufferedReader(f);
                    br.lines()
                            .skip(1) // Skip header.
                            .forEach(line -> {
                                WaAS_Wave5_PERSON_Record p;
                                p = new WaAS_Wave5_PERSON_Record(line);
                                short CASEW1Check;
                                CASEW1Check = p.getCASEW1();
                                short CASEW2Check;
                                CASEW2Check = p.getCASEW2();
                                short CASEW3Check;
                                CASEW3Check = p.getCASEW3();
                                short CASEW4Check;
                                CASEW4Check = p.getCASEW4();
                                short CASEW5;
                                CASEW5 = p.getCASEW5();
                                short CASEW4;
                                CASEW4 = CASEW5ToCASEW4.get(CASEW5);
                                short CASEW3;
                                CASEW3 = CASEW4ToCASEW3.get(CASEW4);
                                short CASEW2;
                                CASEW2 = CASEW3ToCASEW2.get(CASEW3);
                                short CASEW1;
                                CASEW1 = CASEW2ToCASEW1.get(CASEW2);
                                //printCheck(WaAS_Data.W2, CASEW1Check, CASEW1, CASEW1ToCASEW2);
                                //printCheck(WaAS_Data.W3, CASEW2Check, CASEW2, CASEW2ToCASEW3);
                                //printCheck(WaAS_Data.W4, CASEW3Check, CASEW3, CASEW3ToCASEW4);
                                printCheck(WaAS_Data.W5, CASEW4Check, CASEW4, CASEW4ToCASEW5);
                                HashMap<Short, WaAS_Combined_Record> m;
                                m = c.getData();
                                WaAS_Combined_Record cr;
                                cr = m.get(CASEW1);
                                if (cr == null) {
                                    log("No combined record "
                                            + "for CASEW1 " + CASEW1 + "! "
                                            + "This may be a data error, "
                                            + "or this person may have "
                                            + "moved from one hhold "
                                            + "to another?");
                                } else {
                                    HashSet<Short> CASEW2s;
                                    CASEW2s = CASEW1ToCASEW2.get(CASEW1);
                                    CASEW2s.stream().forEach(k2 -> {
                                        HashSet<Short> CASEW3s;
                                        CASEW3s = CASEW2ToCASEW3.get(CASEW2);
                                        CASEW3s.stream().forEach(k3 -> {
                                            HashSet<Short> CASEW4s;
                                            CASEW4s = CASEW3ToCASEW4.get(CASEW3);
                                            CASEW4s.stream().forEach(k4 -> {
                                                HashSet<Short> CASEW5s;
                                                CASEW5s = CASEW4ToCASEW5.get(CASEW4);
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
                                                        log("Adding people, but there "
                                                                + "is no hhold record "
                                                                + "for CASEW5 "
                                                                + CASEW5 + "!");
                                                    }
                                                    w5rec.getPeople().add(p);
                                                });
                                            });
                                        });
                                    });
                                }
                            });
                    logEnd(m2);
                    // Close br
                    Generic_IO.closeBufferedReader(br);
                    // Cache and clear collection
                    data.cacheSubsetCollection(cID, c);
                    data.clearCollection(cID);
                    logEnd(m1);
                });
        logEnd(m0);
    }

    /**
     * Method for running JavaCodeGeneration
     */
    public void runJavaCodeGeneration() {
        String[] args;
        args = null;
        WaAS_JavaCodeGenerator.main(args);
    }

    protected void initlog(int i) {
        logF = new File(Files.getOutputDataDir(Strings), "log" + i + ".txt");
        logPW = Generic_IO.getPrintWriter(logF, true); // Append to log file.
    }

    /**
     * Read input data and create subsets. Organise for person records that each
     * subset is split into separate files one for each collection. The
     * collections will be merged one by one in doDataProcessingStep2.
     *
     * @param indir
     * @param outdir
     * @param hholdHandler
     */
    public void doDataProcessingStep1New(File indir, File outdir,
            WaAS_HHOLD_Handler hholdHandler) {
        initlog(1);
        // For convenience/code brevity.
        byte NWAVES;
        NWAVES = WaAS_Data.NWAVES;
        /**
         * Step 1: Load hhold data into cache and memory.
         */
        Object[] hholdData;
        hholdData = hholdHandler.load();
        /**
         * Step 2: Unpack hholdData. hholdData is an Object[] of length 2. r[0]
         * is a TreeMap with Integer keys which are the CASE id for the wave and
         * the values are WaAS_Wave1Or2Or3Or4Or5_HHOLD_Record>. r[1] is an
         * array of TreeSets where: For Wave 5; r[1][0] is a list of CASEW5
         * values, r[1][1] is a list of CASEW4 values, r[1][2] is a list of
         * CASEW3 values, r[1][3] is a list of CASEW2 values, r[1][4] is a list
         * of CASEW1 values. For Wave 4; r[1][0] is a list of CASEW4 values,
         * r[1][1] is a list of CASEW3 values, r[1][2] is a list of CASEW2
         * values, r[1][3] is a list of CASEW1 values. For Wave 3; r[1][0] is a
         * list of CASEW3 values, r[1][1] is a list of CASEW2 values, r[1][2] is
         * a list of CASEW1 values. For Wave 2; r[1][0] is a list of CASEW2
         * values, r[1][1] is a list of CASEW1 values. For Wave 1: r[1][0] is a
         * list of CASEW1 values.
         */
        HashMap<Byte, TreeSet<Short>[]> iDLists;
        iDLists = new HashMap<>();
        TreeSet<Short>[] iDList;
        // W1
        Object[] hholdDataW1;
        hholdDataW1 = (Object[]) hholdData[0];
        iDLists.put(WaAS_Data.W1, (TreeSet<Short>[]) hholdDataW1[1]);
        // W2
        Object[] hholdDataW2;
        hholdDataW2 = (Object[]) hholdData[1];
        iDLists.put(WaAS_Data.W2, (TreeSet<Short>[]) hholdDataW2[1]);
        // W3
        Object[] hholdDataW3;
        hholdDataW3 = (Object[]) hholdData[2];
        iDLists.put(WaAS_Data.W3, (TreeSet<Short>[]) hholdDataW3[1]);
        // W4
        Object[] hholdDataW4;
        hholdDataW4 = (Object[]) hholdData[3];
        iDLists.put(WaAS_Data.W4, (TreeSet<Short>[]) hholdDataW4[1]);
        // W5
        Object[] hholdDataW5;
        hholdDataW5 = (Object[]) hholdData[4];
        iDLists.put(WaAS_Data.W5, (TreeSet<Short>[]) hholdDataW5[1]);
        /**
         * Step 3: Print out the Number of Households in each wave.
         *
         * @return r - an Object[] of length 2. r[0] is a TreeMap with keys as
         * CASEW5 and values as WaAS_Wave5_HHOLD_Records. r[1] is an array
         * of TreeSets where: r[1][0] is a list of CASEW1 values, r[1][1] is a
         * list of CASEW2 values, r[1][2] is a list of CASEW3 values, r[1][3] is
         * a list of CASEW4 values.
         */
        for (byte wave = NWAVES; wave > 0; wave--) {
            iDList = iDLists.get(wave);
            for (int i = wave; i > 0; i--) {
                String m;
                if (i == wave) {
                    if (wave > 2) {
                        m = "" + iDList[i].size()
                                + "\tNumber of HHOLD IDs in Wave " + wave
                                + " reported as being in ";
                        for (int j = wave - 1; j > 0; j--) {
                            m += "Wave " + j + ", ";
                        }
                        log(m);
                    }
                } else {
                    m = "" + iDList[i].size()
                            + "\tNumber of HHOLD IDs in Wave " + wave
                            + " reported as being in Wave " + i;
                    log(m);
                }
            }
        }
        logPW.close();
    }

    public static void log0(String s) {
        logPW.println(s);
    }

    public static void log1(String s) {
        System.out.println(s);
    }

    public static void log(String s) {
        logPW.println(s);
        System.out.println(s);
    }

    public static void logStart(String s) {
        s = "<" + s + ">";
        logPW.println(s);
        System.out.println(s);
    }

    public static void logEnd(String s) {
        s = "</" + s + ">";
        logPW.println(s);
        System.out.println(s);
    }

    boolean doJavaCodeGeneration = false;
    boolean doLoadDataIntoCaches = false;

}
