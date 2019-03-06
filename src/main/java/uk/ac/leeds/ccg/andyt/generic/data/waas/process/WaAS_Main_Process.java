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
import java.util.TreeMap;
import java.util.TreeSet;
import uk.ac.leeds.ccg.andyt.generic.core.Generic_Environment;
import uk.ac.leeds.ccg.andyt.generic.io.Generic_IO;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.andyt.generic.data.waas.io.WaAS_Files;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Object;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Strings;
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
        //p.doLoadHouseholdsInAllWaves = true;
        p.doLoadHouseholdsInPairedWaves = true;
        p.run();
    }

    public void run() {
        if (doJavaCodeGeneration) {
            runJavaCodeGeneration();
        }
            File indir = files.getWaASInputDir();
            File generateddir = files.getGeneratedWaASDir();
            File outdir = new File(generateddir, WaAS_Strings.s_Subsets);
            outdir.mkdirs();
            WaAS_HHOLD_Handler hH = new WaAS_HHOLD_Handler(env, indir);
            int chunkSize;
            chunkSize = 256; //1024; 512; 256;
        if (doLoadHouseholdsInAllWaves) {
            doDataProcessingStep1(indir, outdir, hH);
            doDataProcessingStep2(indir, outdir, hH, chunkSize);
            doDataProcessingStep3(indir, outdir, hH);
        }
        if (doLoadHouseholdsInPairedWaves) {
            Object[] w2inw1 = hH.loadHouseholdsInPreviousWave(W2);
            Object[] w3inw2 = hH.loadHouseholdsInPreviousWave(W3);
            Object[] w4inw3 = hH.loadHouseholdsInPreviousWave(W4);
            Object[] w5inw4 = hH.loadHouseholdsInPreviousWave(W5);
            int debug = 1;
        }
    }

    /**
     * Read input data and create household all sets. This method is independent
     * of the other steps.
     *
     * @param indir
     * @param outdir
     * @param hH
     */
    public void doDataProcessingStep3(File indir, File outdir,
            WaAS_HHOLD_Handler hH) {
        hH.loadAllW1();
        hH.loadAllW2();
        hH.loadAllW3();
        hH.loadAllW4();
        hH.loadAllW5();
    }

    /**
     * Merge Person and Household Data
     *
     * @param indir
     * @param outdir
     * @param hH
     * @param chunkSize
     */
    public void doDataProcessingStep2(File indir, File outdir,
            WaAS_HHOLD_Handler hH, int chunkSize) {
        String m = "doDataProcessingStep2";
        env.logStartTag(m);
        WaAS_PERSON_Handler pH = new WaAS_PERSON_Handler(env, indir);
        env.log("Merge Person and Household Data");
        /**
         * Wave 1
         */
        Object[] o;
        o = doDataProcessingStep2Wave1(data, pH, indir, outdir, hH, chunkSize);
        int nOC = (Integer) o[0];
        TreeMap<Short, HashSet<Short>> CIDToCASEW1;
        CIDToCASEW1 = (TreeMap<Short, HashSet<Short>>) o[1];
        HashMap<Short, Short> CASEW1ToCID = (HashMap<Short, Short>) o[2];
        Object[] lookups;
        /**
         * Wave 2
         */
        lookups = hH.loadSubsetLookups(W1);
        TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2;
        CASEW1ToCASEW2 = (TreeMap<Short, HashSet<Short>>) lookups[0];
        TreeMap<Short, Short> CASEW2ToCASEW1;
        CASEW2ToCASEW1 = (TreeMap<Short, Short>) lookups[1];
        doDataProcessingStep2Wave2(data, pH, indir, outdir, hH, nOC,
                CASEW1ToCID, CIDToCASEW1, CASEW1ToCASEW2, CASEW2ToCASEW1);
        /**
         * Wave 3
         */
        lookups = hH.loadSubsetLookups(W2);
        TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3;
        CASEW2ToCASEW3 = (TreeMap<Short, HashSet<Short>>) lookups[0];
        TreeMap<Short, Short> CASEW3ToCASEW2;
        CASEW3ToCASEW2 = (TreeMap<Short, Short>) lookups[1];
        doDataProcessingStep2Wave3(data, pH, indir, outdir, hH, nOC,
                CASEW1ToCID, CIDToCASEW1, CASEW1ToCASEW2, CASEW2ToCASEW1,
                CASEW2ToCASEW3, CASEW3ToCASEW2);
        /**
         * Wave 4
         */
        lookups = hH.loadSubsetLookups(W3);
        TreeMap<Short, HashSet<Short>> CASEW3ToCASEW4;
        CASEW3ToCASEW4 = (TreeMap<Short, HashSet<Short>>) lookups[0];
        TreeMap<Short, Short> CASEW4ToCASEW3;
        CASEW4ToCASEW3 = (TreeMap<Short, Short>) lookups[1];
        doDataProcessingStep2Wave4(data, pH, indir, outdir, hH, nOC,
                CASEW1ToCID, CIDToCASEW1, CASEW1ToCASEW2, CASEW2ToCASEW1,
                CASEW2ToCASEW3, CASEW3ToCASEW2, CASEW3ToCASEW4, CASEW4ToCASEW3);
        /**
         * Wave 5
         */
        lookups = hH.loadSubsetLookups(W4);
        TreeMap<Short, HashSet<Short>> CASEW4ToCASEW5;
        CASEW4ToCASEW5 = (TreeMap<Short, HashSet<Short>>) lookups[0];
        TreeMap<Short, Short> CASEW5ToCASEW4;
        CASEW5ToCASEW4 = (TreeMap<Short, Short>) lookups[1];
        doDataProcessingStep2Wave5(data, pH, indir, outdir, hH, nOC,
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
     * @param pH personHandler
     * @param indir
     * @param outdir
     * @param hH hholdHandler
     * @param chunkSize
     * @return
     */
    public Object[] doDataProcessingStep2Wave1(WaAS_Data data, 
            WaAS_PERSON_Handler pH, File indir, File outdir,
            WaAS_HHOLD_Handler hH, int chunkSize) {
        // Wave 1
        String m0 = "doDataProcessingStep2Wave1";
        env.logStartTag(m0);
        Object[] r = new Object[3];
        TreeMap<Short, WaAS_Wave1_HHOLD_Record> hs;
        hs = hH.loadCachedSubsetW1();
        TreeSet<Short> CASEW1IDs = new TreeSet<>();
        CASEW1IDs.addAll(hs.keySet());
        int nOC;
        nOC = (int) Math.ceil((double) CASEW1IDs.size() / (double) chunkSize);
        r[0] = nOC;
        Object[] ps;
        ps = pH.loadSubsetWave1(CASEW1IDs, nOC, W1, outdir);
        TreeMap<Short, HashSet<Short>> CIDToCASEW1;
        CIDToCASEW1 = (TreeMap<Short, HashSet<Short>>) ps[0];
        TreeMap<Short, File> cFs;
        cFs = (TreeMap<Short, File>) ps[2];
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
                HashMap<Short, WaAS_Combined_Record> m;
                m = c.getData();
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
    public void doDataProcessingStep2Wave2(WaAS_Data data,
            WaAS_PERSON_Handler personHandler, File indir, File outdir,
            WaAS_HHOLD_Handler hholdHandler, int nOC,
            HashMap<Short, Short> CASEW1ToCID,
            TreeMap<Short, HashSet<Short>> CIDToCASEW1,
            TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2,
            TreeMap<Short, Short> CASEW2ToCASEW1) {
        // Wave 2
        String m0 = "doDataProcessingStep2Wave2";
        env.logStartTag(m0);
        TreeMap<Short, WaAS_Wave2_HHOLD_Record> hs;
        hs = hholdHandler.loadCachedSubsetW2();
        TreeMap<Short, File> cFs = personHandler.loadSubsetWave2(nOC,
                CASEW1ToCID, W2, outdir, CASEW2ToCASEW1);
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
            File f;
            BufferedReader br;
            f = cFs.get(cID);
            br = Generic_IO.getBufferedReader(f);
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
            env.log("Person in Wave " + wave + " record given by "
                    + "CASEW" + wave + " " + CASEWX + " has a "
                    + "CASEW" + (wave - 1) + " as " + CASEWXCheck + ", "
                    + "but in the CASEW" + wave + "ToCASEW" + (wave - 1) + " "
                    + "lookup this is " + CASEWX);
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
     * @param pH personHandler
     * @param indir
     * @param outdir
     * @param hH hholdHandler
     * @param nOC
     * @param CASEW1ToCID
     * @param CIDToCASEW1
     * @param CASEW1ToCASEW2
     * @param CASEW2ToCASEW1
     * @param CASEW2ToCASEW3
     * @param CASEW3ToCASEW2
     */
    public void doDataProcessingStep2Wave3(WaAS_Data data,
            WaAS_PERSON_Handler pH, File indir, File outdir,
            WaAS_HHOLD_Handler hH, int nOC,
            HashMap<Short, Short> CASEW1ToCID,
            TreeMap<Short, HashSet<Short>> CIDToCASEW1,
            TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2,
            TreeMap<Short, Short> CASEW2ToCASEW1,
            TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3,
            TreeMap<Short, Short> CASEW3ToCASEW2) {
        // Wave 3;
        String m0 = "doDataProcessingStep2Wave3";
        env.logStartTag(m0);
        TreeMap<Short, WaAS_Wave3_HHOLD_Record> hs;
        hs = hH.loadCachedSubsetW3();
        TreeMap<Short, File> cFs = pH.loadSubsetWave3(nOC, CASEW1ToCID,
                W3, outdir, CASEW2ToCASEW1, CASEW3ToCASEW2);
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
     * @param pH personHandler
     * @param indir
     * @param outdir
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
    public void doDataProcessingStep2Wave4(WaAS_Data data,
            WaAS_PERSON_Handler pH, File indir, File outdir,
            WaAS_HHOLD_Handler hH, int nOC,
            HashMap<Short, Short> CASEW1ToCID,
            TreeMap<Short, HashSet<Short>> CIDToCASEW1,
            TreeMap<Short, HashSet<Short>> CASEW1ToCASEW2,
            TreeMap<Short, Short> CASEW2ToCASEW1,
            TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3,
            TreeMap<Short, Short> CASEW3ToCASEW2,
            TreeMap<Short, HashSet<Short>> CASEW3ToCASEW4,
            TreeMap<Short, Short> CASEW4ToCASEW3) {
        // Wave 4
        String m0 = "doDataProcessingStep2Wave4";
        env.logStartTag(m0);
        TreeMap<Short, WaAS_Wave4_HHOLD_Record> hs = hH.loadCachedSubsetW4();
        TreeMap<Short, File> cFs;
        cFs = pH.loadSubsetWave4(nOC, CASEW1ToCID, W4,
                outdir, CASEW2ToCASEW1, CASEW3ToCASEW2, CASEW4ToCASEW3);
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
    public void doDataProcessingStep2Wave5(WaAS_Data data,
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
        String m0 = "doDataProcessingStep2Wave5";
        env.logStartTag(m0);
        TreeMap<Short, WaAS_Wave5_HHOLD_Record> hs;
        hs = hholdHandler.loadCachedSubsetW5();
        TreeMap<Short, File> cFs;
        cFs = personHandler.loadSubsetWave5(nOC, CASEW1ToCID, W5,
                outdir, CASEW2ToCASEW1, CASEW3ToCASEW2, CASEW4ToCASEW3,
                CASEW5ToCASEW4);
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
                short CASEW4 = CASEW5ToCASEW4.get(CASEW5);
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
     * Method for running JavaCodeGeneration
     */
    public void runJavaCodeGeneration() {
        WaAS_JavaCodeGenerator.main(null);
    }

    /**
     * Read input data and create subsets. Organise for person records that each
     * subset is split into separate files one for each collection. The
     * collections will be merged one by one in doDataProcessingStep2.
     *
     * @param indir
     * @param outdir
     * @param hH hholdHandler
     */
    public void doDataProcessingStep1(File indir, File outdir,
            WaAS_HHOLD_Handler hH) {
        String m0 = "doDataProcessingStep1";
        env.logStartTag(m0);
        /**
         * Step 1: Load hhold data into cache and memory.
         */
        Object[] hholdData = hH.loadHouseholdsInAllWaves();
        /**
         * Step 2: Unpack hholdData. hholdData is an Object[] of length 2. r[0]
         * is a TreeMap with Integer keys which are the CASE id for the wave and
         * the values are WaAS_Wave1Or2Or3Or4Or5_HHOLD_Record>. r[1] is an array
         * of TreeSets where: For Wave 5; r[1][0] is a list of CASEW5 values,
         * r[1][1] is a list of CASEW4 values, r[1][2] is a list of CASEW3
         * values, r[1][3] is a list of CASEW2 values, r[1][4] is a list of
         * CASEW1 values. For Wave 4; r[1][0] is a list of CASEW4 values,
         * r[1][1] is a list of CASEW3 values, r[1][2] is a list of CASEW2
         * values, r[1][3] is a list of CASEW1 values. For Wave 3; r[1][0] is a
         * list of CASEW3 values, r[1][1] is a list of CASEW2 values, r[1][2] is
         * a list of CASEW1 values. For Wave 2; r[1][0] is a list of CASEW2
         * values, r[1][1] is a list of CASEW1 values. For Wave 1: r[1][0] is a
         * list of CASEW1 values.
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

    boolean doJavaCodeGeneration = false;
    boolean doLoadHouseholdsInAllWaves = false;
    boolean doLoadHouseholdsInPairedWaves = false;

}
