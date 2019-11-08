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
package uk.ac.leeds.ccg.andyt.generic.data.waas.data.handlers;

import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W1ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W4ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W5ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W2ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W3ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_WID;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.TreeSet;
import uk.ac.leeds.ccg.andyt.data.Data_Collection;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.andyt.generic.util.Generic_Collections;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Strings;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_CombinedRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_GORSubsetsAndLookups;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W2Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W3Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W4Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W5Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W1HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W2HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W3HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W4HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W5HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_RecordID;
import uk.ac.leeds.ccg.andyt.stats.Generic_Statistics;

/**
 *
 * @author geoagdt
 */
public class WaAS_HHOLD_Handler extends WaAS_Handler {

    public WaAS_HHOLD_Handler(WaAS_Environment e) {
        super(e);
    }

    @Override
    public String getType() {
        return WaAS_Strings.s_hhold;
    }
    
    /**
     * Load All Wave 5 records.
     *
     * @return a TreeMap with keys as CASEW5 and values as
     * WaAS_Wave5_HHOLD_Records.
     * @throws java.io.FileNotFoundException If the input file is not found.
     */
    public TreeMap<WaAS_W5ID, WaAS_W5HRecord> loadAllW5() 
            throws FileNotFoundException, IOException {
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
            BufferedReader br = io.getBufferedReader(f);
            br.readLine(); // skip header
            String line = br.readLine();
            int i = 0;
            while (line != null) {
                WaAS_W5HRecord rec = new WaAS_W5HRecord(new WaAS_RecordID(i), line);
                i++;
                line = br.readLine();
                short CASEW5 = rec.getCASEW5();
                WaAS_W5ID w5ID = new WaAS_W5ID(CASEW5);
                r.put(w5ID, rec);
                we.data.CASEW5_To_w5.put(CASEW5, w5ID);
            }
            // Close br
            io.closeBufferedReader(br);
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
     * @throws java.io.FileNotFoundException If the input file is not found.
     */
    public TreeMap<WaAS_W4ID, WaAS_W4HRecord> loadAllW4() 
            throws FileNotFoundException, IOException {
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
            BufferedReader br = io.getBufferedReader(f);
            br.readLine(); // skip header
            String line = br.readLine();
            int i = 0;
            while (line != null) {
                WaAS_W4HRecord rec = new WaAS_W4HRecord(new WaAS_RecordID(i), line);
                i++;
                line = br.readLine();
                short CASEW4 = rec.getCASEW4();
                WaAS_W4ID w4ID = new WaAS_W4ID(CASEW4);
                r.put(w4ID, rec);
                we.data.CASEW4_To_w4.put(CASEW4, w4ID);
            }
            // Close br
            io.closeBufferedReader(br);
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
     * @throws java.io.FileNotFoundException If the input file is not found.
     */
    public TreeMap<WaAS_W3ID, WaAS_W3HRecord> loadAllW3() 
            throws FileNotFoundException, IOException {
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
            BufferedReader br = io.getBufferedReader(f);
            br.readLine(); // skip header
            String line = br.readLine();
            int i = 0;
            while (line != null) {
                WaAS_W3HRecord rec = new WaAS_W3HRecord(new WaAS_RecordID(i), line);
                i++;
                line = br.readLine();
                short CASEW3 = rec.getCASEW3();
                WaAS_W3ID w3ID = new WaAS_W3ID(CASEW3);
                r.put(w3ID, rec);
                we.data.CASEW3_To_w3.put(CASEW3, w3ID);
            }
            // Close br
            io.closeBufferedReader(br);
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
     * @throws java.io.FileNotFoundException If the input file is not found.
     */
    public TreeMap<WaAS_W2ID, WaAS_W2HRecord> loadAllW2() 
            throws FileNotFoundException, IOException {
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
            BufferedReader br = io.getBufferedReader(f);
            br.readLine(); // skip header
            String line = br.readLine();
            int i = 0;
            while (line != null) {
                WaAS_W2HRecord rec = new WaAS_W2HRecord(new WaAS_RecordID(i), line);
                i++;
                line = br.readLine();
                short CASEW2 = rec.getCASEW2();
                WaAS_W2ID w2ID = new WaAS_W2ID(CASEW2);
                r.put(w2ID, rec);
                we.data.CASEW2_To_w2.put(CASEW2, w2ID);
            }
            // Close br
            io.closeBufferedReader(br);
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
     * @throws java.io.FileNotFoundException If the input file is not found.
     */
    public TreeMap<WaAS_W1ID, WaAS_W1HRecord> loadAllW1() 
            throws FileNotFoundException, IOException {
        String m = "loadAllW1(data)";
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
            BufferedReader br = io.getBufferedReader(f);
            br.readLine(); // skip header
            String line = br.readLine();
            int i = 0;
            while (line != null) {
                WaAS_W1HRecord rec = new WaAS_W1HRecord(new WaAS_RecordID(i), line);
                i++;
                line = br.readLine();
                short CASEW1 = rec.getCASEW1();
                WaAS_W1ID w1ID = new WaAS_W1ID(CASEW1);
                r.put(w1ID, rec);
                we.data.CASEW1_To_w1.put(CASEW1, w1ID);
            }
            // Close br
            io.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W1, cf, r);
        }
        env.logEndTag(m);
        return r;
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
        return new File(we.files.getGeneratedWaASDir(), getType() + WaAS_Strings.s_W
                + wave + WaAS_Strings.s_All + we.files.DOT_DAT);
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
     * @param subset Subset of CASEW1 for all records to be included.
     * @return Map with keys as GOR and Values as map with keys as CASEWX and
     * values as HVALUE.
     */
    public HashMap<Byte, HashMap<WaAS_WID, Double>> getVariableForGORSubsets(
            String variableName, byte wave,
            WaAS_GORSubsetsAndLookups GORSubsetsAndLookups, 
            HashSet<WaAS_W1ID> subset) {
        HashMap<Byte, HashMap<WaAS_WID, Double>> r = new HashMap<>();
        Iterator<Byte> ite = GORSubsetsAndLookups.gor_To_w1.keySet().iterator();
        while (ite.hasNext()) {
            r.put(ite.next(), new HashMap<>());
        }
        if (wave == W1) {
            we.data.dataSimple.keySet().stream().forEach(cID -> {
                Data_Collection c = we.data.getCollection(cID);
                c.data.keySet().stream().forEach(i -> {
                    WaAS_W1ID w1ID = (WaAS_W1ID) i;
                    if (subset.contains(w1ID)) {
                        WaAS_CombinedRecord cr = (WaAS_CombinedRecord) c.data.get(w1ID);
                        WaAS_W1HRecord w1 = cr.w1Rec.getHr();
                        Byte GOR = GORSubsetsAndLookups.w1_To_gor.get(w1ID);
                        Generic_Collections.addToMap(r, GOR, w1ID, w1.getHVALUE());
                    }
                });
                we.data.clearCollection(cID);
            });
        } else if (wave == W2) {
            we.data.data.keySet().stream().forEach(cID -> {
                Data_Collection c = we.data.getCollection(cID);
                c.data.keySet().stream().forEach(i -> {
                    WaAS_W1ID w1ID = (WaAS_W1ID) i;
                    if (subset.contains(w1ID)) {
                        WaAS_CombinedRecord cr = (WaAS_CombinedRecord) c.data.get(w1ID);
                        HashMap<WaAS_W2ID, WaAS_W2Record> recs = cr.w2Recs;
                        Iterator<WaAS_W2ID> ite2 = recs.keySet().iterator();
                        while (ite2.hasNext()) {
                            WaAS_W2ID w2ID = ite2.next();
                            Byte GOR = GORSubsetsAndLookups.w2_To_gor.get(w2ID);
                            WaAS_W2HRecord w2 = recs.get(w2ID).getHr();
                            Generic_Collections.addToMap(r, GOR, w2ID, w2.getHVALUE());
                        }
                    }
                });
                we.data.clearCollection(cID);
            });
        } else if (wave == W3) {
            we.data.data.keySet().stream().forEach(cID -> {
                Data_Collection c = we.data.getCollection(cID);
                c.data.keySet().stream().forEach(i -> {
                    WaAS_W1ID w1ID = (WaAS_W1ID) i;
                    if (subset.contains(w1ID)) {
                        WaAS_CombinedRecord cr = (WaAS_CombinedRecord) c.data.get(w1ID);
                        HashMap<WaAS_W2ID, HashMap<WaAS_W3ID, WaAS_W3Record>> recs = cr.w3Recs;
                        Iterator<WaAS_W2ID> ite1 = recs.keySet().iterator();
                        while (ite1.hasNext()) {
                            WaAS_W2ID w2ID = ite1.next();
                            HashMap<WaAS_W3ID, WaAS_W3Record> w3_2 = recs.get(w2ID);
                            Iterator<WaAS_W3ID> ite2 = w3_2.keySet().iterator();
                            while (ite2.hasNext()) {
                                WaAS_W3ID w3ID = ite2.next();
                                Byte GOR = GORSubsetsAndLookups.w3_To_gor.get(w3ID);
                                WaAS_W3HRecord w3 = w3_2.get(w3ID).getHr();
                                Generic_Collections.addToMap(r, GOR, w3ID, w3.getHVALUE());
                            }
                        }
                    }
                });
                we.data.clearCollection(cID);
            });
        } else if (wave == W4) {
            we.data.data.keySet().stream().forEach(cID -> {
                Data_Collection c = we.data.getCollection(cID);
                c.data.keySet().stream().forEach(i -> {
                    WaAS_W1ID w1ID = (WaAS_W1ID) i;
                    if (subset.contains(w1ID)) {
                        WaAS_CombinedRecord cr = (WaAS_CombinedRecord) c.data.get(w1ID);
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
                                    Byte GOR = GORSubsetsAndLookups.w4_To_gor.get(w4ID);
                                    WaAS_W4HRecord w4 = w4_3.get(w4ID).getHr();
                                    Generic_Collections.addToMap(r, GOR, w4ID, w4.getHVALUE());
                                }
                            }
                        }
                    }
                });
                we.data.clearCollection(cID);
            });
        } else if (wave == W5) {
            we.data.data.keySet().stream().forEach(cID -> {
                Data_Collection c = we.data.getCollection(cID);
                c.data.keySet().stream().forEach(i -> {
                    WaAS_W1ID w1ID = (WaAS_W1ID) i;
                    if (subset.contains(w1ID)) {
                        WaAS_CombinedRecord cr = (WaAS_CombinedRecord) c.data.get(w1ID);
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
                                        Byte GOR = GORSubsetsAndLookups.w5_To_gor.get(w5ID);
                                        WaAS_W5HRecord w5 = w5_4.get(w5ID).getHr();
                                        Generic_Collections.addToMap(r, GOR, w5ID, w5.getHVALUE());
                                    }
                                }
                            }
                        }
                    }
                });
                we.data.clearCollection(cID);
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
     * @param subset
     * @return
     */
    public TreeMap<Byte, Double> getChangeVariableSubset(String variableName,
            ArrayList<Byte> gors, WaAS_GORSubsetsAndLookups GORSubsetsAndLookups,
            TreeMap<Byte, String> GORNameLookup, HashSet<WaAS_W1ID> subset) {
        TreeMap<Byte, Double> r = new TreeMap<>();
        HashMap<Byte, HashMap<WaAS_WID, Double>>[] variableSubsets;
        variableSubsets = new HashMap[NWAVES];
        for (byte w = 0; w < NWAVES; w++) {
            variableSubsets[w] = getVariableForGORSubsets(variableName,
                    (byte) (w + 1), GORSubsetsAndLookups, subset);
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
        for (byte w = 1; w < NWAVES + 1; w++) {
            h += "," + variableName + "W" + w + "_Count," + variableName + "W"
                    + w + "_ZeroCount," + variableName + "W" + w
                    + "_NegativeCount," + variableName + "W" + w + "_Average";
        }
        env.log(h);
        Iterator<Byte> ite = gors.iterator();
        while (ite.hasNext()) {
            byte gor = ite.next();
            double[][] var = new double[NWAVES][];
            for (byte w = 0; w < NWAVES; w++) {
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
            for (byte w = 0; w < NWAVES; w++) {
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
     * @param <K>
     * @param vName Variable name
     * @param gors
     * @param GORNameLookup
     * @return
     */
    public <K> TreeMap<Byte, Double> getChangeVariableAll(String vName,
            ArrayList<Byte> gors, TreeMap<Byte, String> GORNameLookup) throws FileNotFoundException, IOException {
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
        for (byte w = 1; w < NWAVES + 1; w++) {
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
            double[][] v = new double[NWAVES][];
            for (byte w = 0; w < NWAVES; w++) {
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
            for (byte w = 0; w < NWAVES; w++) {
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
