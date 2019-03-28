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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Object;
import uk.ac.leeds.ccg.andyt.generic.io.Generic_IO;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Strings;
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
import uk.ac.leeds.ccg.andyt.generic.data.waas.io.WaAS_Files;
import uk.ac.leeds.ccg.andyt.generic.util.Generic_Collections;

/**
 *
 * @author geoagdt
 */
public abstract class WaAS_Handler extends WaAS_Object {

    public transient WaAS_Files files;
    protected transient final byte W1;
    protected transient final byte W2;
    protected transient final byte W3;
    protected transient final byte W4;
    protected transient final byte W5;

    protected final String TYPE;

    public WaAS_Handler(WaAS_Environment e, String type) {
        super(e);
        files = e.files;
        W1 = WaAS_Data.W1;
        W2 = WaAS_Data.W2;
        W3 = WaAS_Data.W3;
        W4 = WaAS_Data.W4;
        W5 = WaAS_Data.W5;
        TYPE = type;
    }

    /**
     * @param wave the wave for which the source input File is returned.
     * @return the source input File for a particular WaAS Wave.
     */
    public File getInputFile(byte wave) {
        String filename;
        filename = "was_wave_" + wave + "_" + TYPE + "_eul_final";
        if (wave < 4) {
            filename += "_v2";
        }
        filename += ".tab";
        return new File(files.getInputWaASDir(), filename);
    }

    protected Object load(byte wave, File f) {
        String m = "load " + getString0(wave, f);
        env.logStartTag(m);
        Object r = Generic_IO.readObject(f);
        env.logEndTag(m);
        return r;
    }

    protected String getString0(byte wave, File f) {
        return getString0(wave) + WaAS_Strings.symbol_space
                + WaAS_Strings.s_WaAS + WaAS_Strings.symbol_space
                + WaAS_Strings.s_file + WaAS_Strings.symbol_space + f.toString();
    }

    /**
     * A simple wrapper for
     * {@link Generic_IO#writeObject(java.lang.Object, java.io.File)}
     *
     * @param wave The wave to be cached.
     * @param f The File to cache to.
     * @param o The Object to cache.
     */
    protected void cache(byte wave, File f, Object o) {
        String m = "cache " + getString0(wave, f);
        env.logStartTag(m);
        Generic_IO.writeObject(o, f);
        env.logEndTag(m);
    }

    /**
     *
     * @param wave The wave to be cached.
     * @param type The name of the type of subset to be cached.
     * @return
     */
    public File getSubsetCacheFile(byte wave, String type) {
        return new File(files.getGeneratedWaASSubsetsDir(),
                TYPE + WaAS_Strings.s_W + wave + WaAS_Strings.symbol_underscore
                + type + WaAS_Files.DOT_DAT);
    }

    /**
     *
     * @param wave The wave to be cached.
     * @param type The name of the type of subset to be cached.
     * @return
     */
    public File getSubsetCacheFile2(byte wave, String type) {
        return new File(files.getGeneratedWaASSubsetsDir(),
                TYPE + WaAS_Strings.s_W + wave + WaAS_Strings.symbol_underscore
                + type + "2" + WaAS_Files.DOT_DAT);
    }

    /**
     *
     * @param wave The wave to be cached.
     * @param o The subset to be cached
     * @param type The name of the type of subset to be cached.
     */
    public void cacheSubset(byte wave, Object o, String type) {
        cache(wave, getSubsetCacheFile(wave, type), o);
    }

    /**
     * Writes to file the subset look ups.
     *
     * @param wave The wave of lookups from and to to be cached.
     * @param m0 The lookups from wave to (wave + 1).
     * @param m1 The lookups from (wave + 1) to wave.
     */
    public void cacheSubsetLookups(byte wave, TreeMap<Short, HashSet<Short>> m0,
            TreeMap<Short, Short> m1) {
        cache(wave, getSubsetLookupToFile(wave), m0);
        cache(wave, getSubsetLookupFromFile(wave), m1);
    }

    /**
     * @param wave
     * @return the File for a subset lookup to wave.
     */
    public File getSubsetLookupToFile(byte wave) {
        return new File(files.getGeneratedWaASSubsetsDir(),
                getString0(wave) + getStringToWaveDotDat(wave + 1));
    }

    /**
     * @param wave
     * @return the File for a subset lookup from wave.
     */
    public File getSubsetLookupFromFile(byte wave) {
        return new File(files.getGeneratedWaASSubsetsDir(),
                getString0(wave + 1) + getStringToWaveDotDat(wave));
    }

    public TreeMap<Short, HashSet<Short>> loadSubsetLookupTo(byte wave) {
        return (TreeMap<Short, HashSet<Short>>) Generic_IO.readObject(
                getSubsetLookupToFile(wave));
    }

    public TreeMap<Short, Short> loadSubsetLookupFrom(byte wave) {
        return (TreeMap<Short, Short>) Generic_IO.readObject(
                getSubsetLookupFromFile(wave));
    }

    protected String getString0(int wave) {
        return TYPE + WaAS_Strings.s_W + wave;
    }

    protected String getString1(byte wave, short cID) {
        return getString0(wave) + WaAS_Strings.symbol_underscore + cID;
    }

    protected String getStringToWaveDotDat(int wave) {
        return WaAS_Strings.s_To + WaAS_Strings.s_W + wave + WaAS_Files.DOT_DAT;
    }

    public void cacheSubsetCollection(short cID, byte wave, Object o) {
        WaAS_Handler.this.cache(wave, getSubsetCollectionFile(cID, wave), o);
    }

    public Object loadSubsetCollection(short cID, byte wave) {
        return load(wave, getSubsetCollectionFile(cID, wave));
    }

    public File getSubsetCollectionFile(short cID, byte wave) {
        return new File(files.getGeneratedWaASSubsetsDir(),
                getString1(wave, cID) + WaAS_Files.DOT_DAT);
    }

    /**
     * 1,2,4,5,6,7,8,9,10,11,12
     *
     * @return
     */
    public static ArrayList<Byte> getGORs() {
        ArrayList<Byte> gors;
        gors = new ArrayList<>();
        for (byte gor = 1; gor < 13; gor++) {
            if (gor != 3) {
                gors.add(gor);
            }
        }
        return gors;
    }

    /**
     * Init GORSubsets and GORLookups
     *
     * @param name
     * @param data
     * @param gors
     * @param subset
     * @return
     */
    public Object[] getGORSubsetsAndLookup(String name, WaAS_Data data,
            ArrayList<Byte> gors, HashSet<Short> subset) {
        Object[] r;
        File f = new File(files.getOutputDataDir(), name + "GORSubsetsAndLookups.dat");
        if (f.exists()) {
            r = (Object[]) Generic_IO.readObject(f);
        } else {
            r = new Object[2];
            HashMap<Byte, HashSet<Short>>[] r0 = new HashMap[WaAS_Data.NWAVES];
            r[0] = r0;
            HashMap<Short, Byte>[] r1 = new HashMap[WaAS_Data.NWAVES];
            r[1] = r1;
            for (byte w = 0; w < WaAS_Data.NWAVES; w++) {
                r0[w] = new HashMap<>();
                Iterator<Byte> ite = gors.iterator();
                while (ite.hasNext()) {
                    byte gor = ite.next();
                    r0[w].put(gor, new HashSet<>());
                }
                r1[w] = new HashMap<>();
            }
            // Wave 1
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr = c.getData().get(CASEW1);
                        byte GOR = cr.w1Record.getHhold().getGOR();
                        Generic_Collections.addToMap(r0[0], GOR, CASEW1);
                        r1[0].put(CASEW1, GOR);
                    }
                });
                data.clearCollection(cID);
            });
            // Wave 2
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
                            WaAS_Wave2_Record w2 = w2Records.get(CASEW2);
                            byte GOR = w2.getHhold().getGOR();
                            Generic_Collections.addToMap(r0[1], GOR, CASEW2);
                            r1[1].put(CASEW2, GOR);
                        }
                    }
                });
                data.clearCollection(cID);
            });
            // Wave 3
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr = c.getData().get(CASEW1);
                        HashMap<Short, HashMap<Short, WaAS_Wave3_Record>> w3Records;
                        w3Records = cr.w3Records;
                        Iterator<Short> ite2 = w3Records.keySet().iterator();
                        while (ite2.hasNext()) {
                            Short CASEW2 = ite2.next();
                            HashMap<Short, WaAS_Wave3_Record> w3_2;
                            w3_2 = w3Records.get(CASEW2);
                            Iterator<Short> ite3 = w3_2.keySet().iterator();
                            while (ite3.hasNext()) {
                                Short CASEW3 = ite3.next();
                                WaAS_Wave3_Record w3 = w3_2.get(CASEW3);
                                byte GOR = w3.getHhold().getGOR();
                                Generic_Collections.addToMap(r0[2], GOR, CASEW3);
                                r1[2].put(CASEW3, GOR);
                            }
                        }
                    }
                });
                data.clearCollection(cID);
            });
            // Wave 4
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr = c.getData().get(CASEW1);
                        HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave4_Record>>> w4Records;
                        w4Records = cr.w4Records;
                        Iterator<Short> ite2 = w4Records.keySet().iterator();
                        while (ite2.hasNext()) {
                            Short CASEW2 = ite2.next();
                            HashMap<Short, HashMap<Short, WaAS_Wave4_Record>> w4_2;
                            w4_2 = w4Records.get(CASEW2);
                            Iterator<Short> ite3 = w4_2.keySet().iterator();
                            while (ite3.hasNext()) {
                                Short CASEW3 = ite3.next();
                                HashMap<Short, WaAS_Wave4_Record> w4_3;
                                w4_3 = w4_2.get(CASEW3);
                                Iterator<Short> ite4 = w4_3.keySet().iterator();
                                while (ite4.hasNext()) {
                                    Short CASEW4 = ite4.next();
                                    WaAS_Wave4_Record w4 = w4_3.get(CASEW4);
                                    byte GOR = w4.getHhold().getGOR();
                                    Generic_Collections.addToMap(r0[3], GOR, CASEW4);
                                    r1[3].put(CASEW4, GOR);
                                }
                            }
                        }
                    }
                });
                data.clearCollection(cID);
            });
            // Wave 5
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c;
                c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_Combined_Record cr = c.getData().get(CASEW1);
                        HashMap<Short, HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>>> w5Records;
                        w5Records = cr.w5Records;
                        Iterator<Short> ite2 = w5Records.keySet().iterator();
                        while (ite2.hasNext()) {
                            Short CASEW2 = ite2.next();
                            HashMap<Short, HashMap<Short, HashMap<Short, WaAS_Wave5_Record>>> w5_2;
                            w5_2 = w5Records.get(CASEW2);
                            Iterator<Short> ite3 = w5_2.keySet().iterator();
                            while (ite3.hasNext()) {
                                Short CASEW3 = ite3.next();
                                HashMap<Short, HashMap<Short, WaAS_Wave5_Record>> w5_3;
                                w5_3 = w5_2.get(CASEW3);
                                Iterator<Short> ite4 = w5_3.keySet().iterator();
                                while (ite4.hasNext()) {
                                    Short CASEW4 = ite4.next();
                                    HashMap<Short, WaAS_Wave5_Record> w5_4;
                                    w5_4 = w5_3.get(CASEW4);
                                    Iterator<Short> ite5 = w5_4.keySet().iterator();
                                    while (ite5.hasNext()) {
                                        Short CASEW5 = ite5.next();
                                        WaAS_Wave5_Record w5 = w5_4.get(CASEW5);
                                        byte GOR = w5.getHhold().getGOR();
                                        Generic_Collections.addToMap(r0[4], GOR, CASEW5);
                                        r1[4].put(CASEW5, GOR);
                                    }
                                }
                            }
                        }
                    }
                });
                data.clearCollection(cID);
            });
            Generic_IO.writeObject(r, f);
        }
        return r;
    }

    /**
     * Value label information for Government Office Regions
     *
     * @return
     */
    public static TreeMap<Byte, String> getGORNameLookup() {
        TreeMap<Byte, String> r;
        r = new TreeMap<>();
        r.put((byte) 1, "North East");
        r.put((byte) 2, "North West");
        r.put((byte) 3, "The Wirral");
        r.put((byte) 4, "Yorkshire & Humber");
        r.put((byte) 5, "East Midlands");
        r.put((byte) 6, "West Midlands");
        r.put((byte) 7, "East of England");
        r.put((byte) 8, "London");
        r.put((byte) 9, "South East");
        r.put((byte) 10, "South West");
        r.put((byte) 11, "Wales");
        r.put((byte) 12, "Scotland");
        return r;
    }

    /**
     * Go through hholds for all waves and figure which ones have not
     * significantly changed in terms of hhold composition. Having children and
     * children leaving home is fine. Anything else is perhaps an issue...
     *
     * @param data
     * @return
     */
    public HashSet<Short> getStableHouseholdCompositionSubset(WaAS_Data data) {
        String m = "getStableHouseholdCompositionSubset";
        env.logStartTag(m);
        HashSet<Short> r;
        String fn = "SameCompositionHashSet_CASEW1.dat";
        File f = new File(files.getOutputDataDir(), fn);
        if (f.exists()) {
            r = (HashSet<Short>) Generic_IO.readObject(f);
        } else {
            r = new HashSet<>();
            env.log("Number of combined records " + data.CASEW1ToCID.size());
            env.log("Number of collections of combined records " + data.data.size());
            int count0 = 0;
            int count1 = 0;
            int count2 = 0;
            int count3 = 0;
            // Check For Household Records
            String m1 = "Check For Household Records";
            env.logStartTag(m1);
            Iterator<Short> ite = data.data.keySet().iterator();
            while (ite.hasNext()) {
                short cID = ite.next();
                WaAS_Collection c = data.getCollection(cID);
                HashMap<Short, WaAS_Combined_Record> cData = c.getData();
                Iterator<Short> ite2 = cData.keySet().iterator();
                while (ite2.hasNext()) {
                    short CASEW1 = ite2.next();
                    WaAS_Combined_Record cr = cData.get(CASEW1);
                    boolean check = process0(CASEW1, cr);
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
                        r.add(CASEW1);
                    }
                }
                data.clearCollection(cID);
            }
            env.log("" + count0 + " Total hholds in all 5 waves.");
            env.log("" + count1 + " Total hholds that are a single hhold over all 5 "
                    + "waves.");
            env.log("" + count2 + " Total hholds that are a single hhold over all 5 "
                    + "waves and have same number of adults in all 5 waves.");
            env.log("" + count3 + " Total hholds that are a single hhold over all 5 "
                    + "waves and have the same basic adult household composition "
                    + "over all 5 waves.");
            env.logEndTag(m1);
            env.logEndTag(m);
            Generic_IO.writeObject(r, f);
        }
        env.log("Total number of initial households in wave 1 " + r.size());
        env.logEndTag(m);
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
        boolean r = true;
        if (cr.w1Record == null) {
            env.log("There is no Wave 1 record for CASEW1 " + CASEW1);
            r = false;
        }
        if (cr.w2Records.isEmpty()) {
            env.log("There are no Wave 2 records for CASEW1 " + CASEW1);
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
                                            env.log("!w5_3.containsKey(CASEW4) " + m5);
                                        }
                                    } else {
                                        env.log("!w5_2.containsKey(CASEW3) " + m5);
                                        r = false;
                                    }
                                } else {
                                    env.log("!cr.w5Records.containsKey(CASEW2) " + m5);
                                    r = false;
                                }
                            }
                        } else {
                            env.log("!w4_2.containsKey(CASEW3) " + m4);
                            r = false;
                        }
                    } else {
                        env.log("!cr.w4Records.containsKey(CASEW2) " + m4);
                        r = false;
                    }
                }
            } else {
                env.log("!cr.w3Records.containsKey(CASEW2) " + m3);
                r = false;
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
            env.log("There are multiple Wave 2 records for CASEW1 " + CASEW1);
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
                    env.log(m3);
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
                                    env.log(m4);
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
                                                        env.log(m5);
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
                                                    env.log("!w5_3.containsKey(CASEW4) " + m5);
                                                }
                                            } else {
                                                env.log("!w5_2.containsKey(CASEW3) " + m5);
                                                r = false;
                                            }
                                        } else {
                                            env.log("!cr.w5Records.containsKey(CASEW2) " + m5);
                                            r = false;
                                        }
                                    }
                                }
                            } else {
                                env.log("!w4_2.containsKey(CASEW3) " + m4);
                                r = false;
                            }
                        } else {
                            env.log("!cr.w4Records.containsKey(CASEW2) " + m4);
                            r = false;
                        }
                    }
                }
            } else {
                env.log(m3);
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
            env.log("There are multiple Wave 2 records for CASEW1 " + CASEW1);
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
                    env.log(m3);
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
                                    env.log(m4);
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
                                                        env.log(m5);
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
                                                    env.log("!w5_3.containsKey(CASEW4) " + m5);
                                                }
                                            } else {
                                                env.log("!w5_2.containsKey(CASEW3) " + m5);
                                                r = false;
                                            }
                                        } else {
                                            env.log("!cr.w5Records.containsKey(CASEW2) " + m5);
                                            r = false;
                                        }
                                    }
                                }
                            } else {
                                env.log("!w4_2.containsKey(CASEW3) " + m4);
                                r = false;
                            }
                        } else {
                            env.log("!cr.w4Records.containsKey(CASEW2) " + m4);
                            r = false;
                        }
                    }
                }
            } else {
                env.log(m3);
            }
        }
        return r;
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
        boolean r = true;
        if (cr.w2Records.size() > 1) {
            env.log("There are multiple Wave 2 records for CASEW1 " + CASEW1);
            r = false;
        }
        Iterator<Short> ite2 = cr.w2Records.keySet().iterator();
        while (ite2.hasNext()) {
            Short CASEW2 = ite2.next();
            WaAS_Wave2_Record w2rec = cr.w2Records.get(CASEW2);
            String m3 = "There are multiple Wave 3 records for "
                    + "CASEW2 " + CASEW2 + " in CASEW1 " + CASEW1;
            if (cr.w3Records.containsKey(CASEW2)) {
                HashMap<Short, WaAS_Wave3_Record> w3_2;
                w3_2 = cr.w3Records.get(CASEW2);
                if (w3_2.size() > 1) {
                    env.log(m3);
                    r = false;
                } else {
                    Iterator<Short> ite3 = w3_2.keySet().iterator();
                    while (ite3.hasNext()) {
                        Short CASEW3 = ite3.next();
                        WaAS_Wave3_Record w3rec = w3_2.get(CASEW3);
                        String m4 = "There are multiple Wave 4 records for "
                                + "CASEW3 " + CASEW3 + " in CASEW2 " + CASEW2
                                + " in CASEW1 " + CASEW1;
                        if (cr.w4Records.containsKey(CASEW2)) {
                            HashMap<Short, HashMap<Short, WaAS_Wave4_Record>> w4_2;
                            w4_2 = cr.w4Records.get(CASEW2);
                            if (w4_2.containsKey(CASEW3)) {
                                HashMap<Short, WaAS_Wave4_Record> w4_3;
                                w4_3 = w4_2.get(CASEW3);
                                if (w4_3.size() > 1) {
                                    env.log(m4);
                                    r = false;
                                } else {
                                    Iterator<Short> ite4;
                                    ite4 = w4_3.keySet().iterator();
                                    while (ite4.hasNext()) {
                                        Short CASEW4 = ite4.next();
                                        WaAS_Wave4_Record w4rec;
                                        w4rec = w4_3.get(CASEW4);
                                        String m5 = "There are multiple Wave 5 "
                                                + "records for "
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
                                                        env.log(m5);
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
                                                    env.log("!w5_3.containsKey(CASEW4) " + m5);
                                                }
                                            } else {
                                                env.log("!w5_2.containsKey(CASEW3) " + m5);
                                                r = false;
                                            }
                                        } else {
                                            env.log("!cr.w5Records.containsKey(CASEW2) " + m5);
                                            r = false;
                                        }
                                    }
                                }
                            } else {
                                env.log("!w4_2.containsKey(CASEW3) " + m4);
                                r = false;
                            }
                        } else {
                            env.log("!cr.w4Records.containsKey(CASEW2) " + m4);
                            r = false;
                        }
                    }
                }
            } else {
                env.log(m3);
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
    
    
}
