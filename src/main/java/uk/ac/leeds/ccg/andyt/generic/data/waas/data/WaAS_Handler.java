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

    protected void cache(byte wave, File f, Object o) {
        String m = "cache " + getString0(wave, f);
        env.logStartTag(m);
        Generic_IO.writeObject(o, f);
        env.logEndTag(m);
    }

    /**
     *
     * @param wave
     * @param type
     * @return
     */
    public File getSubsetCacheFile(byte wave, String type) {
        return new File(files.getGeneratedWaASSubsetsDir(),
                TYPE + WaAS_Strings.s_W + wave + WaAS_Strings.symbol_underscore
                + type + WaAS_Files.DOT_DAT);
    }

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
     * Get the GOR Subsets for subset.
     *
     * @param data
     * @param gors
     * @param subset
     * @return
     */
    public static Object[] getGORSubsetsAndLookup(WaAS_Data data,
            ArrayList<Byte> gors, HashSet<Short> subset) {
        Object[] r = new Object[2];
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
}
