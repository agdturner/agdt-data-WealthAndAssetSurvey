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
import uk.ac.leeds.ccg.andyt.data.interval.Data_IntervalLong1;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Object;
import uk.ac.leeds.ccg.andyt.generic.io.Generic_IO;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Strings;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W1HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W2HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W3HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W4HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W5HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_W1W2W3W4W5PRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_W1PRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_W2PRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_W3PRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_W4PRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_W5PRecord;
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
//    public void cacheSubsetLookups(byte wave, TreeMap<Short, HashSet<Short>> m0,
//            TreeMap<Short, Short> m1) {
    public <K, V> void cacheSubsetLookups(byte wave, TreeMap<K, HashSet<V>> m0,
            TreeMap<V, K> m1) {
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

    public TreeMap<WaAS_W1ID, HashSet<WaAS_W2ID>> loadSubsetLookupToW1() {
        return (TreeMap<WaAS_W1ID, HashSet<WaAS_W2ID>>) Generic_IO.readObject(
                getSubsetLookupToFile(W1));
    }

    public TreeMap<WaAS_W2ID, HashSet<WaAS_W3ID>> loadSubsetLookupToW2() {
        return (TreeMap<WaAS_W2ID, HashSet<WaAS_W3ID>>) Generic_IO.readObject(
                getSubsetLookupToFile(W2));
    }

    public TreeMap<WaAS_W3ID, HashSet<WaAS_W4ID>> loadSubsetLookupToW3() {
        return (TreeMap<WaAS_W3ID, HashSet<WaAS_W4ID>>) Generic_IO.readObject(
                getSubsetLookupToFile(W3));
    }

    public TreeMap<WaAS_W4ID, HashSet<WaAS_W5ID>> loadSubsetLookupToW4() {
        return (TreeMap<WaAS_W4ID, HashSet<WaAS_W5ID>>) Generic_IO.readObject(
                getSubsetLookupToFile(W4));
    }

    public TreeMap<WaAS_W2ID, WaAS_W1ID> loadSubsetLookupFromW1() {
        return (TreeMap<WaAS_W2ID, WaAS_W1ID>) Generic_IO.readObject(
                getSubsetLookupFromFile(W1));
    }

    public TreeMap<WaAS_W3ID, WaAS_W2ID> loadSubsetLookupFromW2() {
        return (TreeMap<WaAS_W3ID, WaAS_W2ID>) Generic_IO.readObject(
                getSubsetLookupFromFile(W2));
    }

    public TreeMap<WaAS_W4ID, WaAS_W3ID> loadSubsetLookupFromW3() {
        return (TreeMap<WaAS_W4ID, WaAS_W3ID>) Generic_IO.readObject(
                getSubsetLookupFromFile(W3));
    }

    public TreeMap<WaAS_W5ID, WaAS_W4ID> loadSubsetLookupFromW4() {
        return (TreeMap<WaAS_W5ID, WaAS_W4ID>) Generic_IO.readObject(
                getSubsetLookupFromFile(W1));
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
    public WaAS_GORSubsetsAndLookups getGORSubsetsAndLookup(String name, WaAS_Data data,
            ArrayList<Byte> gors, HashSet<WaAS_W1ID> subset) {
        WaAS_GORSubsetsAndLookups r;
        File f = new File(files.getOutputDataDir(), name + "GORSubsetsAndLookups.dat");
        if (f.exists()) {
            r = (WaAS_GORSubsetsAndLookups) Generic_IO.readObject(f);
        } else {
            r = new WaAS_GORSubsetsAndLookups(gors);
            // Wave 1
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_CombinedRecord cr = c.getData().get(CASEW1);
                        byte GOR = cr.w1Record.getHhold().getGOR();
                        Generic_Collections.addToMap(r.GOR2W1IDSet, GOR, CASEW1);
                        r.W1ID2GOR.put(CASEW1, GOR);
                    }
                });
                data.clearCollection(cID);
            });
            // Wave 2
            data.data.keySet().stream().forEach(cID -> {
                WaAS_Collection c = data.getCollection(cID);
                c.getData().keySet().stream().forEach(CASEW1 -> {
                    if (subset.contains(CASEW1)) {
                        WaAS_CombinedRecord cr = c.getData().get(CASEW1);
                        HashMap<WaAS_W2ID, WaAS_W2Record> w2Records;
                        w2Records = cr.w2Records;
                        Iterator<WaAS_W2ID> ite2 = w2Records.keySet().iterator();
                        while (ite2.hasNext()) {
                            WaAS_W2ID CASEW2 = ite2.next();
                            WaAS_W2Record w2 = w2Records.get(CASEW2);
                            byte GOR = w2.getHhold().getGOR();
                            Generic_Collections.addToMap(r.GOR2W2IDSet, GOR, CASEW2);
                            r.W2ID2GOR.put(CASEW2, GOR);
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
                        WaAS_CombinedRecord cr = c.getData().get(CASEW1);
                        HashMap<WaAS_W2ID, HashMap<WaAS_W3ID, WaAS_W3Record>> w3Records;
                        w3Records = cr.w3Records;
                        Iterator<WaAS_W2ID> ite2 = w3Records.keySet().iterator();
                        while (ite2.hasNext()) {
                            WaAS_W2ID CASEW2 = ite2.next();
                            HashMap<WaAS_W3ID, WaAS_W3Record> w3_2;
                            w3_2 = w3Records.get(CASEW2);
                            Iterator<WaAS_W3ID> ite3 = w3_2.keySet().iterator();
                            while (ite3.hasNext()) {
                                WaAS_W3ID CASEW3 = ite3.next();
                                WaAS_W3Record w3 = w3_2.get(CASEW3);
                                byte GOR = w3.getHhold().getGOR();
                                Generic_Collections.addToMap(r.GOR2W3IDSet, GOR, CASEW3);
                                r.W3ID2GOR.put(CASEW3, GOR);
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
                        WaAS_CombinedRecord cr = c.getData().get(CASEW1);
                        HashMap<WaAS_W2ID, HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, WaAS_W4Record>>> w4Records;
                        w4Records = cr.w4Records;
                        Iterator<WaAS_W2ID> ite2 = w4Records.keySet().iterator();
                        while (ite2.hasNext()) {
                            WaAS_W2ID CASEW2 = ite2.next();
                            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, WaAS_W4Record>> w4_2;
                            w4_2 = w4Records.get(CASEW2);
                            Iterator<WaAS_W3ID> ite3 = w4_2.keySet().iterator();
                            while (ite3.hasNext()) {
                                WaAS_W3ID CASEW3 = ite3.next();
                                HashMap<WaAS_W4ID, WaAS_W4Record> w4_3;
                                w4_3 = w4_2.get(CASEW3);
                                Iterator<WaAS_W4ID> ite4 = w4_3.keySet().iterator();
                                while (ite4.hasNext()) {
                                    WaAS_W4ID CASEW4 = ite4.next();
                                    WaAS_W4Record w4 = w4_3.get(CASEW4);
                                    byte GOR = w4.getHhold().getGOR();
                                    Generic_Collections.addToMap(r.GOR2W4IDSet, GOR, CASEW4);
                                    r.W4ID2GOR.put(CASEW4, GOR);
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
                        WaAS_CombinedRecord cr = c.getData().get(CASEW1);
                        HashMap<WaAS_W2ID, HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>>>> w5Records;
                        w5Records = cr.w5Records;
                        Iterator<WaAS_W2ID> ite2 = w5Records.keySet().iterator();
                        while (ite2.hasNext()) {
                            WaAS_W2ID CASEW2 = ite2.next();
                            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>>> w5_2;
                            w5_2 = w5Records.get(CASEW2);
                            Iterator<WaAS_W3ID> ite3 = w5_2.keySet().iterator();
                            while (ite3.hasNext()) {
                                WaAS_W3ID CASEW3 = ite3.next();
                                HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>> w5_3;
                                w5_3 = w5_2.get(CASEW3);
                                Iterator<WaAS_W4ID> ite4 = w5_3.keySet().iterator();
                                while (ite4.hasNext()) {
                                    WaAS_W4ID CASEW4 = ite4.next();
                                    HashMap<WaAS_W5ID, WaAS_W5Record> w5_4;
                                    w5_4 = w5_3.get(CASEW4);
                                    Iterator<WaAS_W5ID> ite5 = w5_4.keySet().iterator();
                                    while (ite5.hasNext()) {
                                        WaAS_W5ID CASEW5 = ite5.next();
                                        WaAS_W5Record w5 = w5_4.get(CASEW5);
                                        byte GOR = w5.getHhold().getGOR();
                                        Generic_Collections.addToMap(r.GOR2W5IDSet, GOR, CASEW5);
                                        r.W5ID2GOR.put(CASEW5, GOR);
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
     * The lookup is the same for the first 5 waves of WaAS. From the
     * documentation we have:
     * <ul>
     * <li>Value = 1.0	Label = Less than £250</li>
     * <li>Value = 2.0	Label = £250 to £499</li>
     * <li>Value = 3.0	Label = £400[sic] to £749</li>
     * <li>Value = 4.0	Label = £750 to £999</li>
     * <li>Value = 5.0	Label = £1,000 to £1,249</li>
     * <li>Value = 6.0	Label = £1,250 to £1,499</li>
     * <li>Value = 7.0	Label = £1,500 to £1,749</li>
     * <li>Value = 8.0	Label = £1,750 to £1,999</li>
     * <li>Value = 9.0	Label = £2,000 to £2,499</li>
     * <li>Value = 10.0	Label = £2,500 to £2,999</li>
     * <li>Value = 11.0	Label = £3,000 to £3,999</li>
     * <li>Value = 12.0	Label = £4,000 to £4,999</li>
     * <li>Value = 13.0	Label = £5,000 to £7,499</li>
     * <li>Value = 14.0	Label = £7,500 to £9,999</li>
     * <li>Value = 15.0	Label = £10,000 or more</li>
     * <li>Value = -9.0	Label = Do not know</li>
     * <li>Value = -8.0	Label = Refusal</li>
     * <li>Value = -7.0	Label = Not applicable</li>
     * <li>Value = -6.0	Label = Error partial</li>
     * </ul>
     *
     * @return
     */
    public TreeMap<Byte, Data_IntervalLong1> getSEESMLookup() {
        String m = "getSEESMLookup";
        env.logStartTag(m);
        TreeMap<Byte, Data_IntervalLong1> r = new TreeMap<>();
        long d = 250L;
        long l = 0L;
        long u = d;
        for (long x = 1; x <= 9; x++) {
            r.put((byte) x, new Data_IntervalLong1(l, u));
            l = u;
            u += d;
        }
        l = 2500L;
        u = 3000L;
        r.put((byte) 10, new Data_IntervalLong1(l, u));
        d = 1000L;
        l = u;
        u += d;
        for (long x = 11; x <= 13; x++) {
            r.put((byte) x, new Data_IntervalLong1(l, u));
            l = u;
            u += d;
        }
        l = 7500L;
        u = 10000L;
        r.put((byte) 14, new Data_IntervalLong1(l, u));
        l = u;
        u = Long.MAX_VALUE;
        r.put((byte) 15, new Data_IntervalLong1(l, u));
        env.logEndTag(m);
        return r;
    }

    /**
     * For Waves 1 and 2 there is the following:
     *
     * <ul>
     * <li>Value = 1.0	Label = Less than £20,000</li>
     * <li>Value = 2.0	Label = £20,000 to £39,999</li>
     * <li>Value = 3.0	Label = £40,000 to £59,999</li>
     * <li>Value = 4.0	Label = £60,000 to £99,999</li>
     * <li>Value = 5.0	Label = £100,000 to £149,999</li>
     * <li>Value = 6.0	Label = £150,000 to £199,999</li>
     * <li>Value = 7.0	Label = £200,000 to £249,999</li>
     * <li>Value = 8.0	Label = £250,000 to £299,999</li>
     * <li>Value = 9.0	Label = £300,000 to £499,999</li>
     * <li>Value = 10.0	Label = £500,000 or more</li>
     * </ul>
     *
     * For Waves 3, 4 and 5 there is the following:
     * <ul>
     * <li>Value = 1.0	Label = Less than £60,000</li>
     * <li>Value = 2.0	Label = £60,000 to £99,999</li>
     * <li>Value = 3.0	Label = £100,000 to £149,999</li>
     * <li>Value = 4.0	Label = £150,000 to £199,999</li>
     * <li>Value = 5.0	Label = £200,000 to £249,999</li>
     * <li>Value = 6.0	Label = £250,000 to £299,999</li>
     * <li>Value = 7.0	Label = £300,000 to £349,999</li>
     * <li>Value = 8.0	Label = £350,000 to £399,999</li>
     * <li>Value = 9.0	Label = £400,000 to £499,999</li>
     * <li>Value = 10.0	Label = £500,000 to £749,999</li>
     * <li>Value = 11.0	Label = £750,000 to £999,999</li>
     * <li>Value = 12.0	Label = £1 million or more</li>
     * </ul>
     *
     * @param w
     * @return
     */
    public TreeMap<Byte, Data_IntervalLong1> getHPRICEBLookup(byte w) {
        String m = "getHPRICEBLookup";
        env.logStartTag(m);
        TreeMap<Byte, Data_IntervalLong1> r = new TreeMap<>();
        if (w == WaAS_Data.W1 || w == WaAS_Data.W2) {
            r.put((byte) 1, new Data_IntervalLong1(0L, 20000L));
            r.put((byte) 2, new Data_IntervalLong1(20000L, 40000L));
            r.put((byte) 3, new Data_IntervalLong1(40000L, 60000L));
            r.put((byte) 4, new Data_IntervalLong1(60000L, 100000L));
            r.put((byte) 5, new Data_IntervalLong1(100000L, 150000L));
            r.put((byte) 6, new Data_IntervalLong1(150000L, 200000L));
            r.put((byte) 7, new Data_IntervalLong1(200000L, 250000L));
            r.put((byte) 8, new Data_IntervalLong1(250000L, 300000L));
            r.put((byte) 9, new Data_IntervalLong1(300000L, 500000L));
            r.put((byte) 9, new Data_IntervalLong1(500000L, Long.MAX_VALUE));
        } else if (w == WaAS_Data.W3 || w == WaAS_Data.W4 || w == WaAS_Data.W5) {
            long l = 0L;
            long u = 60000L;
            r.put((byte) 1, new Data_IntervalLong1(l, u));
            l = u;
            u = 100000L;
            r.put((byte) 2, new Data_IntervalLong1(l, u));
            long d = 100000L;
            for (long x = 3; x <= 9; x++) {
                l = u;
                u += d;
                r.put((byte) x, new Data_IntervalLong1(l, u));
            }
            l = u;
            r.put((byte) 10, new Data_IntervalLong1(l, 750000L));
            l = u;
            r.put((byte) 11, new Data_IntervalLong1(l, 1000000L));
            l = u;
            r.put((byte) 12, new Data_IntervalLong1(l, Long.MAX_VALUE));
        } else {
            env.log("Exception: Unrecognised wave " + w);
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Get subset.
     *
     * @param data
     * @param type <ul>
     * <li>If type = 1 then the subset returned is those that have records in
     * each wave.</li>
     * <li>If type = 2 then the subset returned is those that have one and only
     * one record in each wave.</li>
     * <li>If type = 3 then the subset returned is those that have one and only
     * one record in each wave and the same number of adults.</li>
     * <li>If type = 4 then the subset returned is those that have one and only
     * one record in each wave and the same basic household composition.</li>
     * </ul>
     * @return
     */
    public HashSet<WaAS_W1ID> getSubset(WaAS_Data data, int type) {
        String m = "getSubset";
        env.logStartTag(m);
        HashSet<WaAS_W1ID> r;
        String fn = "Subset" + type + "HashSet_CASEW1.dat";
        File f = new File(files.getOutputDataDir(), fn);
        if (f.exists()) {
            r = (HashSet<WaAS_W1ID>) Generic_IO.readObject(f);
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
            Iterator<WaAS_CollectionID> ite = data.data.keySet().iterator();
            switch (type) {
                case 1:
                    while (ite.hasNext()) {
                        WaAS_CollectionID cID = ite.next();
                        WaAS_Collection c = data.getCollection(cID);
                        HashMap<WaAS_W1ID, WaAS_CombinedRecord> cData = c.getData();
                        Iterator<WaAS_W1ID> ite2 = cData.keySet().iterator();
                        while (ite2.hasNext()) {
                            WaAS_W1ID CASEW1 = ite2.next();
                            WaAS_CombinedRecord cr = cData.get(CASEW1);
                            if (isRecordInEachWave(CASEW1, cr)) {
                                count0++;
                                env.log("" + count0 + "\t records in each wave.");
                                r.add(CASEW1);
                            }
                            if (isOnlyOneRecordInEachWave(CASEW1, cr)) {
                                count1++;
                                env.log("" + count1 + "\t records with only one record in each wave.");
                                //r.add(CASEW1);
                            }
                            if (isSameNumberOfAdultsInEachWave(CASEW1, cr)) {
                                count2++;
                                env.log("" + count2 + "\t records with same number of adults in each wave.");
                                //r.add(CASEW1);
                            }
                            if (isSameHHoldCompositionInEachWave(CASEW1, cr)) {
                                count3++;
                                env.log("" + count3 + "\t records with same basic household composition in each wave.");
                                //r.add(CASEW1);
                            }
                        }
                        data.clearCollection(cID);
                    }
                    break;
                case 2:
                    while (ite.hasNext()) {
                        WaAS_CollectionID cID = ite.next();
                        WaAS_Collection c = data.getCollection(cID);
                        HashMap<WaAS_W1ID, WaAS_CombinedRecord> cData = c.getData();
                        Iterator<WaAS_W1ID> ite2 = cData.keySet().iterator();
                        while (ite2.hasNext()) {
                            WaAS_W1ID CASEW1 = ite2.next();
                            WaAS_CombinedRecord cr = cData.get(CASEW1);
                            if (isRecordInEachWave(CASEW1, cr)) {
                                count0++;
                                env.log("" + count0 + "\t records in each wave.");
                                //r.add(CASEW1);
                            }
                            if (isOnlyOneRecordInEachWave(CASEW1, cr)) {
                                count1++;
                                env.log("" + count1 + "\t records with only one record in each wave.");
                                r.add(CASEW1);
                            }
                            if (isSameNumberOfAdultsInEachWave(CASEW1, cr)) {
                                count2++;
                                env.log("" + count2 + "\t records with same number of adults in each wave.");
                                //r.add(CASEW1);
                            }
                            if (isSameHHoldCompositionInEachWave(CASEW1, cr)) {
                                count3++;
                                env.log("" + count3 + "\t records with same basic household composition in each wave.");
                                //r.add(CASEW1);
                            }
                        }
                        data.clearCollection(cID);
                    }
                    break;
                case 3:
                    while (ite.hasNext()) {
                        WaAS_CollectionID cID = ite.next();
                        WaAS_Collection c = data.getCollection(cID);
                        HashMap<WaAS_W1ID, WaAS_CombinedRecord> cData = c.getData();
                        Iterator<WaAS_W1ID> ite2 = cData.keySet().iterator();
                        while (ite2.hasNext()) {
                            WaAS_W1ID CASEW1 = ite2.next();
                            WaAS_CombinedRecord cr = cData.get(CASEW1);
                            if (isRecordInEachWave(CASEW1, cr)) {
                                count0++;
                                env.log("" + count0 + "\t records in each wave.");
                                //r.add(CASEW1);
                            }
                            if (isOnlyOneRecordInEachWave(CASEW1, cr)) {
                                count1++;
                                env.log("" + count1 + "\t records with only one record in each wave.");
                                //r.add(CASEW1);
                            }
                            if (isSameNumberOfAdultsInEachWave(CASEW1, cr)) {
                                count2++;
                                env.log("" + count2 + "\t records with same number of adults in each wave.");
                                r.add(CASEW1);
                            }
                            if (isSameHHoldCompositionInEachWave(CASEW1, cr)) {
                                count3++;
                                env.log("" + count3 + "\t records with same basic household composition in each wave.");
                                //r.add(CASEW1);
                            }
                        }
                        data.clearCollection(cID);
                    }
                    break;
                default:
                    while (ite.hasNext()) {
                        WaAS_CollectionID cID = ite.next();
                        WaAS_Collection c = data.getCollection(cID);
                        HashMap<WaAS_W1ID, WaAS_CombinedRecord> cData = c.getData();
                        Iterator<WaAS_W1ID> ite2 = cData.keySet().iterator();
                        while (ite2.hasNext()) {
                            WaAS_W1ID CASEW1 = ite2.next();
                            WaAS_CombinedRecord cr = cData.get(CASEW1);
                            if (isRecordInEachWave(CASEW1, cr)) {
                                count0++;
                                env.log("" + count0 + "\t records in each wave.");
                                //r.add(CASEW1);
                            }
                            if (isOnlyOneRecordInEachWave(CASEW1, cr)) {
                                count1++;
                                env.log("" + count1 + "\t records with only one record in each wave.");
                                //r.add(CASEW1);
                            }
                            if (isSameNumberOfAdultsInEachWave(CASEW1, cr)) {
                                count2++;
                                env.log("" + count2 + "\t records with same number of adults in each wave.");
                                //r.add(CASEW1);
                            }
                            if (isSameHHoldCompositionInEachWave(CASEW1, cr)) {
                                count3++;
                                env.log("" + count3 + "\t records with same basic household composition in each wave.");
                                r.add(CASEW1);
                            }
                        }
                        data.clearCollection(cID);
                    }
                    break;

            }
            env.log("" + count0 + "\t" + "Total hholds in all waves.");
            String m3 = " in all waves";
            String m4 = "Total hholds that are a single hhold" + m3;
            String m5 = " and that have the same ";
            env.log("" + count1 + "\t" + m4 + ".");
            env.log("" + count2 + "\t" + m4 + m5 + "number of adults" + m3 + ".");
            env.log("" + count3 + "\t" + m4 + m5 + "basic adult household "
                    + "composition" + m3 + ".");
            env.logEndTag(m1);
            env.logEndTag(m);
            Generic_IO.writeObject(r, f);
        }
        env.log("Total number of initial households in wave 1 " + r.size());
        env.logEndTag(m);
        return r;
    }

    /**
     * Checks if cr has records for each wave (isRecordInEachWave).
     *
     * @param CASEW1
     * @param cr
     * @return true iff cr has records for each wave.
     */
    protected boolean isRecordInEachWave(WaAS_W1ID CASEW1,
            WaAS_CombinedRecord cr) {
        boolean r = false;
        if (cr.w1Record == null) {
            env.log("No Wave 1 record(s) for CASEW1 " + CASEW1);
        }
        if (cr.w2Records.isEmpty()) {
            env.log("No Wave 2 record(s) for CASEW1 " + CASEW1);
        }
        Iterator<WaAS_W2ID> ite2 = cr.w2Records.keySet().iterator();
        while (ite2.hasNext()) {
            WaAS_W2ID CASEW2 = ite2.next();
            if (cr.w3Records.containsKey(CASEW2)) {
                HashMap<WaAS_W3ID, WaAS_W3Record> w3_2 = cr.w3Records.get(CASEW2);
                Iterator<WaAS_W3ID> ite3 = w3_2.keySet().iterator();
                while (ite3.hasNext()) {
                    WaAS_W3ID CASEW3 = ite3.next();
                    r = isRecordInEachWaveCheckW3(CASEW1, cr, CASEW2, CASEW3);
                    if (r == true) {
                        return r;
                    }
                }
            } else {
                env.log("No Wave 3 record(s) for CASEW2 " + CASEW2 + " in CASEW1 " + CASEW1);
            }
        }
        return r;
    }

    private boolean isRecordInEachWaveCheckW3(WaAS_W1ID CASEW1,
            WaAS_CombinedRecord cr, WaAS_W2ID CASEW2, WaAS_W3ID CASEW3) {
        boolean r = false;
        if (cr.w4Records.containsKey(CASEW2)) {
            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, WaAS_W4Record>> w4_2;
            w4_2 = cr.w4Records.get(CASEW2);
            if (w4_2.containsKey(CASEW3)) {
                HashMap<WaAS_W4ID, WaAS_W4Record> w4_3 = w4_2.get(CASEW3);
                Iterator<WaAS_W4ID> ite4 = w4_3.keySet().iterator();
                while (ite4.hasNext()) {
                    WaAS_W4ID CASEW4 = ite4.next();
                    r = isRecordInEachWaveCheckW4(CASEW1, cr, CASEW2, CASEW3, CASEW4);
                    if (r == true) {
                        return r;
                    }
                }
            } else {
                env.log("There are no Wave 4 records for CASEW3 " + CASEW3
                        + " in CASEW2 " + CASEW2 + " in " + "CASEW1 " + CASEW1);
            }
        } else {
            env.log("There are no Wave 4 records for CASEW2 " + CASEW2 + " in "
                    + "CASEW1 " + CASEW1);
        }
        return r;
    }

    private boolean isRecordInEachWaveCheckW4(WaAS_W1ID CASEW1,
            WaAS_CombinedRecord cr, WaAS_W2ID CASEW2, WaAS_W3ID CASEW3, 
            WaAS_W4ID CASEW4) {
        boolean r = false;
        if (cr.w5Records.containsKey(CASEW2)) {
            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>>> w5_2;
            w5_2 = cr.w5Records.get(CASEW2);
            if (w5_2.containsKey(CASEW3)) {
                HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>> w5_3;
                w5_3 = w5_2.get(CASEW3);
                if (w5_3.containsKey(CASEW4)) {
                    HashMap<WaAS_W5ID, WaAS_W5Record> w5_4;
                    w5_4 = w5_3.get(CASEW4);
                    if (!w5_4.keySet().isEmpty()) {
                        return true;
                    }
                } else {
                    env.log("No Wave 5 records for CASEW4 " + CASEW4 + " in "
                            + "CASEW3 " + CASEW3 + " in CASEW2 " + CASEW2
                            + " in CASEW1 " + CASEW1 + "!");
                }
            } else {
                env.log("No Wave 5 records for CASEW4 in CASEW3 " + CASEW3
                        + " in CASEW2 " + CASEW2 + " in CASEW1 " + CASEW1
                        + "!");
            }
        } else {
            env.log("No Wave 5 records for CASEW2 " + CASEW2 + " in CASEW1 "
                    + CASEW1 + "!");
        }
        return r;
    }

    /**
     * Checks if cr has only 1 record for each wave (isOnlyOneRecordInEachWave).
     *
     * @param CASEW1
     * @param cr
     * @return true iff cr has only 1 record for each wave.
     */
    public boolean isOnlyOneRecordInEachWave(WaAS_W1ID CASEW1,
            WaAS_CombinedRecord cr) {
        boolean r = false;
        if (cr.w2Records.size() > 1) {
            env.log("Multiple Wave 2 records for CASEW1 " + CASEW1 + "!");
        } else {
            WaAS_W2ID CASEW2 = cr.w2Records.keySet().iterator().next();
            r = isOnlyOneRecordInEachWaveCheckW2(CASEW1, cr, CASEW2);
        }
        return r;
    }

    private boolean isOnlyOneRecordInEachWaveCheckW2(WaAS_W1ID CASEW1,
            WaAS_CombinedRecord cr, WaAS_W2ID CASEW2) {
        boolean r = false;
        if (cr.w3Records.containsKey(CASEW2)) {
            HashMap<WaAS_W3ID, WaAS_W3Record> w3_2 = cr.w3Records.get(CASEW2);
            if (w3_2.size() > 1) {
                env.log("Multiple Wave 3 records for CASEW2 " + CASEW2 + " in "
                        + "CASEW1 " + CASEW1 + "!");
            } else {
                WaAS_W3ID CASEW3 = w3_2.keySet().iterator().next();
                r = isOnlyOneRecordInEachWaveCheckW3(CASEW1, cr, CASEW2,
                        CASEW3);
            }
        } else {
            env.log("No Wave 3 record for CASEW2 " + CASEW2 + " in CASEW1 "
                    + CASEW1 + "!");
        }
        return r;
    }

    private boolean isOnlyOneRecordInEachWaveCheckW3(WaAS_W1ID CASEW1,
            WaAS_CombinedRecord cr, WaAS_W2ID CASEW2, WaAS_W3ID CASEW3) {
        boolean r = false;
        if (cr.w4Records.containsKey(CASEW2)) {
            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, WaAS_W4Record>> w4_2;
            w4_2 = cr.w4Records.get(CASEW2);
            if (w4_2.containsKey(CASEW3)) {
                HashMap<WaAS_W4ID, WaAS_W4Record> w4_3 = w4_2.get(CASEW3);
                if (w4_3.size() > 1) {
                    env.log("Multiple Wave 4 records for CASEW3 " + CASEW3
                            + " in CASEW2 " + CASEW2 + " in CASEW1 " + CASEW1
                            + "!");
                } else {
                    WaAS_W4ID CASEW4 = w4_3.keySet().iterator().next();
                    r = isOnlyOneRecordInEachWaveCheckW4(CASEW1, cr, CASEW2,
                            CASEW3, CASEW4);
                }
            } else {
                env.log("No Wave 4 record for CASEW3 " + CASEW3 + " in CASEW2 "
                        + CASEW2 + " in CASEW1 " + CASEW1 + "!");
            }
        } else {
            env.log("No Wave 4 records for CASEW2 " + CASEW2 + " in CASEW1 "
                    + CASEW1 + "!");
        }
        return r;
    }

    private boolean isOnlyOneRecordInEachWaveCheckW4(WaAS_W1ID CASEW1,
            WaAS_CombinedRecord cr, WaAS_W2ID CASEW2, WaAS_W3ID CASEW3, 
            WaAS_W4ID CASEW4) {
        boolean r = false;
        if (cr.w5Records.containsKey(CASEW2)) {
            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>>> w5_2;
            w5_2 = cr.w5Records.get(CASEW2);
            if (w5_2.containsKey(CASEW3)) {
                HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>> w5_3;
                w5_3 = w5_2.get(CASEW3);
                if (w5_3.containsKey(CASEW4)) {
                    HashMap<WaAS_W5ID, WaAS_W5Record> w5_4 = w5_3.get(CASEW4);
                    if (w5_4.size() > 1) {
                        env.log("Multiple Wave 5 records for CASEW4 " + CASEW4
                                + " in CASEW3 " + CASEW3 + " in CASEW2 "
                                + CASEW2 + " in CASEW1 " + CASEW1 + "!");
                    } else {
                        r = true;
                    }
                } else {
                    env.log("No Wave 5 record for CASEW4 " + CASEW4 + " in "
                            + "CASEW3 " + CASEW3 + " in CASEW2 " + CASEW2
                            + " in CASEW1 " + CASEW1 + "!");
                }
            } else {
                env.log("No Wave 5 records for CASEW3 " + CASEW3 + " in CASEW2 "
                        + CASEW2 + " in CASEW1 " + CASEW1 + "!");
            }
        } else {
            env.log("No Wave 5 records for CASEW2 " + CASEW2 + " in CASEW1 "
                    + CASEW1 + "!");
        }
        return r;
    }

    /**
     * Checks if cr has the same number of adults in each wave for those hholds
     * that have only 1 record for each wave (isSameNumberOfAdultsInEachWave).
     *
     * @param CASEW1
     * @param cr
     * @return true iff cr has only 1 record for each wave.
     */
    public boolean isSameNumberOfAdultsInEachWave(WaAS_W1ID CASEW1,
            WaAS_CombinedRecord cr) {
        boolean r = false;
        if (cr.w2Records.size() > 1) {
            env.log("Multiple Wave 2 records for CASEW1 " + CASEW1 + "!");
        } else {
            Iterator<WaAS_W2ID> ite = cr.w2Records.keySet().iterator();
            while (ite.hasNext()) {
                WaAS_W2ID CASEW2 = ite.next();
                WaAS_W2Record w2rec = cr.w2Records.get(CASEW2);
                r = isSameNumberOfAdultsInEachWaveCheckW2(CASEW1, cr, CASEW2,
                        w2rec);
            }
        }
        return r;
    }

    private boolean isSameNumberOfAdultsInEachWaveCheckW2(WaAS_W1ID CASEW1,
            WaAS_CombinedRecord cr, WaAS_W2ID CASEW2, WaAS_W2Record w2rec) {
        boolean r = false;
        if (cr.w3Records.containsKey(CASEW2)) {
            HashMap<WaAS_W3ID, WaAS_W3Record> w3_2;
            w3_2 = cr.w3Records.get(CASEW2);
            if (w3_2.size() > 1) {
                env.log("Multiple Wave 3 records for CASEW2 " + CASEW2 + " in "
                        + "CASEW1 " + CASEW1 + "!");
            } else {
                Iterator<WaAS_W3ID> ite = w3_2.keySet().iterator();
                while (ite.hasNext()) {
                    WaAS_W3ID CASEW3 = ite.next();
                    WaAS_W3Record w3rec = w3_2.get(CASEW3);
                    r = isSameNumberOfAdultsInEachWaveCheckW3(CASEW1, cr,
                            CASEW2, w2rec, CASEW3, w3rec);
                }
            }
        } else {
            env.log("No Wave 3 record for CASEW2 " + CASEW2 + " in CASEW1 "
                    + CASEW1 + "!");
        }
        return r;
    }

    private boolean isSameNumberOfAdultsInEachWaveCheckW3(WaAS_W1ID CASEW1,
            WaAS_CombinedRecord cr, WaAS_W2ID CASEW2, WaAS_W2Record w2rec,
            WaAS_W3ID CASEW3, WaAS_W3Record w3rec) {
        boolean r = false;
        if (cr.w4Records.containsKey(CASEW2)) {
            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, WaAS_W4Record>> w4_2;
            w4_2 = cr.w4Records.get(CASEW2);
            if (w4_2.containsKey(CASEW3)) {
                HashMap<WaAS_W4ID, WaAS_W4Record> w4_3 = w4_2.get(CASEW3);
                if (w4_3.size() > 1) {
                    env.log("Multiple Wave 4 records for CASEW3 " + CASEW3
                            + " in CASEW2 " + CASEW2 + " in CASEW1 " + CASEW1
                            + "!");
                } else {
                    Iterator<WaAS_W4ID> ite = w4_3.keySet().iterator();
                    while (ite.hasNext()) {
                        WaAS_W4ID CASEW4 = ite.next();
                        WaAS_W4Record w4rec = w4_3.get(CASEW4);
                        r = isSameNumberOfAdultsInEachWaveCheckW4(CASEW1, cr,
                                CASEW2, w2rec, CASEW3, w3rec, CASEW4, w4rec);
                    }
                }
            } else {
                env.log("No Wave 4 records for CASEW3 " + CASEW3 + " in CASEW2 "
                        + CASEW2 + " in CASEW1 " + CASEW1 + "!");
            }
        } else {
            env.log("No Wave 4 records for CASEW2 " + CASEW2 + " in CASEW1 "
                    + CASEW1 + "!");
        }
        return r;
    }

    private boolean isSameNumberOfAdultsInEachWaveCheckW4(WaAS_W1ID CASEW1,
            WaAS_CombinedRecord cr, WaAS_W2ID CASEW2, WaAS_W2Record w2rec,
            WaAS_W3ID CASEW3, WaAS_W3Record w3rec, WaAS_W4ID CASEW4,
            WaAS_W4Record w4rec) {
        boolean r = false;
        if (cr.w5Records.containsKey(CASEW2)) {
            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>>> w5_2;
            w5_2 = cr.w5Records.get(CASEW2);
            if (w5_2.containsKey(CASEW3)) {
                HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>> w5_3;
                w5_3 = w5_2.get(CASEW3);
                if (w5_3.containsKey(CASEW4)) {
                    HashMap<WaAS_W5ID, WaAS_W5Record> w5_4;
                    w5_4 = w5_3.get(CASEW4);
                    if (w5_4.size() > 1) {
                        env.log("Multiple Wave 5 records for CASEW4 " + CASEW4
                                + " in CASEW3 " + CASEW3 + " in CASEW2 "
                                + CASEW2 + " in CASEW1 " + CASEW1 + "!");
                    } else {
                        Iterator<WaAS_W5ID> ite = w5_4.keySet().iterator();
                        while (ite.hasNext()) {
                            WaAS_W5ID CASEW5 = ite.next();
                            WaAS_W5Record w5rec = w5_4.get(CASEW5);
                            byte w1 = cr.w1Record.getHhold().getNUMADULT();
                            byte w2 = w2rec.getHhold().getNUMADULT();
                            byte w3 = w3rec.getHhold().getNUMADULT();
                            byte w4 = w4rec.getHhold().getNUMADULT();
                            byte w5 = w5rec.getHhold().getNUMADULT();
                            if (w1 == w2 && w2 == w3 && w3 == w4 && w4 == w5) {
                                r = true;
                            }
                        }
                    }
                } else {
                    env.log("No Wave 5 records for CASEW4 " + CASEW4 + " in "
                            + "CASEW3 " + CASEW3 + " in CASEW2 " + CASEW2
                            + " in CASEW1 " + CASEW1 + "!");
                }
            } else {
                env.log("No Wave 5 records for CASEW3 " + CASEW3 + " in CASEW2 "
                        + CASEW2 + " in CASEW1 " + CASEW1 + "!");
            }
        } else {
            env.log("No Wave 5 records for CASEW2 " + CASEW2 + " in CASEW1 "
                    + CASEW1 + "!");
        }
        return r;
    }

    /**
     * Checks if {@code cr} has the same basic adult household composition in
     * each wave for those hholds that have only 1 record for each wave
     * (isSameHHoldCompositionInEachWave). Iff this is the case then true is
     * returned. The number of adults in a household is allowed to decrease. If
     * the number of adults increases, then a further check is done: If the
     * number of householders is the same and the number of children has
     * decreased (it might be assumed that children have become non-dependents).
     * But, if that is not the case, then if the number of dependents increases
     * for any wave then false is returned.
     *
     * @param CASEW1
     * @param cr
     * @return true iff cr has only 1 record for each wave.
     */
    public boolean isSameHHoldCompositionInEachWave(WaAS_W1ID CASEW1,
            WaAS_CombinedRecord cr) {
        boolean r = false;
        if (cr.w2Records.size() > 1) {
            env.log("Multiple Wave 2 records for CASEW1 " + CASEW1 + "!");
        }
        Iterator<WaAS_W2ID> ite = cr.w2Records.keySet().iterator();
        while (ite.hasNext()) {
            WaAS_W2ID CASEW2 = ite.next();
            WaAS_W2Record w2rec = cr.w2Records.get(CASEW2);
            r = isSameHHoldCompositionInEachWaveCheckW2(CASEW1, cr, CASEW2,
                    w2rec);
        }
        return r;
    }

    private boolean isSameHHoldCompositionInEachWaveCheckW2(WaAS_W1ID CASEW1,
            WaAS_CombinedRecord cr, WaAS_W2ID CASEW2, WaAS_W2Record w2rec) {
        boolean r = false;
        if (cr.w3Records.containsKey(CASEW2)) {
            HashMap<WaAS_W3ID, WaAS_W3Record> w3_2 = cr.w3Records.get(CASEW2);
            if (w3_2.size() > 1) {
                env.log("Multiple Wave 3 records for CASEW2 " + CASEW2 + " in "
                        + "CASEW1 " + CASEW1 + "!");
            } else {
                Iterator<WaAS_W3ID> ite = w3_2.keySet().iterator();
                while (ite.hasNext()) {
                    WaAS_W3ID CASEW3 = ite.next();
                    WaAS_W3Record w3rec = w3_2.get(CASEW3);
                    r = isSameHHoldCompositionInEachWaveCheckW3(CASEW1, cr,
                            CASEW2, w2rec, CASEW3, w3rec);
                }
            }
        } else {
            env.log("No Wave 3 records for CASEW2 " + CASEW2 + " in CASEW1 "
                    + CASEW1 + "!");
        }
        return r;
    }

    private boolean isSameHHoldCompositionInEachWaveCheckW3(WaAS_W1ID CASEW1,
            WaAS_CombinedRecord cr, WaAS_W2ID CASEW2, WaAS_W2Record w2rec,
            WaAS_W3ID CASEW3, WaAS_W3Record w3rec) {
        boolean r = false;
        if (cr.w4Records.containsKey(CASEW2)) {
            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, WaAS_W4Record>> w4_2;
            w4_2 = cr.w4Records.get(CASEW2);
            if (w4_2.containsKey(CASEW3)) {
                if (w4_2.size() > 1) {
                    env.log("Multiple Wave 4 records for CASEW3 " + CASEW3
                            + " in CASEW2 " + CASEW2 + " in CASEW1 " + CASEW1
                            + "!");
                } else {
                    HashMap<WaAS_W4ID, WaAS_W4Record> w4_3 = w4_2.get(CASEW3);
                    Iterator<WaAS_W4ID> ite = w4_3.keySet().iterator();
                    while (ite.hasNext()) {
                        WaAS_W4ID CASEW4 = ite.next();
                        WaAS_W4Record w4rec = w4_3.get(CASEW4);
                        r = isSameHHoldCompositionInEachWaveCheckW4(CASEW1, cr,
                                CASEW2, w2rec, CASEW3, w3rec, CASEW4, w4rec);
                    }
                }
            } else {
                env.log("No Wave 4 records for CASEW3 " + CASEW3 + " in CASEW2 "
                        + CASEW2 + " in CASEW1 " + CASEW1 + "!");
            }
        } else {
            env.log("No Wave 4 records for CASEW2 " + CASEW2 + " in CASEW1 "
                    + CASEW1 + "!");
        }
        return r;
    }

    private boolean isSameHHoldCompositionInEachWaveCheckW4(WaAS_W1ID CASEW1,
            WaAS_CombinedRecord cr, WaAS_W2ID CASEW2, WaAS_W2Record w2rec,
            WaAS_W3ID CASEW3, WaAS_W3Record w3rec, WaAS_W4ID CASEW4,
            WaAS_W4Record w4rec) {
        boolean r = false;
        if (cr.w5Records.containsKey(CASEW2)) {
            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>>> w5_2;
            w5_2 = cr.w5Records.get(CASEW2);
            if (w5_2.containsKey(CASEW3)) {
                if (w5_2.size() > 1) {
                    env.log("Multiple Wave 5 records for CASEW3 " + CASEW3
                            + " in CASEW2 " + CASEW2 + " in CASEW1 " + CASEW1
                            + "!");
                } else {
                    HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>> w5_3;
                    w5_3 = w5_2.get(CASEW3);
                    if (w5_3.containsKey(CASEW4)) {
                        HashMap<WaAS_W5ID, WaAS_W5Record> w5_4;
                        w5_4 = w5_3.get(CASEW4);
                        if (w5_4.size() > 1) {
                            env.log("Multiple Wave 5 records for CASEW4 "
                                    + CASEW4 + " in CASEW3 " + CASEW3 + " in "
                                    + "CASEW2 " + CASEW2 + " in CASEW1 "
                                    + CASEW1 + "!");
                        } else {
                            Iterator<WaAS_W5ID> ite5 = w5_4.keySet().iterator();
                            while (ite5.hasNext()) {
                                WaAS_W5ID CASEW5 = ite5.next();
                                WaAS_W5Record w5rec = w5_4.get(CASEW5);
                                r = isSameBasicHHoldComposition(cr, w2rec,
                                        w3rec, w4rec, w5rec);
                            }
                        }
                    } else {
                        env.log("No Wave 5 records for CASEW4 " + CASEW4 + " in "
                                + "CASEW3 " + CASEW3 + " in CASEW2 " + CASEW2
                                + " in CASEW1 " + CASEW1 + "!");
                    }
                }
            } else {
                env.log("No Wave 5 records for CASEW3 " + CASEW3 + " in CASEW2 "
                        + CASEW2 + " in CASEW1 " + CASEW1 + "!");
            }
        } else {
            env.log("No Wave 5 records for CASEW2 " + CASEW2 + " in CASEW1 "
                    + CASEW1 + "!");
        }
        return r;
    }

    /**
     * @param cr
     * @param w2rec
     * @param w3rec
     * @param w4rec
     * @param w5rec
     * @return true iff the household has the same basic composition in all
     * waves.
     */
    public boolean isSameBasicHHoldComposition(WaAS_CombinedRecord cr,
            WaAS_W2Record w2rec, WaAS_W3Record w3rec,
            WaAS_W4Record w4rec, WaAS_W5Record w5rec) {
        boolean r = true;
        // Wave 1
        WaAS_W1HRecord w1hhold = cr.w1Record.getHhold();
        ArrayList<WaAS_W1PRecord> w1people = cr.w1Record.getPeople();
        // Wave 2
        WaAS_W2HRecord w2hhold = w2rec.getHhold();
        ArrayList<WaAS_W2PRecord> w2people = w2rec.getPeople();
        // Wave 3
        WaAS_W3HRecord w3hhold = w3rec.getHhold();
        ArrayList<WaAS_W3PRecord> w3people = w3rec.getPeople();
        // Wave 4
        WaAS_W4HRecord w4hhold = w4rec.getHhold();
        ArrayList<WaAS_W4PRecord> w4people = w4rec.getPeople();
        // Wave 5
        WaAS_W5HRecord w5hhold = w5rec.getHhold();
        ArrayList<WaAS_W5PRecord> w5people = w5rec.getPeople();
        // W1
        byte w1NUMADULT = w1hhold.getNUMADULT();
        byte w1NUMCHILD = w1hhold.getNUMCHILD();
        //byte w1NUMDEPCH = w1hhold.getNUMDEPCH();
        byte w1NUMHHLDR = w1hhold.getNUMHHLDR();
        // W2
        byte w2NUMADULT = w2hhold.getNUMADULT();
        byte w2NUMCHILD = w2hhold.getNUMCHILD();
        //byte w2NUMDEPCH = w2hhold.getNUMDEPCH_HH();
        //boolean w2NUMNDEP = w2hhold.getNUMNDEP();
        byte w2NUMHHLDR = w2hhold.getNUMHHLDR();
        // W3
        byte w3NUMADULT = w3hhold.getNUMADULT();
        byte w3NUMCHILD = w3hhold.getNUMCHILD();
        //byte w3NUMDEPCH = w3hhold.getNUMDEPCH();
        byte w3NUMHHLDR = w3hhold.getNUMHHLDR();
        // W4
        byte w4NUMADULT = w4hhold.getNUMADULT();
        byte w4NUMCHILD = w4hhold.getNUMCHILD();
        //byte w4NUMDEPCH = w4hhold.getNUMDEPCH();
        byte w4NUMHHLDR = w4hhold.getNUMHHLDR();
        // W5
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
        WaAS_W1W2W3W4W5PRecord p2;
        Iterator<T> ite;
        ite = people.iterator();
        while (ite.hasNext()) {
            p2 = (WaAS_W1W2W3W4W5PRecord) ite.next();
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
