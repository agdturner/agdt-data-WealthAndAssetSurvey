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

import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_CollectionID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W4ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W1ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W5ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W2ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W3ID;
import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import uk.ac.leeds.ccg.andyt.data.interval.Data_IntervalLong1;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Object;
import uk.ac.leeds.ccg.andyt.generic.io.Generic_IO;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Strings;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Collection;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_CombinedRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_GORSubsetsAndLookups;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_W1Data;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W1Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_W2Data;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W2Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_W3Data;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W3Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_W4Data;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W4Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_W5Data;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W5Record;
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

    // For convenience.
    protected final byte W1;
    protected final byte W2;
    protected final byte W3;
    protected final byte W4;
    protected final byte W5;
    protected final byte NWAVES;

    public WaAS_Handler(WaAS_Environment e) {
        super(e);
        W1 = WaAS_Environment.W1;
        W2 = WaAS_Environment.W2;
        W3 = WaAS_Environment.W3;
        W4 = WaAS_Environment.W4;
        W5 = WaAS_Environment.W5;
        NWAVES = WaAS_Environment.NWAVES;
    }

    public abstract String getType();
    
    /**
     *
     * @param wave
     * @return
     */
    protected File getFile(byte wave) {
        return new File(env.files.getGeneratedWaASDir(),
                getType() + WaAS_Strings.s_W + wave + WaAS_Files.DOT_DAT);
    }

    /**
     * @param wave the wave for which the source input File is returned.
     * @return the source input File for a particular WaAS Wave.
     */
    public File getInputFile(byte wave) {
        String filename;
        filename = "was_wave_" + wave + "_" + getType() + "_eul_final";
        if (wave < 4) {
            filename += "_v2";
        }
        filename += ".tab";
        return new File(env.files.getInputWaASDir(), filename);
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
        return new File(env.files.getGeneratedWaASSubsetsDir(),
                getType() + WaAS_Strings.s_w + wave + type + WaAS_Files.DOT_DAT);
    }

    /**
     *
     * @param wave The wave to be cached.
     * @param type The name of the type of subset to be cached.
     * @return
     */
    public File getSubsetCacheFile2(byte wave, String type) {
        return new File(env.files.getGeneratedWaASSubsetsDir(),
                getType() + WaAS_Strings.s_w + wave + type + "_2" + WaAS_Files.DOT_DAT);
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
     * @param <K>
     * @param <V>
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
        return new File(env.files.getGeneratedWaASSubsetsDir(),
                getString0(wave) + getStringToWaveDotDat(wave + 1));
    }

    /**
     * @param wave
     * @return the File for a subset lookup from wave.
     */
    public File getSubsetLookupFromFile(byte wave) {
        return new File(env.files.getGeneratedWaASSubsetsDir(),
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
                getSubsetLookupFromFile(W4));
    }

    protected String getString0(int wave) {
        return getType() + WaAS_Strings.s_w + wave;
    }

    protected String getString1(byte wave, short cID) {
        return getString0(wave) + WaAS_Strings.symbol_underscore + cID;
    }

    protected String getStringToWaveDotDat(int wave) {
        return WaAS_Strings.s__To_ + WaAS_Strings.s_w + wave + WaAS_Files.DOT_DAT;
    }

    public void cacheSubsetCollection(short cID, byte wave, Object o) {
        cache(wave, getSubsetCollectionFile(cID, wave), o);
    }

    public Object loadSubsetCollection(short cID, byte wave) {
        return load(wave, getSubsetCollectionFile(cID, wave));
    }

    public File getSubsetCollectionFile(short cID, byte wave) {
        return new File(env.files.getGeneratedWaASSubsetsDir(),
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
     * @param gors
     * @param subset
     * @return
     */
    public WaAS_GORSubsetsAndLookups getGORSubsetsAndLookups(String name,
            ArrayList<Byte> gors, HashSet<WaAS_W1ID> subset) {
        WaAS_GORSubsetsAndLookups r;
        File f = new File(env.files.getOutputDataDir(), name + "GORSubsetsAndLookups.dat");
        if (f.exists()) {
            r = (WaAS_GORSubsetsAndLookups) Generic_IO.readObject(f);
        } else {
            r = new WaAS_GORSubsetsAndLookups(gors);
            // Wave 1
            env.data.collections.keySet().stream().forEach(cID -> {
                WaAS_Collection c = env.data.getCollection(cID);
                c.getData().keySet().stream().forEach(w1ID -> {
                    if (subset.contains(w1ID)) {
                        WaAS_CombinedRecord cr = c.getData().get(w1ID);
                        byte GOR = cr.w1Rec.getHhold().getGOR();
                        Generic_Collections.addToMap(r.gor_To_w1, GOR, w1ID);
                        r.w1_To_gor.put(w1ID, GOR);
                    }
                });
                env.data.clearCollection(cID);
            });
            // Wave 2
            env.data.collections.keySet().stream().forEach(cID -> {
                WaAS_Collection c = env.data.getCollection(cID);
                c.getData().keySet().stream().forEach(w1ID -> {
                    if (subset.contains(w1ID)) {
                        WaAS_CombinedRecord cr = c.getData().get(w1ID);
                        Iterator<WaAS_W2ID> ite2 = cr.w2Recs.keySet().iterator();
                        while (ite2.hasNext()) {
                            WaAS_W2ID w2ID = ite2.next();
                            WaAS_W2Record w2 = cr.w2Recs.get(w2ID);
                            byte GOR = w2.getHhold().getGOR();
                            Generic_Collections.addToMap(r.gor_To_w2, GOR, w2ID);
                            r.w2_To_gor.put(w2ID, GOR);
                        }
                    }
                });
                env.data.clearCollection(cID);
            });
            // Wave 3
            env.data.collections.keySet().stream().forEach(cID -> {
                WaAS_Collection c = env.data.getCollection(cID);
                c.getData().keySet().stream().forEach(w1ID -> {
                    if (subset.contains(w1ID)) {
                        WaAS_CombinedRecord cr = c.getData().get(w1ID);
                        Iterator<WaAS_W2ID> ite2 = cr.w3Recs.keySet().iterator();
                        while (ite2.hasNext()) {
                            WaAS_W2ID w2ID = ite2.next();
                            HashMap<WaAS_W3ID, WaAS_W3Record> w3_2 = cr.w3Recs.get(w2ID);
                            Iterator<WaAS_W3ID> ite3 = w3_2.keySet().iterator();
                            while (ite3.hasNext()) {
                                WaAS_W3ID w3ID = ite3.next();
                                WaAS_W3Record w3 = w3_2.get(w3ID);
                                byte GOR = w3.getHhold().getGOR();
                                Generic_Collections.addToMap(r.gor_To_w3, GOR, w3ID);
                                r.w3_To_gor.put(w3ID, GOR);
                            }
                        }
                    }
                });
                env.data.clearCollection(cID);
            });
            // Wave 4
            env.data.collections.keySet().stream().forEach(cID -> {
                WaAS_Collection c = env.data.getCollection(cID);
                c.getData().keySet().stream().forEach(w1ID -> {
                    if (subset.contains(w1ID)) {
                        WaAS_CombinedRecord cr = c.getData().get(w1ID);
                        Iterator<WaAS_W2ID> ite2 = cr.w4Recs.keySet().iterator();
                        while (ite2.hasNext()) {
                            WaAS_W2ID w2ID = ite2.next();
                            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, WaAS_W4Record>> w4_2;
                            w4_2 = cr.w4Recs.get(w2ID);
                            Iterator<WaAS_W3ID> ite3 = w4_2.keySet().iterator();
                            while (ite3.hasNext()) {
                                WaAS_W3ID w3ID = ite3.next();
                                HashMap<WaAS_W4ID, WaAS_W4Record> w4_3 = w4_2.get(w3ID);
                                Iterator<WaAS_W4ID> ite4 = w4_3.keySet().iterator();
                                while (ite4.hasNext()) {
                                    WaAS_W4ID w4ID = ite4.next();
                                    WaAS_W4Record w4 = w4_3.get(w4ID);
                                    byte GOR = w4.getHhold().getGOR();
                                    Generic_Collections.addToMap(r.gor_To_w4, GOR, w4ID);
                                    r.w4_To_gor.put(w4ID, GOR);
                                }
                            }
                        }
                    }
                });
                env.data.clearCollection(cID);
            });
            // Wave 5
            env.data.collections.keySet().stream().forEach(cID -> {
                WaAS_Collection c;
                c = env.data.getCollection(cID);
                c.getData().keySet().stream().forEach(w1ID -> {
                    if (subset.contains(w1ID)) {
                        WaAS_CombinedRecord cr = c.getData().get(w1ID);
                        Iterator<WaAS_W2ID> ite2 = cr.w5Recs.keySet().iterator();
                        while (ite2.hasNext()) {
                            WaAS_W2ID w2ID = ite2.next();
                            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>>> w5_2;
                            w5_2 = cr.w5Recs.get(w2ID);
                            Iterator<WaAS_W3ID> ite3 = w5_2.keySet().iterator();
                            while (ite3.hasNext()) {
                                WaAS_W3ID w3ID = ite3.next();
                                HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>> w5_3 = w5_2.get(w3ID);
                                Iterator<WaAS_W4ID> ite4 = w5_3.keySet().iterator();
                                while (ite4.hasNext()) {
                                    WaAS_W4ID w4ID = ite4.next();
                                    HashMap<WaAS_W5ID, WaAS_W5Record> w5_4 = w5_3.get(w4ID);
                                    Iterator<WaAS_W5ID> ite5 = w5_4.keySet().iterator();
                                    while (ite5.hasNext()) {
                                        WaAS_W5ID w5ID = ite5.next();
                                        WaAS_W5Record w5 = w5_4.get(w5ID);
                                        byte GOR = w5.getHhold().getGOR();
                                        Generic_Collections.addToMap(r.gor_to_w5, GOR, w5ID);
                                        r.w5_To_gor.put(w5ID, GOR);
                                    }
                                }
                            }
                        }
                    }
                });
                env.data.clearCollection(cID);
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
        if (w == W1 || w == W2) {
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
        } else if (w == W3 || w == W4 || w == W5) {
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
    public HashSet<WaAS_W1ID> getSubset(int type) {
        String m = "getSubset";
        env.logStartTag(m);
        HashSet<WaAS_W1ID> r;
        String fn = "Subset" + type + "HashSet_CASEW1.dat";
        File f = new File(env.files.getOutputDataDir(), fn);
        if (f.exists()) {
            r = (HashSet<WaAS_W1ID>) Generic_IO.readObject(f);
        } else {
            r = new HashSet<>();
            env.log("Number of combined records " + env.data.w1_To_c.size());
            env.log("Number of collections of combined records " + env.data.collections.size());
            int count0 = 0;
            int count1 = 0;
            int count2 = 0;
            int count3 = 0;
            // Check For Household Records
            String m1 = "Check For Household Records";
            env.logStartTag(m1);
            Iterator<WaAS_CollectionID> ite = env.data.collections.keySet().iterator();
            switch (type) {
                case 1:
                    while (ite.hasNext()) {
                        WaAS_CollectionID cID = ite.next();
                        WaAS_Collection c = env.data.getCollection(cID);
                        HashMap<WaAS_W1ID, WaAS_CombinedRecord> cData = c.getData();
                        Iterator<WaAS_W1ID> ite2 = cData.keySet().iterator();
                        while (ite2.hasNext()) {
                            WaAS_W1ID w1ID = ite2.next();
                            WaAS_CombinedRecord cr = cData.get(w1ID);
                            if (isRecordInEachWave(w1ID, cr)) {
                                count0++;
                                //env.log("" + count0 + "\t records in each wave.");
                                r.add(w1ID);
                            }
                            if (isOnlyOneRecordInEachWave(w1ID, cr)) {
                                count1++;
                                //env.log("" + count1 + "\t records with only one record in each wave.");
                                //r.add(w1ID);
                            }
                            if (isSameNumberOfAdultsInEachWave(w1ID, cr)) {
                                count2++;
                                //env.log("" + count2 + "\t records with same number of adults in each wave.");
                                //r.add(w1ID);
                            }
                            if (isSameHHoldCompositionInEachWave(w1ID, cr)) {
                                count3++;
                                //env.log("" + count3 + "\t records with same basic household composition in each wave.");
                                //r.add(w1ID);
                            }
                        }
                        env.data.clearCollection(cID);
                    }
                    break;
                case 2:
                    while (ite.hasNext()) {
                        WaAS_CollectionID cID = ite.next();
                        WaAS_Collection c = env.data.getCollection(cID);
                        HashMap<WaAS_W1ID, WaAS_CombinedRecord> cData = c.getData();
                        Iterator<WaAS_W1ID> ite2 = cData.keySet().iterator();
                        while (ite2.hasNext()) {
                            WaAS_W1ID w1ID = ite2.next();
                            WaAS_CombinedRecord cr = cData.get(w1ID);
                            if (isRecordInEachWave(w1ID, cr)) {
                                count0++;
                                //env.log("" + count0 + "\t records in each wave.");
                                //r.add(w1ID);
                            }
                            if (isOnlyOneRecordInEachWave(w1ID, cr)) {
                                count1++;
                                //env.log("" + count1 + "\t records with only one record in each wave.");
                                r.add(w1ID);
                            }
                            if (isSameNumberOfAdultsInEachWave(w1ID, cr)) {
                                count2++;
                                //env.log("" + count2 + "\t records with same number of adults in each wave.");
                                //r.add(w1ID);
                            }
                            if (isSameHHoldCompositionInEachWave(w1ID, cr)) {
                                count3++;
                                //env.log("" + count3 + "\t records with same basic household composition in each wave.");
                                //r.add(w1ID);
                            }
                        }
                        env.data.clearCollection(cID);
                    }
                    break;
                case 3:
                    while (ite.hasNext()) {
                        WaAS_CollectionID cID = ite.next();
                        WaAS_Collection c = env.data.getCollection(cID);
                        HashMap<WaAS_W1ID, WaAS_CombinedRecord> cData = c.getData();
                        Iterator<WaAS_W1ID> ite2 = cData.keySet().iterator();
                        while (ite2.hasNext()) {
                            WaAS_W1ID w1ID = ite2.next();
                            WaAS_CombinedRecord cr = cData.get(w1ID);
                            if (isRecordInEachWave(w1ID, cr)) {
                                count0++;
                                //env.log("" + count0 + "\t records in each wave.");
                                //r.add(w1ID);
                            }
                            if (isOnlyOneRecordInEachWave(w1ID, cr)) {
                                count1++;
                                //env.log("" + count1 + "\t records with only one record in each wave.");
                                //r.add(w1ID);
                            }
                            if (isSameNumberOfAdultsInEachWave(w1ID, cr)) {
                                count2++;
                                //env.log("" + count2 + "\t records with same number of adults in each wave.");
                                r.add(w1ID);
                            }
                            if (isSameHHoldCompositionInEachWave(w1ID, cr)) {
                                count3++;
                                //env.log("" + count3 + "\t records with same basic household composition in each wave.");
                                //r.add(w1ID);
                            }
                        }
                        env.data.clearCollection(cID);
                    }
                    break;
                default:
                    while (ite.hasNext()) {
                        WaAS_CollectionID cID = ite.next();
                        WaAS_Collection c = env.data.getCollection(cID);
                        HashMap<WaAS_W1ID, WaAS_CombinedRecord> cData = c.getData();
                        Iterator<WaAS_W1ID> ite2 = cData.keySet().iterator();
                        while (ite2.hasNext()) {
                            WaAS_W1ID w1ID = ite2.next();
                            WaAS_CombinedRecord cr = cData.get(w1ID);
                            if (isRecordInEachWave(w1ID, cr)) {
                                count0++;
                                //env.log("" + count0 + "\t records in each wave.");
                                //r.add(w1ID);
                            }
                            if (isOnlyOneRecordInEachWave(w1ID, cr)) {
                                count1++;
                                //env.log("" + count1 + "\t records with only one record in each wave.");
                                //r.add(w1ID);
                            }
                            if (isSameNumberOfAdultsInEachWave(w1ID, cr)) {
                                count2++;
                                //env.log("" + count2 + "\t records with same number of adults in each wave.");
                                //r.add(w1ID);
                            }
                            if (isSameHHoldCompositionInEachWave(w1ID, cr)) {
                                count3++;
                                //env.log("" + count3 + "\t records with same basic household composition in each wave.");
                                r.add(w1ID);
                            }
                        }
                        env.data.clearCollection(cID);
                    }
                    break;
            }
            env.log("" + count0 + "\t" + "Total hholds in all waves.");
            String m3 = " in all waves";
            String m4 = "Total hholds that are a single hhold" + m3;
            String m5 = " and that have the same ";
            env.log("" + count1 + "\t" + m4 + ".");
            env.log("" + count2 + "\t" + m4 + m5 + "number of adults" + m3 + ".");
            env.log("" + count3 + "\t" + m4 + m5 + "basic adult household composition" + m3 + ".");
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
     * @param w1ID
     * @param cr
     * @return true iff cr has records for each wave.
     */
    protected boolean isRecordInEachWave(WaAS_W1ID w1ID,
            WaAS_CombinedRecord cr) {
        boolean r = false;
        if (cr.w1Rec == null) {
            env.log("No Wave 1 record(s) for " + w1ID + "!");
        }
        if (cr.w2Recs.isEmpty()) {
            env.log("No Wave 2 record(s) for " + w1ID + "!");
        }
        Iterator<WaAS_W2ID> ite2 = cr.w2Recs.keySet().iterator();
        while (ite2.hasNext()) {
            WaAS_W2ID w2ID = ite2.next();
            if (cr.w3Recs.containsKey(w2ID)) {
                HashMap<WaAS_W3ID, WaAS_W3Record> w3_2 = cr.w3Recs.get(w2ID);
                Iterator<WaAS_W3ID> ite3 = w3_2.keySet().iterator();
                while (ite3.hasNext()) {
                    WaAS_W3ID w3ID = ite3.next();
                    r = isRecordInEachWaveCheckW3(w1ID, cr, w2ID, w3ID);
                    if (r == true) {
                        return r;
                    }
                }
            } else {
                env.log("No Wave 3 record(s) for " + w2ID + " in " + w1ID + "!");
            }
        }
        return r;
    }

    private boolean isRecordInEachWaveCheckW3(WaAS_W1ID w1ID,
            WaAS_CombinedRecord cr, WaAS_W2ID w2ID, WaAS_W3ID w3ID) {
        boolean r = false;
        if (cr.w4Recs.containsKey(w2ID)) {
            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, WaAS_W4Record>> w4_2;
            w4_2 = cr.w4Recs.get(w2ID);
            if (w4_2.containsKey(w3ID)) {
                HashMap<WaAS_W4ID, WaAS_W4Record> w4_3 = w4_2.get(w3ID);
                Iterator<WaAS_W4ID> ite4 = w4_3.keySet().iterator();
                while (ite4.hasNext()) {
                    WaAS_W4ID CASEW4 = ite4.next();
                    r = isRecordInEachWaveCheckW4(w1ID, cr, w2ID, w3ID, CASEW4);
                    if (r == true) {
                        return r;
                    }
                }
            } else {
                env.log("No Wave 4 records for " + w3ID + " in " + w2ID + " in "
                        + w1ID + "!");
            }
        } else {
            env.log("No Wave 4 records for " + w2ID + " in " + w1ID + "!");
        }
        return r;
    }

    private boolean isRecordInEachWaveCheckW4(WaAS_W1ID w1ID,
            WaAS_CombinedRecord cr, WaAS_W2ID w2ID, WaAS_W3ID w3ID,
            WaAS_W4ID w4ID) {
        boolean r = false;
        if (cr.w5Recs.containsKey(w2ID)) {
            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>>> w5_2;
            w5_2 = cr.w5Recs.get(w2ID);
            if (w5_2.containsKey(w3ID)) {
                HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>> w5_3;
                w5_3 = w5_2.get(w3ID);
                if (w5_3.containsKey(w4ID)) {
                    HashMap<WaAS_W5ID, WaAS_W5Record> w5_4 = w5_3.get(w4ID);
                    if (!w5_4.keySet().isEmpty()) {
                        return true;
                    }
                } else {
                    env.log("No Wave 5 records for " + w4ID + " in " + w3ID
                            + " in " + w2ID + " in " + w1ID + "!");
                }
            } else {
                env.log("No Wave 5 records for " + w3ID + " in " + w2ID
                        + " in " + w1ID + "!");
            }
        } else {
            env.log("No Wave 5 records for " + w2ID + " in " + w1ID + "!");
        }
        return r;
    }

    /**
     * Checks if cr has only 1 record for each wave (isOnlyOneRecordInEachWave).
     *
     * @param w1ID
     * @param cr
     * @return true iff cr has only 1 record for each wave.
     */
    public boolean isOnlyOneRecordInEachWave(WaAS_W1ID w1ID, WaAS_CombinedRecord cr) {
        boolean r = false;
        if (cr.w2Recs.size() > 1) {
            env.log("Multiple Wave 2 records for " + w1ID + "!");
        } else {
            WaAS_W2ID w2ID = cr.w2Recs.keySet().iterator().next();
            r = isOnlyOneRecordInEachWaveCheckW2(w1ID, cr, w2ID);
        }
        return r;
    }

    private boolean isOnlyOneRecordInEachWaveCheckW2(WaAS_W1ID w1ID,
            WaAS_CombinedRecord cr, WaAS_W2ID w2ID) {
        boolean r = false;
        if (cr.w3Recs.containsKey(w2ID)) {
            HashMap<WaAS_W3ID, WaAS_W3Record> w3_2 = cr.w3Recs.get(w2ID);
            if (w3_2.size() > 1) {
                env.log("Multiple Wave 3 records for CASEW2 " + w2ID + " in "
                        + "CASEW1 " + w1ID + "!");
            } else {
                WaAS_W3ID CASEW3 = w3_2.keySet().iterator().next();
                r = isOnlyOneRecordInEachWaveCheckW3(w1ID, cr, w2ID,
                        CASEW3);
            }
        } else {
            env.log("No Wave 3 record for " + w2ID + " in " + w1ID + "!");
        }
        return r;
    }

    private boolean isOnlyOneRecordInEachWaveCheckW3(WaAS_W1ID w1ID,
            WaAS_CombinedRecord cr, WaAS_W2ID w2ID, WaAS_W3ID w3ID) {
        boolean r = false;
        if (cr.w4Recs.containsKey(w2ID)) {
            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, WaAS_W4Record>> w4_2;
            w4_2 = cr.w4Recs.get(w2ID);
            if (w4_2.containsKey(w3ID)) {
                HashMap<WaAS_W4ID, WaAS_W4Record> w4_3 = w4_2.get(w3ID);
                if (w4_3.size() > 1) {
                    env.log("Multiple Wave 4 records for " + w3ID + " in "
                            + w2ID + " in " + w1ID + "!");
                } else {
                    WaAS_W4ID w4ID = w4_3.keySet().iterator().next();
                    r = isOnlyOneRecordInEachWaveCheckW4(w1ID, cr, w2ID,
                            w3ID, w4ID);
                }
            } else {
                env.log("No Wave 4 record for " + w3ID + " in " + w2ID + " in "
                        + w1ID + "!");
            }
        } else {
            env.log("No Wave 4 records for " + w2ID + " in " + w1ID + "!");
        }
        return r;
    }

    private boolean isOnlyOneRecordInEachWaveCheckW4(WaAS_W1ID w1ID,
            WaAS_CombinedRecord cr, WaAS_W2ID w2ID, WaAS_W3ID w3ID,
            WaAS_W4ID w4ID) {
        boolean r = false;
        if (cr.w5Recs.containsKey(w2ID)) {
            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>>> w5_2;
            w5_2 = cr.w5Recs.get(w2ID);
            if (w5_2.containsKey(w3ID)) {
                HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>> w5_3 = w5_2.get(w3ID);
                if (w5_3.containsKey(w4ID)) {
                    HashMap<WaAS_W5ID, WaAS_W5Record> w5_4 = w5_3.get(w4ID);
                    if (w5_4.size() > 1) {
                        env.log("Multiple Wave 5 records for " + w4ID + " in "
                                + w3ID + " in " + w2ID + " in " + w1ID + "!");
                    } else {
                        r = true;
                    }
                } else {
                    env.log("No Wave 5 record for " + w4ID + " in " + w3ID
                            + " in " + w2ID + " in " + w1ID + "!");
                }
            } else {
                env.log("No Wave 5 records for " + w3ID + " in " + w2ID + " in "
                        + w1ID + "!");
            }
        } else {
            env.log("No Wave 5 records for " + w2ID + " in " + w1ID + "!");
        }
        return r;
    }

    /**
     * Checks if cr has the same number of adults in each wave for those hholds
     * that have only 1 record for each wave (isSameNumberOfAdultsInEachWave).
     *
     * @param w1ID
     * @param cr
     * @return true iff cr has only 1 record for each wave.
     */
    public boolean isSameNumberOfAdultsInEachWave(WaAS_W1ID w1ID,
            WaAS_CombinedRecord cr) {
        boolean r = false;
        if (cr.w2Recs.size() > 1) {
            env.log("Multiple Wave 2 records for " + w1ID + "!");
        } else {
            Iterator<WaAS_W2ID> ite = cr.w2Recs.keySet().iterator();
            while (ite.hasNext()) {
                WaAS_W2ID w2ID = ite.next();
                WaAS_W2Record w2rec = cr.w2Recs.get(w2ID);
                r = isSameNumberOfAdultsInEachWaveCheckW2(w1ID, cr, w2ID, w2rec);
            }
        }
        return r;
    }

    private boolean isSameNumberOfAdultsInEachWaveCheckW2(WaAS_W1ID w1ID,
            WaAS_CombinedRecord cr, WaAS_W2ID w2ID, WaAS_W2Record w2rec) {
        boolean r = false;
        if (cr.w3Recs.containsKey(w2ID)) {
            HashMap<WaAS_W3ID, WaAS_W3Record> w3_2 = cr.w3Recs.get(w2ID);
            if (w3_2.size() > 1) {
                env.log("Multiple Wave 3 records for " + w2ID + " in " + w1ID + "!");
            } else {
                Iterator<WaAS_W3ID> ite = w3_2.keySet().iterator();
                while (ite.hasNext()) {
                    WaAS_W3ID w3ID = ite.next();
                    WaAS_W3Record w3rec = w3_2.get(w3ID);
                    r = isSameNumberOfAdultsInEachWaveCheckW3(w1ID, cr, w2ID,
                            w2rec, w3ID, w3rec);
                }
            }
        } else {
            env.log("No Wave 3 record for " + w2ID + " in " + w1ID + "!");
        }
        return r;
    }

    private boolean isSameNumberOfAdultsInEachWaveCheckW3(WaAS_W1ID w1ID,
            WaAS_CombinedRecord cr, WaAS_W2ID w2ID, WaAS_W2Record w2rec,
            WaAS_W3ID w3ID, WaAS_W3Record w3rec) {
        boolean r = false;
        if (cr.w4Recs.containsKey(w2ID)) {
            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, WaAS_W4Record>> w4_2;
            w4_2 = cr.w4Recs.get(w2ID);
            if (w4_2.containsKey(w3ID)) {
                HashMap<WaAS_W4ID, WaAS_W4Record> w4_3 = w4_2.get(w3ID);
                if (w4_3.size() > 1) {
                    env.log("Multiple Wave 4 records for " + w3ID + " in "
                            + w2ID + " in " + w1ID + "!");
                } else {
                    Iterator<WaAS_W4ID> ite = w4_3.keySet().iterator();
                    while (ite.hasNext()) {
                        WaAS_W4ID w4ID = ite.next();
                        WaAS_W4Record w4rec = w4_3.get(w4ID);
                        r = isSameNumberOfAdultsInEachWaveCheckW4(w1ID, cr,
                                w2ID, w2rec, w3ID, w3rec, w4ID, w4rec);
                    }
                }
            } else {
                env.log("No Wave 4 records for " + w3ID + " in " + w2ID + " in "
                        + w1ID + "!");
            }
        } else {
            env.log("No Wave 4 records for " + w2ID + " in " + w1ID + "!");
        }
        return r;
    }

    private boolean isSameNumberOfAdultsInEachWaveCheckW4(WaAS_W1ID w1ID,
            WaAS_CombinedRecord cr, WaAS_W2ID w2ID, WaAS_W2Record w2rec,
            WaAS_W3ID w3ID, WaAS_W3Record w3rec, WaAS_W4ID w4ID,
            WaAS_W4Record w4rec) {
        boolean r = false;
        if (cr.w5Recs.containsKey(w2ID)) {
            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>>> w5_2;
            w5_2 = cr.w5Recs.get(w2ID);
            if (w5_2.containsKey(w3ID)) {
                HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>> w5_3;
                w5_3 = w5_2.get(w3ID);
                if (w5_3.containsKey(w4ID)) {
                    HashMap<WaAS_W5ID, WaAS_W5Record> w5_4 = w5_3.get(w4ID);
                    if (w5_4.size() > 1) {
                        env.log("Multiple Wave 5 records for " + w4ID + " in "
                                + w3ID + " in " + w2ID + " in " + w1ID + "!");
                    } else {
                        Iterator<WaAS_W5ID> ite = w5_4.keySet().iterator();
                        while (ite.hasNext()) {
                            WaAS_W5ID w5ID = ite.next();
                            WaAS_W5Record w5rec = w5_4.get(w5ID);
                            byte w1 = cr.w1Rec.getHhold().getNUMADULT();
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
                    env.log("No Wave 5 records for " + w4ID + " in " + w3ID
                            + " in " + w2ID + " in " + w1ID + "!");
                }
            } else {
                env.log("No Wave 5 records for " + w3ID + " in " + w2ID
                        + " in " + w1ID + "!");
            }
        } else {
            env.log("No Wave 5 records for " + w2ID + " in " + w1ID + "!");
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
     * @param w1ID
     * @param cr
     * @return true iff cr has only 1 record for each wave.
     */
    public boolean isSameHHoldCompositionInEachWave(WaAS_W1ID w1ID,
            WaAS_CombinedRecord cr) {
        boolean r = false;
        if (cr.w2Recs.size() > 1) {
            env.log("Multiple Wave 2 records for " + w1ID + "!");
        }
        Iterator<WaAS_W2ID> ite = cr.w2Recs.keySet().iterator();
        while (ite.hasNext()) {
            WaAS_W2ID w2ID = ite.next();
            WaAS_W2Record w2rec = cr.w2Recs.get(w2ID);
            r = isSameHHoldCompositionInEachWaveCheckW2(w1ID, cr, w2ID, w2rec);
        }
        return r;
    }

    private boolean isSameHHoldCompositionInEachWaveCheckW2(WaAS_W1ID w1ID,
            WaAS_CombinedRecord cr, WaAS_W2ID w2ID, WaAS_W2Record w2rec) {
        boolean r = false;
        if (cr.w3Recs.containsKey(w2ID)) {
            HashMap<WaAS_W3ID, WaAS_W3Record> w3_2 = cr.w3Recs.get(w2ID);
            if (w3_2.size() > 1) {
                env.log("Multiple Wave 3 records for " + w2ID + " in " + w1ID + "!");
            } else {
                Iterator<WaAS_W3ID> ite = w3_2.keySet().iterator();
                while (ite.hasNext()) {
                    WaAS_W3ID w3ID = ite.next();
                    WaAS_W3Record w3rec = w3_2.get(w3ID);
                    r = isSameHHoldCompositionInEachWaveCheckW3(w1ID, cr, w2ID,
                            w2rec, w3ID, w3rec);
                }
            }
        } else {
            env.log("No Wave 3 records for " + w2ID + " in " + w1ID + "!");
        }
        return r;
    }

    private boolean isSameHHoldCompositionInEachWaveCheckW3(WaAS_W1ID w1ID,
            WaAS_CombinedRecord cr, WaAS_W2ID w2ID, WaAS_W2Record w2rec,
            WaAS_W3ID w3ID, WaAS_W3Record w3rec) {
        boolean r = false;
        if (cr.w4Recs.containsKey(w2ID)) {
            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, WaAS_W4Record>> w4_2;
            w4_2 = cr.w4Recs.get(w2ID);
            if (w4_2.containsKey(w3ID)) {
                if (w4_2.size() > 1) {
                    env.log("Multiple Wave 4 records for " + w3ID + " in "
                            + w2ID + " in " + w1ID + "!");
                } else {
                    HashMap<WaAS_W4ID, WaAS_W4Record> w4_3 = w4_2.get(w3ID);
                    Iterator<WaAS_W4ID> ite = w4_3.keySet().iterator();
                    while (ite.hasNext()) {
                        WaAS_W4ID w4ID = ite.next();
                        WaAS_W4Record w4rec = w4_3.get(w4ID);
                        r = isSameHHoldCompositionInEachWaveCheckW4(w1ID, cr,
                                w2ID, w2rec, w3ID, w3rec, w4ID, w4rec);
                    }
                }
            } else {
                env.log("No Wave 4 records for " + w3ID + " in " + w2ID + " in "
                        + w1ID + "!");
            }
        } else {
            env.log("No Wave 4 records for " + w2ID + " in " + w1ID + "!");
        }
        return r;
    }

    private boolean isSameHHoldCompositionInEachWaveCheckW4(WaAS_W1ID w1ID,
            WaAS_CombinedRecord cr, WaAS_W2ID w2ID, WaAS_W2Record w2rec,
            WaAS_W3ID w3ID, WaAS_W3Record w3rec, WaAS_W4ID w4ID,
            WaAS_W4Record w4rec) {
        boolean r = false;
        if (cr.w5Recs.containsKey(w2ID)) {
            HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>>> w5_2;
            w5_2 = cr.w5Recs.get(w2ID);
            if (w5_2.containsKey(w3ID)) {
                if (w5_2.size() > 1) {
                    env.log("Multiple Wave 5 records for " + w3ID + " in "
                            + w2ID + " in " + w1ID + "!");
                } else {
                    HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>> w5_3;
                    w5_3 = w5_2.get(w3ID);
                    if (w5_3.containsKey(w4ID)) {
                        HashMap<WaAS_W5ID, WaAS_W5Record> w5_4 = w5_3.get(w4ID);
                        if (w5_4.size() > 1) {
                            env.log("Multiple Wave 5 records for " + w4ID
                                    + " in " + w3ID + " in " + w2ID + " in "
                                    + w1ID + "!");
                        } else {
                            Iterator<WaAS_W5ID> ite5 = w5_4.keySet().iterator();
                            while (ite5.hasNext()) {
                                WaAS_W5ID w5ID = ite5.next();
                                WaAS_W5Record w5rec = w5_4.get(w5ID);
                                r = isSameBasicHHoldComposition(cr, w2rec,
                                        w3rec, w4rec, w5rec);
                            }
                        }
                    } else {
                        env.log("No Wave 5 records for " + w4ID + " in " + w3ID
                                + " in " + w2ID + " in " + w1ID + "!");
                    }
                }
            } else {
                env.log("No Wave 5 records for " + w3ID + " in " + w2ID + " in "
                        + w1ID + "!");
            }
        } else {
            env.log("No Wave 5 records for " + w2ID + " in " + w1ID + "!");
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
        WaAS_W1HRecord w1hhold = cr.w1Rec.getHhold();
        ArrayList<WaAS_W1PRecord> w1people = cr.w1Rec.getPeople();
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
        Iterator<T> ite = people.iterator();
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

    /**
     *
     * @param wave
     * @param f
     * @return
     */
    public String getMessage(byte wave, File f) {
        return "load wave " + wave + " " + getType() + " " + WaAS_Strings.s_WaAS
                + " from " + f;
    }

    /**
     * TreeMap<Short, WaAS_W1HRecord>
     *
     * @param type
     * @return Object[]
     */
    public TreeMap<WaAS_W1ID, WaAS_W1Record> loadCachedSubsetW1(String type) {
        String m = "loadCachedSubsetW1(" + type + ")";
        env.logStartTag(m);
        TreeMap<WaAS_W1ID, WaAS_W1Record> r;
        File f = getSubsetCacheFile(W1, type);
        if (f.exists()) {
            r = (TreeMap<WaAS_W1ID, WaAS_W1Record>) Generic_IO.readObject(f);
        } else {
            env.log("File " + f + " does not exist!");

            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<WaAS_W2ID, WaAS_W2Record> loadCachedSubsetW2(String type) {
        String m = "loadCachedSubsetW2(" + type + ")";
        env.logStartTag(m);
        TreeMap<WaAS_W2ID, WaAS_W2Record> r;
        File f = getSubsetCacheFile(W2, type);
        if (f.exists()) {
            r = (TreeMap<WaAS_W2ID, WaAS_W2Record>) Generic_IO.readObject(f);
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<WaAS_W3ID, WaAS_W3Record> loadCachedSubsetW3(String type) {
        String m = "loadCachedSubsetW3(" + type + ")";
        env.logStartTag(m);
        TreeMap<WaAS_W3ID, WaAS_W3Record> r;
        File f = getSubsetCacheFile(W3, type);
        if (f.exists()) {
            r = (TreeMap<WaAS_W3ID, WaAS_W3Record>) Generic_IO.readObject(f);
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<WaAS_W4ID, WaAS_W4Record> loadCachedSubsetW4(String type) {
        String m = "loadCachedSubsetW4(" + type + ")";
        env.logStartTag(m);
        TreeMap<WaAS_W4ID, WaAS_W4Record> r;
        File f = getSubsetCacheFile(W4, type);
        if (f.exists()) {
            r = (TreeMap<WaAS_W4ID, WaAS_W4Record>) Generic_IO.readObject(f);
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<WaAS_W5ID, WaAS_W5Record> loadCachedSubsetW5(String type) {
        String m = "loadCachedSubsetW5(" + type + ")";
        env.logStartTag(m);
        TreeMap<WaAS_W5ID, WaAS_W5Record> r;
        File f = getSubsetCacheFile(W5, type);
        if (f.exists()) {
            r = (TreeMap<WaAS_W5ID, WaAS_W5Record>) Generic_IO.readObject(f);
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 3 records that have w3ID values in {@code s}.
     *
     * @param s a set containing w3ID values.
     * @param type for loading an already computed result. Expected values
     * include: "InW2W4W5".
     *
     * @return the loaded collections
     */
    public WaAS_W3Data loadW3InSAndW2(Set<WaAS_W3ID> s, String type) {
        String m = "loadW3InSAndW2(Set<WaAS_W3ID>, " + type + ")";
        env.logStartTag(m);
        WaAS_W3Data r;
        File cf = getSubsetCacheFile2(W3, type);
        if (cf.exists()) {
            r = (WaAS_W3Data) load(W3, cf);
        } else {
            r = new WaAS_W3Data();
            File f = getInputFile(W3);
            String m1 = getMessage(W3, f);
            env.logStartTag(m1);
            BufferedReader br = loadW3Count(r, f);
            br.lines().skip(1).forEach((l) -> {
                WaAS_W3HRecord rec = new WaAS_W3HRecord(l);
                WaAS_W3ID w3ID = env.data.CASEW3_To_w3.get(rec.getCASEW3());
                WaAS_W3Record w3rec = new WaAS_W3Record(env, w3ID, rec);
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (s.contains(w3ID)) {
                    if (CASEW2 > Short.MIN_VALUE) {
                        //WaAS_W2ID w2ID = new WaAS_W2ID(CASEW2);
                        WaAS_W2ID w2ID = env.data.CASEW2_To_w2.get(CASEW2);
                        r.w3_To_w2.put(w3ID, w2ID);
                        Generic_Collections.addToMap(r.w2_To_w3, w2ID, w3ID);
                        r.lookup.put(w3ID, w3rec);
                    }
                }
                r.all.add(w3ID);
                if (CASEW1 > Short.MIN_VALUE) {
                    //WaAS_W1ID w1ID = new WaAS_W1ID(CASEW1);
                    WaAS_W1ID w1ID = env.data.CASEW1_To_w1.get(CASEW1);
                    r.w1_In_w3.add(w1ID);
                    if (CASEW2 > Short.MIN_VALUE) {
                        r.w3_In_w1w2.add(w3ID);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W3, cf, r);
        }
        env.log("r.lookup.size() " + r.lookup.size());
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 2 records that CASEW2 values in {@code s}.
     *
     * @param s a set containing CASEW2 values.
     * @param type for loading an already computed result. Expected value:
     * {@link WaAS_Strings#s_InW1W2W3W4W5}
     *
     * @return the loaded collections
     */
    public WaAS_W2Data loadW2InSAndW1(Set<WaAS_W2ID> s, String type) {
        String m = "loadW2InSAndW1(Set<WaAS_W2ID>, " + type + ")";
        env.logStartTag(m);
        WaAS_W2Data r;
        File cf = getSubsetCacheFile2(W2, type);
        if (cf.exists()) {
            r = (WaAS_W2Data) load(W2, cf);
        } else {
            r = new WaAS_W2Data();
            File f = getInputFile(W2);
            String m0 = getMessage(W2, f);
            env.logStartTag(m0);
            BufferedReader br = loadW2Count(r, f);
            br.lines().skip(1).forEach((l) -> {
                WaAS_W2HRecord rec = new WaAS_W2HRecord(l);
                WaAS_W2ID w2ID = env.data.CASEW2_To_w2.get(rec.getCASEW2());
                WaAS_W2Record w2rec = new WaAS_W2Record(env, w2ID, rec);
                short CASEW1 = rec.getCASEW1();
                if (s.contains(w2ID)) {
                    if (CASEW1 > Short.MIN_VALUE) {
                        //WaAS_W1ID w1ID = new WaAS_W1ID(CASEW1);
                        WaAS_W1ID w1ID = env.data.CASEW1_To_w1.get(CASEW1);
                        r.w2_To_w1.put(w2ID, w1ID);
                        Generic_Collections.addToMap(r.w1_To_w2, w1ID, w2ID);
                        r.lookup.put(w2ID, w2rec);
                        r.w1_In_w2.add(w1ID);
                    }
                }
                r.all.add(w2ID);
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m0);
            cache(W2, cf, r);
        }
        env.log("r.lookup.size()" + r.lookup.size());
        env.logEndTag(m);
        return r;
    }

    protected BufferedReader loadW3Count(WaAS_W3Data r, File f) {
        BufferedReader br = Generic_IO.getBufferedReader(f);
        int count = br.lines().skip(1).mapToInt((l) -> {
            WaAS_W3HRecord rec = new WaAS_W3HRecord(l);
            short CASEW2 = rec.getCASEW2();
            if (CASEW2 > Short.MIN_VALUE) {
                //WaAS_W2ID w2ID = new WaAS_W2ID(CASEW2);
                WaAS_W2ID w2ID = env.data.CASEW2_To_w2.get(CASEW2);
                if (!r.w2_In_w3.add(w2ID)) {
                    env.log("In Wave 3: hhold with CASEW2 " + CASEW2 + " reportedly split into multiple hholds.");
                    return 1;
                }
            }
            return 0;
        }).sum();
        env.log("There are " + count + " hholds from Wave 2 that " + "reportedly split into multiple hholds in Wave 3.");
        // Close and reopen br
        br = Generic_IO.closeAndGetBufferedReader(br, f);
        return br;
    }

    //    /**
    //     * Load Wave 3 records that have w3ID values in {@code s}.
    //     *
    //     * @param s a set containing w3ID values.
    //     * @param type for loading an already computed result. Expected values
    //     * include: "InW2W4W5" and "InW4".
    //     *
    //     * @return r An Object[] of length 4:
    //     * <ul>
    //     * <li>r[0] is a TreeMap with keys as w3ID and values as
    //     * WaAS_Wave3_HHOLD_Records. For {@code type.equalsIgnoreCase("InW2W4W5")}
    //     * this only contains records for households also in Wave 2, Wave 4 and Wave
    //     * 5. For {@code type.equalsIgnoreCase("InW4")} this only contains records
    //     * for households also in Wave 4.</li>
    //     * <li>r[1] is an array of TreeSets where:
    //     * <ul>
    //     * <li>r[1][0] is a list of CASEW1 values in Wave 3 records.</li>
    //     * <li>r[1][1] is a list of CASEW2 values in Wave 3 records.</li>
    //     * <li>r[1][2] is a list of all w3ID values.</li>
    //     * <li>r[1][3] is a list of w3ID values for records that have CASEW2 and
    //     * CASEW1 values.</li>
    //     * </ul>
    //     * </li>
    //     * <li>r[2] is a TreeMap with keys as w3ID and values as CASEW2.</li>
    //     * <li>r[3] is a TreeMap with keys as CASEW2 and values as HashSets of
    //     * w3ID (which is normally expected to contain just one w3ID).</li>
    //     * </ul>
    //     */
    //    public Object[] loadW3InW2(Set<Short> s, String type) {
    //        String m = "loadW3InW2(Set<Short>, " + type + ")";
    //        e.logStartTag(m);
    //        Object[] r;
    //        File cf = getSubsetCacheFile2(W3, type);
    //        if (cf.exists()) {
    //            r = (Object[]) load(W3, cf);
    //        } else {
    //            r = new Object[4];
    //            File f = getInputFile(W3);
    //            TreeMap<Short, WaAS_W3HRecord> r0 = new TreeMap<>();
    //            r[0] = r0;
    //            TreeSet<Short>[] r1 = getSets(W3 + 1);
    //            r[1] = r1;
    //            /**
    //             * Each hhold in Wave 3 comes from at most one hhold from Wave 2. It
    //             * may be that in the person files there are individuals that have
    //             * come from different hholds in Wave 2 into a hhold in Wave 3. This
    //             * is expected to be rare. One example explanation for this
    //             * happening is someone returning to a hhold having left it.
    //             */
    //            TreeMap<Short, Short> CASEW3ToCASEW2 = new TreeMap<>();
    //            r[2] = CASEW3ToCASEW2;
    //            /**
    //             * There may be instances where hholds from Wave 2 split into two or
    //             * more hholds in Wave 3.
    //             */
    //            TreeMap<Short, HashSet<Short>> CASEW2ToCASEW3 = new TreeMap<>();
    //            r[3] = CASEW2ToCASEW3;
    //            String m1 = getMessage(W3, f);
    //            e.logStartTag(m1);
    //            BufferedReader br = Generic_IO.getBufferedReader(f);
    //            int count = br.lines().skip(1).mapToInt(l -> {
    //                WaAS_W3HRecord rec = new WaAS_W3HRecord(l);
    //                short CASEW2 = rec.getCASEW2();
    //                if (CASEW2 > Short.MIN_VALUE) {
    //                    if (!r1[1].add(CASEW2)) {
    //                        e.log("Between Wave 2 and 3: hhold with CASEW2 "
    //                                + CASEW2 + " reportedly split into multiple "
    //                                + "hholds.");
    //                        return 1;
    //                    }
    //                }
    //                return 0;
    //            }).sum();
    //            e.log("There are " + count + " hholds from Wave 2 "
    //                    + "reportedly split into multiple hholds in Wave 3.");
    //            // Close and reopen br
    //            br = Generic_IO.closeAndGetBufferedReader(br, f);
    //            if (type.equalsIgnoreCase("InW2W4W5")) {
    //                br.lines().skip(1).forEach(l -> {
    //                    WaAS_W3HRecord rec = new WaAS_W3HRecord(l);
    //                    short w3ID = rec.getCASEW3();
    //                    short CASEW2 = rec.getCASEW2();
    //                    short CASEW1 = rec.getCASEW1();
    //                    if (s.contains(w3ID)) {
    //                        if (CASEW2 > Short.MIN_VALUE) {
    //                            CASEW3ToCASEW2.put(w3ID, CASEW2);
    //                            Generic_Collections.addToMap(CASEW2ToCASEW3, CASEW2,
    //                                    w3ID);
    //                            r0.put(w3ID, rec);
    //                        }
    //                    }
    //                    r1[2].add(w3ID);
    //                    if (CASEW1 > Short.MIN_VALUE) {
    //                        r1[0].add(CASEW1);
    //                        if (CASEW2 > Short.MIN_VALUE) {
    //                            r1[3].add(w3ID);
    //                        }
    //                    }
    //                });
    //            } else if (type.equalsIgnoreCase("InW4")) {
    //                br.lines().skip(1).forEach(l -> {
    //                    WaAS_W3HRecord rec = new WaAS_W3HRecord(l);
    //                    short w3ID = rec.getCASEW3();
    //                    short CASEW2 = rec.getCASEW2();
    //                    short CASEW1 = rec.getCASEW1();
    //                    if (s.contains(w3ID)) {
    //                        r0.put(w3ID, rec);
    //                        if (CASEW2 > Short.MIN_VALUE) {
    //                            CASEW3ToCASEW2.put(w3ID, CASEW2);
    //                            Generic_Collections.addToMap(CASEW2ToCASEW3, CASEW2,
    //                                    w3ID);
    //                        }
    //                    }
    //                    r1[2].add(w3ID);
    //                    if (CASEW1 > Short.MIN_VALUE) {
    //                        r1[0].add(CASEW1);
    //                        if (CASEW2 > Short.MIN_VALUE) {
    //                            r1[3].add(w3ID);
    //                        }
    //                    }
    //                });
    //            } else {
    //                e.log("Unrecognised type " + type);
    //            }
    //            // Close br
    //            Generic_IO.closeBufferedReader(br);
    //            e.logEndTag(m1);
    //            cache(W3, cf, r);
    //        }
    //        e.logEndTag(m);
    //        return r;
    //    }
    /**
     * Load Wave 2 records that are reportedly in Wave 1 (those with CASEW1
     * values).
     *
     * @return the loaded collections
     */
    public WaAS_W2Data loadW2InW1() {
        String m = "loadW2InW1";
        env.logStartTag(m);
        WaAS_W2Data r;
        File cf = getSubsetCacheFile2(W2, "InW1");
        if (cf.exists()) {
            r = (WaAS_W2Data) load(W2, cf);
        } else {
            r = new WaAS_W2Data();
            File f = getInputFile(W2);
            String m0 = getMessage(W2, f);
            env.logStartTag(m0);
            BufferedReader br = loadW2Count(r, f);
            br.lines().skip(1).forEach((l) -> {
                WaAS_W2HRecord rec = new WaAS_W2HRecord(l);
                WaAS_W2ID w2ID = env.data.CASEW2_To_w2.get(rec.getCASEW2());
                WaAS_W2Record w2rec = new WaAS_W2Record(env, w2ID, rec);
                short CASEW1 = rec.getCASEW1();
                if (CASEW1 > Short.MIN_VALUE) {
                    WaAS_W1ID w1ID = env.data.CASEW1_To_w1.get(CASEW1);
                    r.w2_To_w1.put(w2ID, w1ID);
                    Generic_Collections.addToMap(r.w1_To_w2, w1ID, w2ID);
                    r.lookup.put(w2ID, w2rec);
                    r.w2_In_w1.add(w2ID);
                }
                r.all.add(w2ID);
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m0);
            cache(W3, cf, r);
        }
        env.log("r.lookup.size()" + r.lookup.size());
        env.logEndTag(m);
        return r;
    }

    protected BufferedReader loadW2Count(WaAS_W2Data r, File f) {
        BufferedReader br = Generic_IO.getBufferedReader(f);
        int count = br.lines().skip(1).mapToInt((l) -> {
            WaAS_W2HRecord rec = new WaAS_W2HRecord(l);
            short CASEW1 = rec.getCASEW1();
            if (CASEW1 > Short.MIN_VALUE) {
                //WaAS_W1ID w1ID = new WaAS_W1ID(CASEW1);
                WaAS_W1ID w1ID = env.data.CASEW1_To_w1.get(CASEW1);
                if (!r.w1_In_w2.add(w1ID)) {
                    env.log("Between Wave 1 and 2: hhold with CASEW1 " + CASEW1 + " reportedly split into multiple hholds.");
                    return 1;
                }
            }
            return 0;
        }).sum();
        env.log("There are " + count + " hholds from Wave 1 that reportedly " + "split into multiple hholds in Wave 2.");
        // Close and reopen br
        br = Generic_IO.closeAndGetBufferedReader(br, f);
        return br;
    }

    /**
     * Load Wave 3 records that are reportedly in Wave 2 (those with CASEW2
     * values).
     *
     * @return the loaded collections
     */
    public WaAS_W3Data loadW3InW2() {
        String m = "loadW3InW2";
        env.logStartTag(m);
        WaAS_W3Data r;
        File cf = getSubsetCacheFile2(W3, WaAS_Strings.s__In_ + WaAS_Strings.s_w2);
        if (cf.exists()) {
            r = (WaAS_W3Data) load(W3, cf);
        } else {
            r = new WaAS_W3Data();
            File f = getInputFile(W3);
            /**
             * Each hhold in Wave 3 comes from at most one hhold from Wave 2. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 2 into a hhold in Wave 3. This
             * is expected to be rare. One example explanation for this
             * happening is someone returning to a hhold having left it.
             */
            String m0 = getMessage(W3, f);
            env.logStartTag(m0);
            BufferedReader br = loadW3Count(r, f);
            br.lines().skip(1).forEach((l) -> {
                WaAS_W3HRecord rec = new WaAS_W3HRecord(l);
                WaAS_W3ID w3ID = env.data.CASEW3_To_w3.get(rec.getCASEW3());
                WaAS_W3Record w3rec = new WaAS_W3Record(env, w3ID, rec);
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (CASEW2 > Short.MIN_VALUE) {
                    //WaAS_W2ID w2ID = new WaAS_W2ID(CASEW2);
                    WaAS_W2ID w2ID = env.data.CASEW2_To_w2.get(CASEW2);
                    r.w3_To_w2.put(w3ID, w2ID);
                    Generic_Collections.addToMap(r.w2_To_w3, w2ID, w3ID);
                    r.lookup.put(w3ID, w3rec);
                }
                r.all.add(w3ID);
                if (CASEW1 > Short.MIN_VALUE) {
                    WaAS_W1ID w1ID = env.data.CASEW1_To_w1.get(CASEW1);
                    r.w1_In_w3.add(w1ID);
                    if (CASEW2 > Short.MIN_VALUE) {
                        r.w3_In_w1w2.add(w3ID);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m0);
            cache(W3, cf, r);
        }
        env.log("r.lookup.size()" + r.lookup.size());
        env.logEndTag(m);
        return r;
    }

    public TreeMap<WaAS_W5ID, WaAS_W5Record> loadCachedSubset2W5(String type) {
        String m = "loadCachedSubset2W5(" + type + ")";
        env.logStartTag(m);
        TreeMap<WaAS_W5ID, WaAS_W5Record> r;
        File f = getSubsetCacheFile2(W5, type);
        if (f.exists()) {
            WaAS_W5Data o = (WaAS_W5Data) Generic_IO.readObject(f);
            r = (TreeMap<WaAS_W5ID, WaAS_W5Record>) o.lookup;
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * TreeMap<Short, WaAS_W1HRecord>
     *
     * @param type
     * @return Object[]
     */
    public TreeMap<WaAS_W1ID, WaAS_W1Record> loadCachedSubset2W1(String type) {
        String m = "loadCachedSubset2W1(" + type + ")";
        env.logStartTag(m);
        TreeMap<WaAS_W1ID, WaAS_W1Record> r;
        File f = getSubsetCacheFile2(W1, type);
        if (f.exists()) {
            WaAS_W1Data o = (WaAS_W1Data) Generic_IO.readObject(f);
            r = (TreeMap<WaAS_W1ID, WaAS_W1Record>) o.lookup;
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 3 records that have w3ID values in {@code s}.
     *
     * @param s a set containing w3ID values.
     * @param type for loading an already computed result. Expected values
     * include: "InW4".
     *
     * @return the loaded collections
     */
    public WaAS_W3Data loadW3InS(Set<WaAS_W3ID> s, String type) {
        String m = "loadW3InS(Set<WaAS_W3ID>, " + type + ")";
        env.logStartTag(m);
        WaAS_W3Data r;
        File cf = getSubsetCacheFile2(W3, type);
        if (cf.exists()) {
            r = (WaAS_W3Data) load(W3, cf);
        } else {
            r = new WaAS_W3Data();
            File f = getInputFile(W3);
            String m1 = getMessage(W3, f);
            env.logStartTag(m1);
            BufferedReader br = loadW3Count(r, f);
            br.lines().skip(1).forEach((l) -> {
                WaAS_W3HRecord rec = new WaAS_W3HRecord(l);
                WaAS_W3ID w3ID = env.data.CASEW3_To_w3.get(rec.getCASEW3());
                WaAS_W3Record w3rec = new WaAS_W3Record(env, w3ID, rec);
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (s.contains(w3ID)) {
                    r.lookup.put(w3ID, w3rec);
                    if (CASEW2 > Short.MIN_VALUE) {
                        WaAS_W2ID w2ID = env.data.CASEW2_To_w2.get(CASEW2);
                        r.w3_To_w2.put(w3ID, w2ID);
                        Generic_Collections.addToMap(r.w2_To_w3, w2ID, w3ID);
                    }
                }
                r.all.add(w3ID);
                if (CASEW1 > Short.MIN_VALUE) {
                    WaAS_W1ID w1ID = env.data.CASEW1_To_w1.get(CASEW1);
                    r.w1_In_w3.add(w1ID);
                    if (CASEW2 > Short.MIN_VALUE) {
                        r.w3_In_w1w2.add(w3ID);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W3, cf, r);
        }
        env.log("r.lookup.size()" + r.lookup.size());
        env.logEndTag(m);
        return r;
    }

    public TreeMap<WaAS_W3ID, WaAS_W3Record> loadCachedSubset2W3(String type) {
        String m = "loadCachedSubset2W3(" + type + ")";
        env.logStartTag(m);
        TreeMap<WaAS_W3ID, WaAS_W3Record> r;
        File f = getSubsetCacheFile2(W3, type);
        if (f.exists()) {
            WaAS_W3Data o = (WaAS_W3Data) Generic_IO.readObject(f);
            r = (TreeMap<WaAS_W3ID, WaAS_W3Record>) o.lookup;
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 4 records that have w3ID values in {@code s}.
     *
     * @param s a set containing w3ID.
     * @param type for loading an already computed result. Expected values
     * include: "InW3W5" and "InW5".
     *
     * @return the loaded collections
     */
    public WaAS_W4Data loadW4InSAndW3(Set<WaAS_W3ID> s, String type) {
        String m = "loadW4(Set<WaAS_W3ID>, " + type + ")";
        env.logStartTag(m);
        WaAS_W4Data r;
        File cf = getSubsetCacheFile2(W4, type);
        if (cf.exists()) {
            r = (WaAS_W4Data) load(W4, cf);
        } else {
            r = new WaAS_W4Data();
            File f = getInputFile(W4);
            /**
             * Each hhold in Wave 4 comes from at most one hhold from Wave 3. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 3 into a hhold in Wave 4. This
             * is expected to be rare. One example explanation for this
             * happening is someone returning to a hhold having left it.
             */
            String m1 = getMessage(W4, f);
            env.logStartTag(m1);
            BufferedReader br = loadW4Count(r, f);
            br.lines().skip(1).forEach((l) -> {
                WaAS_W4HRecord rec = new WaAS_W4HRecord(l);
                WaAS_W4ID w4ID = env.data.CASEW4_To_w4.get(rec.getCASEW4());
                WaAS_W4Record w4rec = new WaAS_W4Record(env, w4ID, rec);
                short CASEW3 = rec.getCASEW3();
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (CASEW3 > Short.MIN_VALUE) {
                    WaAS_W3ID w3ID = env.data.CASEW3_To_w3.get(CASEW3);
                    if (s.contains(w3ID)) {
                        r.w4_To_w3.put(w4ID, w3ID);
                        Generic_Collections.addToMap(r.w3_To_w4, w3ID, w4ID);
                        r.lookup.put(w4ID, w4rec);
                    }
                }
                r.all.add(w4ID);
                if (CASEW2 > Short.MIN_VALUE) {
                    WaAS_W2ID w2ID = env.data.CASEW2_To_w2.get(CASEW2);
                    r.w2_In_w4.add(w2ID);
                }
                if (CASEW1 > Short.MIN_VALUE) {
                    WaAS_W1ID w1ID = env.data.CASEW1_To_w1.get(CASEW1);
                    r.w1_In_w4.add(w1ID);
                    if (CASEW2 > Short.MIN_VALUE && CASEW3 > Short.MIN_VALUE) {
                        r.w4_In_w1w2w3.add(w4ID);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W4, cf, r);
        }
        env.log("r.lookup.size()" + r.lookup.size());
        env.logEndTag(m);
        return r;
    }

    /**
     * Read through f and count the number of split records. Add to r.w3_In_w4.
     *
     * @param r
     * @param f
     * @return
     */
    protected BufferedReader loadW4Count(WaAS_W4Data r, File f) {
        BufferedReader br = Generic_IO.getBufferedReader(f);
        int count = br.lines().skip(1).mapToInt((l) -> {
            WaAS_W4HRecord rec = new WaAS_W4HRecord(l);
            short CASEW3 = rec.getCASEW3();
            if (CASEW3 > Short.MIN_VALUE) {
                WaAS_W3ID w3ID = env.data.CASEW3_To_w3.get(CASEW3);
                if (w3ID == null) {
                    env.log("In Wave 4: unrecognised CASEW3 " + CASEW3 + " for CASEW4 " + rec.getCASEW4());
                } else {
                    if (!r.w3_In_w4.add(w3ID)) {
                        env.log("In Wave 4: hhold with CASEW3 " + CASEW3 + " reportedly split into multiple hholds.");
                        return 1;
                    }
                }
            }
            return 0;
        }).sum();
        env.log("There are " + count + " hholds from Wave 3 " + "reportedly split into multiple hholds in Wave 4.");
        // Close and reopen br
        br = Generic_IO.closeAndGetBufferedReader(br, f);
        return br;
    }

    /**
     * Load Wave 5 records that are reportedly in Wave 4 (those with CASEW4
     * values).
     *
     * @return the loaded collections
     */
    public WaAS_W5Data loadW5InW4() {
        String m = "loadW5InW4";
        env.logStartTag(m);
        WaAS_W5Data r;
        File cf = getSubsetCacheFile2(W5, WaAS_Strings.s__In_ + WaAS_Strings.s_w4);
        if (cf.exists()) {
            r = (WaAS_W5Data) load(W5, cf);
        } else {
            r = new WaAS_W5Data();
            File f = getInputFile(W5);
            /**
             * Each hhold in Wave 5 comes from at most one hhold from Wave 4. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 4 into a hhold in Wave 5. This
             * is expected to be rare. One example explanation for this
             * happening is someone returning to a hhold having left it.
             */
            String m1 = getMessage(W5, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            int count = br.lines().skip(1).mapToInt((l) -> {
                WaAS_W5HRecord rec = new WaAS_W5HRecord(l);
                short CASEW4 = rec.getCASEW4();
                if (CASEW4 > Short.MIN_VALUE) {
                    WaAS_W4ID w4ID = env.data.CASEW4_To_w4.get(rec.getCASEW4());
                    //WaAS_W4ID w4ID = new WaAS_W4ID(CASEW4);
                    WaAS_W5ID w5ID = env.data.CASEW5_To_w5.get(rec.getCASEW5());
                    //WaAS_W5ID w5ID = new WaAS_W5ID(rec.getCASEW5());
                    if (!r.w4_In_w5.add(w4ID)) {
                        env.log("Between Wave 4 and 5: hhold with CASEW4 " + CASEW4 + " reportedly split into multiple " + "hholds.");
                        return 1;
                    }
                    r.w5_To_w4.put(w5ID, w4ID);
                }
                return 0;
            }).sum();
            env.log("There are " + count + " hholds from Wave 4 that " + "reportedly split into multiple hholds in Wave 5.");
            // Close and reopen br
            br = Generic_IO.closeAndGetBufferedReader(br, f);
            br.lines().skip(1).forEach((l) -> {
                WaAS_W5HRecord rec = new WaAS_W5HRecord(l);
                WaAS_W5ID w5ID = env.data.CASEW5_To_w5.get(rec.getCASEW5());
                WaAS_W5Record w5rec = new WaAS_W5Record(env, w5ID, rec);
                short CASEW4 = rec.getCASEW4();
                short CASEW3 = rec.getCASEW3();
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (CASEW4 > Short.MIN_VALUE) {
                    WaAS_W4ID w4ID = env.data.CASEW4_To_w4.get(CASEW4);
                    Generic_Collections.addToMap(r.w4_To_w5, w4ID, w5ID);
                    r.lookup.put(w5ID, w5rec);
                }
                r.all.add(w5ID);
                if (CASEW3 > Short.MIN_VALUE) {
                    WaAS_W3ID w3ID = env.data.CASEW3_To_w3.get(CASEW3);
                    if (w3ID == null) {
                        env.log("env.data.CASEW3_To_w3.get(CASEW3) = null for CASEW3 " + CASEW3);
                    } else {
                        r.w3_In_w5.add(w3ID);
                    }
                }
                if (CASEW2 > Short.MIN_VALUE) {
                    WaAS_W2ID w2ID = env.data.CASEW2_To_w2.get(CASEW2);
                    r.w2_In_w5.add(w2ID);
                }
                if (CASEW1 > Short.MIN_VALUE) {
                    WaAS_W1ID w1ID = env.data.CASEW1_To_w1.get(CASEW1);
                    r.w1_In_w5.add(w1ID);
                    if (CASEW2 > Short.MIN_VALUE && CASEW3 > Short.MIN_VALUE && CASEW4 > Short.MIN_VALUE) {
                        r.w5_In_w1w2w3w4.add(w5ID);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W5, cf, r);
            env.log("r.lookup.size() " + r.lookup.size());
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 4 records that have CASEW4 values in {@code s}.
     *
     * @param s a set containing w3ID.
     * @param type for loading an already computed result. Expected values
     * include: "InW3W5" and "InW5".
     *
     * @return the loaded collections
     */
    public WaAS_W4Data loadW4InS(Set<WaAS_W4ID> s, String type) {
        String m = "loadW4InS(Set<WaAS_W4ID>, " + type + ")";
        env.logStartTag(m);
        WaAS_W4Data r;
        File cf = getSubsetCacheFile2(W4, type);
        if (cf.exists()) {
            r = (WaAS_W4Data) load(W4, cf);
        } else {
            r = new WaAS_W4Data();
            File f = getInputFile(W4);
            /**
             * Each hhold in Wave 4 comes from at most one hhold from Wave 3. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 3 into a hhold in Wave 4. This
             * is expected to be rare. One example explanation for this
             * happening is someone returning to a hhold having left it.
             */
            String m1 = getMessage(W4, f);
            env.logStartTag(m1);
            BufferedReader br = loadW4Count(r, f);
            br.lines().skip(1).forEach((l) -> {
                WaAS_W4HRecord rec = new WaAS_W4HRecord(l);
                WaAS_W4ID w4ID = env.data.CASEW4_To_w4.get(rec.getCASEW4());
                WaAS_W4Record w4rec = new WaAS_W4Record(env, w4ID, rec);
                short CASEW3 = rec.getCASEW3();
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (s.contains(w4ID)) {
                    r.lookup.put(w4ID, w4rec);
                    if (CASEW3 > Short.MIN_VALUE) {
                        WaAS_W3ID w3ID = env.data.CASEW3_To_w3.get(CASEW3);
                        r.w4_To_w3.put(w4ID, w3ID);
                        if (w3ID == null) {
                            env.log("env.data.CASEW3_To_w3.get(CASEW3) = null for CASEW3 " + CASEW3);
                        } else {
                            Generic_Collections.addToMap(r.w3_To_w4, w3ID, w4ID);
                        }
                    }
                }
                r.all.add(w4ID);
                if (CASEW2 > Short.MIN_VALUE) {
                    WaAS_W2ID w2ID = env.data.CASEW2_To_w2.get(CASEW2);
                    r.w2_In_w4.add(w2ID);
                }
                if (CASEW1 > Short.MIN_VALUE) {
                    WaAS_W1ID w1ID = env.data.CASEW1_To_w1.get(CASEW1);
                    r.w1_In_w4.add(w1ID);
                    if (CASEW2 > Short.MIN_VALUE && CASEW3 > Short.MIN_VALUE) {
                        r.w4_In_w1w2w3.add(w4ID);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W4, cf, r);
        }
        env.log("r.lookup.size() " + r.lookup.size());
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 2 records that CASEW2 values in {@code s}.
     *
     * @param s a set containing CASEW2 values.
     * @param type for loading an already computed result. Expected value:
     * {@link WaAS_Strings#s_InW3}
     *
     * @return the loaded collections
     */
    public WaAS_W2Data loadW2InS(Set<WaAS_W2ID> s, String type) {
        String m = "loadW2InS(Set<WaAS_W2ID>, " + type + ")";
        env.logStartTag(m);
        WaAS_W2Data r;
        File cf = getSubsetCacheFile2(W2, type);
        if (cf.exists()) {
            r = (WaAS_W2Data) load(W2, cf);
        } else {
            r = new WaAS_W2Data();
            File f = getInputFile(W2);
            String m0 = getMessage(W2, f);
            env.logStartTag(m0);
            BufferedReader br = loadW2Count(r, f);
            br.lines().skip(1).forEach((l) -> {
                WaAS_W2HRecord rec = new WaAS_W2HRecord(l);
                WaAS_W2ID w2ID = env.data.CASEW2_To_w2.get(rec.getCASEW2());
                WaAS_W2Record w2rec = new WaAS_W2Record(env, w2ID, rec);
                short CASEW1 = rec.getCASEW1();
                if (s.contains(w2ID)) {
                    r.lookup.put(w2ID, w2rec);
                    if (CASEW1 > Short.MIN_VALUE) {
                        WaAS_W1ID w1ID = env.data.CASEW1_To_w1.get(CASEW1);
                        r.w2_To_w1.put(w2ID, w1ID);
                        Generic_Collections.addToMap(r.w1_To_w2, w1ID, w2ID);
                        r.w1_In_w2.add(w1ID);
                    }
                }
                r.all.add(w2ID);
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m0);
            cache(W2, cf, r);
        }
        env.log("r.lookup.size()" + r.lookup.size());
        env.logEndTag(m);
        return r;
    }

    public TreeMap<WaAS_W4ID, WaAS_W4Record> loadCachedSubset2W4(String type) {
        String m = "loadCachedSubset2W4(" + type + ")";
        env.logStartTag(m);
        TreeMap<WaAS_W4ID, WaAS_W4Record> r;
        File f = getSubsetCacheFile2(W4, type);
        if (f.exists()) {
            WaAS_W4Data o = (WaAS_W4Data) Generic_IO.readObject(f);
            r = (TreeMap<WaAS_W4ID, WaAS_W4Record>) o.lookup;
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    public TreeMap<WaAS_W2ID, WaAS_W2Record> loadCachedSubset2W2(String type) {
        String m = "loadCachedSubset2W2(" + type + ")";
        env.logStartTag(m);
        TreeMap<WaAS_W2ID, WaAS_W2Record> r;
        File f = getSubsetCacheFile2(W2, type);
        if (f.exists()) {
            WaAS_W2Data o = (WaAS_W2Data) Generic_IO.readObject(f);
            r = (TreeMap<WaAS_W2ID, WaAS_W2Record>) o.lookup;
        } else {
            env.log("File " + f + " does not exist!");
            r = null;
        }
        env.logEndTag(m);
        return r;
    }

    /**
     * Load Wave 4 records that are reportedly in Wave 3 (those with w3ID
     * values).
     *
     * @return the loaded collections
     */
    public WaAS_W4Data loadW4InW3() {
        String m = "loadW4InW3";
        env.logStartTag(m);
        WaAS_W4Data r;
        File cf = getSubsetCacheFile2(W4, WaAS_Strings.s__In_ + WaAS_Strings.s_w3);
        if (cf.exists()) {
            r = (WaAS_W4Data) load(W4, cf);
        } else {
            r = new WaAS_W4Data();
            File f = getInputFile(W4);
            /**
             * Each hhold in Wave 4 comes from at most one hhold from Wave 3. It
             * may be that in the person files there are individuals that have
             * come from different hholds in Wave 3 into a hhold in Wave 4. This
             * is expected to be rare. One example explanation for this
             * happening is someone returning to a hhold having left it.
             */
            String m0 = getMessage(W4, f);
            env.logStartTag(m0);
            BufferedReader br = loadW4Count(r, f);
            br.lines().skip(1).forEach((l) -> {
                WaAS_W4HRecord rec = new WaAS_W4HRecord(l);
                WaAS_W4ID w4ID = env.data.CASEW4_To_w4.get(rec.getCASEW4());
                WaAS_W4Record w4rec = new WaAS_W4Record(env, w4ID, rec);
                short CASEW3 = rec.getCASEW3();
                short CASEW2 = rec.getCASEW2();
                short CASEW1 = rec.getCASEW1();
                if (CASEW3 > Short.MIN_VALUE) {
                    WaAS_W3ID w3ID = env.data.CASEW3_To_w3.get(rec.getCASEW3());
                    r.w4_To_w3.put(w4ID, w3ID);
                    Generic_Collections.addToMap(r.w3_To_w4, w3ID, w4ID);
                    r.lookup.put(w4ID, w4rec);
                }
                r.all.add(w4ID);
                if (CASEW2 > Short.MIN_VALUE) {
                    WaAS_W2ID w2ID = env.data.CASEW2_To_w2.get(rec.getCASEW2());
                    r.w2_In_w4.add(w2ID);
                }
                if (CASEW1 > Short.MIN_VALUE) {
                    WaAS_W1ID w1ID = env.data.CASEW1_To_w1.get(rec.getCASEW1());
                    r.w1_In_w4.add(w1ID);
                    if (CASEW2 > Short.MIN_VALUE && CASEW3 > Short.MIN_VALUE) {
                        r.w4_In_w1w2w3.add(w4ID);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m0);
            cache(W4, cf, r);
        }
        env.log("r.lookup.size()" + r.lookup.size());
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
     * @return the loaded collections
     */
    public WaAS_W1Data loadW1(Set<WaAS_W1ID> s, String type) {
        String m = "loadW1(Set<Short>, " + type + ")";
        env.logStartTag(m);
        WaAS_W1Data r;
        File cf = getSubsetCacheFile2(W1, type);
        if (cf.exists()) {
            r = (WaAS_W1Data) load(W1, cf);
        } else {
            r = new WaAS_W1Data();
            File f = getInputFile(W1);
            String m1 = getMessage(W1, f);
            env.logStartTag(m1);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            br.lines().skip(1).forEach((l) -> {
                WaAS_W1HRecord rec = new WaAS_W1HRecord(l);
                short CASEW1 = rec.getCASEW1();
                if (CASEW1 > Short.MIN_VALUE) {
                    WaAS_W1ID w1ID = env.data.CASEW1_To_w1.get(CASEW1);
                    WaAS_W1Record w1rec = new WaAS_W1Record(env, w1ID, rec);
                    if (s.contains(w1ID)) {
                        r.lookup.put(w1ID, w1rec);
                    }
                    r.all.add(w1ID);
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            env.logEndTag(m1);
            cache(W1, cf, r);
        }
        env.log("r.lookup.size()" + r.lookup.size());
        env.logEndTag(m);
        return r;
    }
    
    /**
     * The normal size of a sub-collections when storing the data in c in nOC 
     * sub-collections.
     * @param c
     * @param nOC numberOfCollections
     * @return (int) Math.ceil(c.size()/ (double) numberOfCollections)
     */
    public static int getCSize(Collection c, int nOC) {
        int n = c.size();
        return (int) Math.ceil((double) n / (double) nOC);
    }
}
