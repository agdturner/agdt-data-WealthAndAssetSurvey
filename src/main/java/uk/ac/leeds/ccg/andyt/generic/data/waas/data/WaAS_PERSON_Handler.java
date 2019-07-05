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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.andyt.generic.io.Generic_IO;
import uk.ac.leeds.ccg.andyt.generic.util.Generic_Collections;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Strings;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_W1PRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_W2PRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_W3PRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_W4PRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_W5PRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.io.WaAS_Files;

/**
 *
 * @author geoagdt
 */
public class WaAS_PERSON_Handler extends WaAS_Handler {

    public WaAS_PERSON_Handler(WaAS_Environment e) {
        super(e);
    }

    /**
     *
     * @param wave
     * @return
     */
    protected File getFile(byte wave) {
        return new File(files.getGeneratedWaASDir(),
                getType() + WaAS_Strings.s_W + wave + WaAS_Files.DOT_DAT);
    }

    @Override
    public String getType() {
        return WaAS_Strings.s_person;
    }

    public class DataSubset implements Serializable {

        /**
         * Collection Files
         */
        public TreeMap<WaAS_CollectionID, File> cFs;

        public int cSize;

        public int nOC;

        /**
         * Create a new DataSubset
         *
         * @param nOC
         * @param wave
         */
        public DataSubset(int cSize, int nOC, byte wave) {
            this.cSize = cSize;
            this.nOC = nOC;
            cFs = new TreeMap<>();
            for (short s = 0; s < nOC; s++) {
                File f = new File(files.getGeneratedWaASSubsetsDir(),
                        WaAS_Strings.s_person + wave
                        + WaAS_Strings.symbol_underscore + s + ".tab");
                WaAS_CollectionID cID = new WaAS_CollectionID(s);
                cFs.put(cID, f);
            }
        }
    }

    public class DataSubsetW1 extends DataSubset {

        /**
         * Lookup from WaAS_CollectionID to WaAS_W1ID.
         */
        public TreeMap<WaAS_CollectionID, HashSet<WaAS_W1ID>> c_To_w1;

        /**
         * Lookup from WaAS_W1ID to WaAS_CollectionID.
         */
        public HashMap<WaAS_W1ID, WaAS_CollectionID> w1_To_c;

        /**
         *
         * @param w1IDs
         * @param cSize
         * @param nOC Number of collections.
         * @param wave
         */
        public DataSubsetW1(TreeSet<WaAS_W1ID> w1IDs, int cSize, int nOC,
                byte wave) {
            super(cSize, nOC, wave);
            c_To_w1 = new TreeMap<>();
            w1_To_c = new HashMap<>();
            Iterator<WaAS_W1ID> ite = w1IDs.iterator();
            short s = 0;
            WaAS_CollectionID cID = new WaAS_CollectionID(s);
            int i = 0;
            while (ite.hasNext()) {
                WaAS_W1ID w1ID = ite.next();
                w1_To_c.put(w1ID, cID);
                i++;
                if (i == cSize) {
                    i = 0;
                    s++;
                    cID = new WaAS_CollectionID(s);
                }
            }
        }
    }

    public class DataSubsetW2 extends DataSubset {

        /**
         * Lookup from WaAS_CollectionID to WaAS_W2ID.
         */
        public TreeMap<WaAS_CollectionID, HashSet<WaAS_W2ID>> c_To_w2;

        /**
         * Lookup from WaAS_W2ID to WaAS_CollectionID.
         */
        public HashMap<WaAS_W2ID, WaAS_CollectionID> w2_To_c;

        /**
         *
         * @param w2IDs
         * @param cSize
         * @param nOC Number of collections.
         * @param wave
         */
        public DataSubsetW2(TreeSet<WaAS_W2ID> w2IDs, int cSize, int nOC,
                byte wave) {
            super(cSize, nOC, wave);
            c_To_w2 = new TreeMap<>();
            w2_To_c = new HashMap<>();
            Iterator<WaAS_W2ID> ite = w2IDs.iterator();
            short s = 0;
            WaAS_CollectionID cID = new WaAS_CollectionID(s);
            int i = 0;
            while (ite.hasNext()) {
                WaAS_W2ID w2ID = ite.next();
                w2_To_c.put(w2ID, cID);
                i++;
                if (i == cSize) {
                    i = 0;
                    s++;
                    cID = new WaAS_CollectionID(s);
                }
            }
        }
    }

    public class DataSubsetW3 extends DataSubset {

        /**
         * Lookup from WaAS_CollectionID to WaAS_W3ID.
         */
        public TreeMap<WaAS_CollectionID, HashSet<WaAS_W3ID>> c_To_w3;

        /**
         * Lookup from WaAS_W3ID to WaAS_CollectionID.
         */
        public HashMap<WaAS_W3ID, WaAS_CollectionID> w3_To_c;

        /**
         *
         * @param w3IDs
         * @param cSize
         * @param nOC Number of collections.
         * @param wave
         */
        public DataSubsetW3(TreeSet<WaAS_W3ID> w3IDs, int cSize, int nOC,
                byte wave) {
            super(cSize, nOC, wave);
            c_To_w3 = new TreeMap<>();
            w3_To_c = new HashMap<>();
            Iterator<WaAS_W3ID> ite = w3IDs.iterator();
            short s = 0;
            WaAS_CollectionID cID = new WaAS_CollectionID(s);
            int i = 0;
            while (ite.hasNext()) {
                WaAS_W3ID w3ID = ite.next();
                w3_To_c.put(w3ID, cID);
                i++;
                if (i == cSize) {
                    i = 0;
                    s++;
                    cID = new WaAS_CollectionID(s);
                }
            }
        }
    }

    public class DataSubsetW4 extends DataSubset {

        /**
         * Lookup from WaAS_CollectionID to WaAS_W4ID.
         */
        public TreeMap<WaAS_CollectionID, HashSet<WaAS_W4ID>> c_To_w4;

        /**
         * Lookup from WaAS_W4ID to WaAS_CollectionID.
         */
        public HashMap<WaAS_W4ID, WaAS_CollectionID> w4_To_c;

        /**
         *
         * @param w4IDs
         * @param cSize
         * @param nOC Number of collections.
         * @param wave
         */
        public DataSubsetW4(TreeSet<WaAS_W4ID> w4IDs, int cSize, int nOC,
                byte wave) {
            super(cSize, nOC, wave);
            c_To_w4 = new TreeMap<>();
            w4_To_c = new HashMap<>();
            Iterator<WaAS_W4ID> ite = w4IDs.iterator();
            short s = 0;
            WaAS_CollectionID cID = new WaAS_CollectionID(s);
            int i = 0;
            while (ite.hasNext()) {
                WaAS_W4ID w4ID = ite.next();
                w4_To_c.put(w4ID, cID);
                i++;
                if (i == cSize) {
                    i = 0;
                    s++;
                    cID = new WaAS_CollectionID(s);
                }
            }
        }
    }

    public class DataSubsetW5 extends DataSubset {

        /**
         * Lookup from WaAS_CollectionID to WaAS_W5ID.
         */
        public TreeMap<WaAS_CollectionID, HashSet<WaAS_W5ID>> c_To_w5;

        /**
         * Lookup from WaAS_W5ID to WaAS_CollectionID.
         */
        public HashMap<WaAS_W5ID, WaAS_CollectionID> w5_To_c;

        /**
         *
         * @param w5IDs
         * @param cSize
         * @param nOC Number of collections.
         * @param wave
         */
        public DataSubsetW5(TreeSet<WaAS_W5ID> w5IDs, int cSize, int nOC,
                byte wave) {
            super(cSize, nOC, wave);
            c_To_w5 = new TreeMap<>();
            w5_To_c = new HashMap<>();
            Iterator<WaAS_W5ID> ite = w5IDs.iterator();
            short s = 0;
            WaAS_CollectionID cID = new WaAS_CollectionID(s);
            int i = 0;
            while (ite.hasNext()) {
                WaAS_W5ID w5ID = ite.next();
                w5_To_c.put(w5ID, cID);
                i++;
                if (i == cSize) {
                    i = 0;
                    s++;
                    cID = new WaAS_CollectionID(s);
                }
            }
        }
    }

    /**
     * Loads Wave 1 of the person WaAS for those records with CASEW1 in
     * CASEW1IDs. If the data are in a cache then the cache is loaded otherwise
     * the data are selected and the cache is written for next time.
     *
     * @param w1IDs
     * @param nOC Number of collections.
     * @param wave
     * @return
     */
    public DataSubsetW1 loadSubsetWave1(TreeSet<WaAS_W1ID> w1IDs, int nOC,
            byte wave) {
        File cf = getFile(wave);
        if (cf.exists()) {
            return (DataSubsetW1) Generic_IO.readObject(cf);
        } else {
            // Calculate how many subsets
            int cSize = getCSize(w1IDs, nOC);
            DataSubsetW1 r = new DataSubsetW1(w1IDs, cSize, nOC, wave);
            HashMap<WaAS_CollectionID, PrintWriter> cPWs = initPWs(r.cFs);
            /**
             * Read through the lines and figure out which lines should be put
             * in which collection.
             */
            File f = getInputFile(wave);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            /**
             * Read and write header.
             */
            addHeader(br, cPWs);
            /**
             * Read through the lines and write them to the appropriate files.
             */
            br.lines().skip(1).forEach(l -> {
                WaAS_W1PRecord rec = new WaAS_W1PRecord(l);
                short CASEW1 = rec.getCASEW1();
                if (CASEW1 > Short.MIN_VALUE) {
                    //WaAS_W1ID w1ID = new WaAS_W1ID(CASEW1);
                WaAS_W1ID w1ID = env.data.CASEW1_To_w1.get(CASEW1);
                    if (r.w1_To_c.containsKey(w1ID)) {
                        WaAS_CollectionID cID = r.w1_To_c.get(w1ID);
                        PrintWriter pw = cPWs.get(cID);
                        pw.println(l);
                        Generic_Collections.addToMap(r.c_To_w1, cID, w1ID);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            // Close the PrintWriters.
            closePWs(cPWs);
            cache(wave, cf, r);
            return r;
        }
    }

    /**
     * Initialise cIDs, cPWs and cFs.
     *
     * @param cFs
     * @return
     */
    protected HashMap<WaAS_CollectionID, PrintWriter> initPWs(
            TreeMap<WaAS_CollectionID, File> cFs) {
        HashMap<WaAS_CollectionID, PrintWriter> r = new HashMap<>();
        Iterator<WaAS_CollectionID> ite = cFs.keySet().iterator();
        while (ite.hasNext()) {
            WaAS_CollectionID cID = ite.next();
            File f = cFs.get(cID);
            r.put(cID, Generic_IO.getPrintWriter(f, false));
        }
        return r;
    }

    /**
     * Reads the header from br and writes this out to the
     * collectionIDPrintWriters.
     *
     * @param br
     * @param cPWs
     */
    protected void addHeader(BufferedReader br,
            HashMap<WaAS_CollectionID, PrintWriter> cPWs) {
        try {
            String header = br.readLine();
            cPWs.keySet().stream().forEach(cID -> {
                cPWs.get(cID).println(header);
            });
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
            Logger.getLogger(WaAS_PERSON_Handler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Close the PrintWriters.
     *
     * @param cPWs
     */
    protected void closePWs(HashMap<WaAS_CollectionID, PrintWriter> cPWs) {
        cPWs.keySet().stream().forEach(cID -> {
            cPWs.get(cID).close();
        });
    }

    /**
     * Loads Wave 2 of the person WaAS for those records with CASEW2 in
     * CASEW2IDs.If this data are in a cache then the cache is loaded otherwise
     * the data are selected and the cache is written for next time.
     *
     * @param dataSubsetW1
     * @param wave
     * @param w2_To_w1
     * @return
     */
    public DataSubsetW2 loadSubsetWave2(DataSubsetW1 dataSubsetW1, byte wave,
            TreeMap<WaAS_W2ID, WaAS_W1ID> w2_To_w1) {
        File cf = getFile(wave);
        if (cf.exists()) {
            return (DataSubsetW2) Generic_IO.readObject(cf);
        } else {
            TreeSet<WaAS_W2ID> w2IDs = new TreeSet<>();
            w2IDs.addAll(w2_To_w1.keySet());
            DataSubsetW2 r = new DataSubsetW2(w2IDs, dataSubsetW1.cSize, dataSubsetW1.nOC, wave);
            HashMap<WaAS_CollectionID, PrintWriter> cPWs = initPWs(r.cFs);
            /**
             * Read through the lines and figure out which lines should be put
             * in which collection.
             */
            File f = getInputFile(wave);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            /**
             * Read and write header.
             */
            addHeader(br, cPWs);
            /**
             * Read through the lines and write them to the appropriate files.
             */
            br.lines().skip(1).forEach(l -> {
                WaAS_W2PRecord rec = new WaAS_W2PRecord(l);
                //WaAS_W2ID w2ID = new WaAS_W2ID(rec.getCASEW2());
                WaAS_W2ID w2ID = env.data.CASEW2_To_w2.get(rec.getCASEW2());
                if (w2_To_w1.containsKey(w2ID)) {
                    WaAS_W1ID w1ID = w2_To_w1.get(w2ID);
                    write(dataSubsetW1.w1_To_c, w1ID, cPWs, l);
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            // Close the PrintWriters.
            closePWs(cPWs);
            cache(wave, cf, r);
            return r;
        }
    }

    /**
     *
     * @param w1_To_c the map for looking up collection ID from CASEW1.
     * @param w1ID the key for the collection ID.
     * @param cPWs the collection PrintWriters indexed by the collection ID
     * looked up from CASEW1.
     * @param s the string to write
     */
    private void write(HashMap<WaAS_W1ID, WaAS_CollectionID> w1_To_c,
            WaAS_W1ID w1ID, HashMap<WaAS_CollectionID, PrintWriter> cPWs,
            String s) {
        if (w1_To_c.containsKey(w1ID)) {
            cPWs.get(w1_To_c.get(w1ID)).println(s);
        }
    }

    /**
     *
     * @param c
     * @param nOC numberOfCollections
     * @return (int) Math.ceil(c.size()/ (double) numberOfCollections)
     */
    protected int getCSize(Collection c, int nOC) {
        int n = c.size();
        return (int) Math.ceil((double) n / (double) nOC);
    }

    /**
     * Loads Wave 3 of the person WaAS for those records with CASEW3 in
     * CASEW3IDs.If this data are in a cache then the cache is loaded otherwise
     * the data are selected and the cache is written for next time.
     *
     * @param dataSubsetW2
     * @param w1_To_c
     * @param wave
     * @param w2_To_w1
     * @param w3_To_w2
     * @return
     */
    public DataSubsetW3 loadSubsetWave3(DataSubsetW2 dataSubsetW2,
            HashMap<WaAS_W1ID, WaAS_CollectionID> w1_To_c, byte wave,
            TreeMap<WaAS_W2ID, WaAS_W1ID> w2_To_w1,
            TreeMap<WaAS_W3ID, WaAS_W2ID> w3_To_w2) {
        File cf = getFile(wave);
        if (cf.exists()) {
            return (DataSubsetW3) Generic_IO.readObject(cf);
        } else {
            TreeSet<WaAS_W3ID> w3IDs = new TreeSet<>();
            w3IDs.addAll(w3_To_w2.keySet());
            DataSubsetW3 r = new DataSubsetW3(w3IDs, dataSubsetW2.cSize, dataSubsetW2.nOC, wave);
            HashMap<WaAS_CollectionID, PrintWriter> cPWs = initPWs(r.cFs);

            /**
             * Read through the lines and figure out which lines should be put
             * in which collection.
             */
            File f = getInputFile(wave);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            /**
             * Read and write header.
             */
            addHeader(br, cPWs);

            /**
             * Read through the lines and write them to the appropriate files.
             */
            br.lines().skip(1).forEach(l -> {
                WaAS_W3PRecord rec = new WaAS_W3PRecord(l);
                //WaAS_W3ID w3ID = new WaAS_W3ID(rec.getCASEW3());
                WaAS_W3ID w3ID = env.data.CASEW3_To_w3.get(rec.getCASEW3());
                if (w3_To_w2.containsKey(w3ID)) {
                    WaAS_W2ID w2ID = w3_To_w2.get(w3ID);
                    WaAS_W1ID w1ID = w2_To_w1.get(w2ID);
                    write(w1_To_c, w1ID, cPWs, l);
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            // Close the PrintWriters.
            closePWs(cPWs);
            cache(wave, cf, r);
            return r;
        }
    }

    /**
     * Loads Wave 4 of the person WaAS for those records with CASEW4 in
     * CASEW4IDs.If this data are in a cache then the cache is loaded otherwise
     * the data are selected and the cache is written for next time.
     *
     * @param dataSubsetW3
     * @param w1_To_c
     * @param wave
     * @param w2_To_w1
     * @param w3_To_w2
     * @param w4_To_w3
     * @return
     */
    public DataSubsetW4 loadSubsetWave4(
            DataSubsetW3 dataSubsetW3,
            HashMap<WaAS_W1ID, WaAS_CollectionID> w1_To_c, byte wave,
            TreeMap<WaAS_W2ID, WaAS_W1ID> w2_To_w1,
            TreeMap<WaAS_W3ID, WaAS_W2ID> w3_To_w2,
            TreeMap<WaAS_W4ID, WaAS_W3ID> w4_To_w3) {
        File cf = getFile(wave);
        if (cf.exists()) {
            return (DataSubsetW4) Generic_IO.readObject(cf);
        } else {
            TreeSet<WaAS_W4ID> w4IDs = new TreeSet<>();
            w4IDs.addAll(w4_To_w3.keySet());
            DataSubsetW4 r = new DataSubsetW4(w4IDs, dataSubsetW3.cSize, dataSubsetW3.nOC, wave);
            HashMap<WaAS_CollectionID, PrintWriter> cPWs = initPWs(r.cFs);
            /**
             * Read through the lines and figure out which lines should be put
             * in which collection.
             */
            File f = getInputFile(wave);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            /**
             * Read and write header.
             */
            addHeader(br, cPWs);
            /**
             * Read through the lines and write them to the appropriate files.
             */
            br.lines().skip(1).forEach(l -> {
                WaAS_W4PRecord rec = new WaAS_W4PRecord(l);
                //WaAS_W4ID w4ID = new WaAS_W4ID(rec.getCASEW4());
                WaAS_W4ID w4ID = env.data.CASEW4_To_w4.get(rec.getCASEW4());
                if (w4_To_w3.containsKey(w4ID)) {
                    WaAS_W3ID w3ID = w4_To_w3.get(w4ID);
                    WaAS_W2ID w2ID = w3_To_w2.get(w3ID);
                    WaAS_W1ID w1ID = w2_To_w1.get(w2ID);
                    write(w1_To_c, w1ID, cPWs, l);
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            // Close the PrintWriters.
            closePWs(cPWs);
            cache(wave, cf, r);
            return r;
        }
    }

    /**
     * Loads Wave 5 of the person WaAS for those records with CASEW5 in
     * CASEW5IDs.If this data are in a cache then the cache is loaded otherwise
     * the data are selected and the cache is written for next time.
     *
     * @param dataSubsetW4
     * @param w1_To_c
     * @param wave
     * @param w2_To_w1
     * @param w3_To_w2
     * @param w5_To_w4
     * @param w4_To_w3
     * @return
     */
    public DataSubsetW5 loadSubsetWave5(DataSubsetW4 dataSubsetW4,
            HashMap<WaAS_W1ID, WaAS_CollectionID> w1_To_c, byte wave,
            TreeMap<WaAS_W2ID, WaAS_W1ID> w2_To_w1,
            TreeMap<WaAS_W3ID, WaAS_W2ID> w3_To_w2,
            TreeMap<WaAS_W4ID, WaAS_W3ID> w4_To_w3,
            TreeMap<WaAS_W5ID, WaAS_W4ID> w5_To_w4) {
        File cf = getFile(wave);
        if (cf.exists()) {
            return (DataSubsetW5) Generic_IO.readObject(cf);
        } else {
            TreeSet<WaAS_W5ID> w5IDs = new TreeSet<>();
            w5IDs.addAll(w5_To_w4.keySet());
            DataSubsetW5 r = new DataSubsetW5(w5IDs, dataSubsetW4.cSize, dataSubsetW4.nOC, wave);
            HashMap<WaAS_CollectionID, PrintWriter> cPWs = initPWs(r.cFs);
            /**
             * Read through the lines and figure out which lines should be put
             * in which collection.
             */
            File f = getInputFile(wave);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            /**
             * Read and write header.
             */
            addHeader(br, cPWs);
            /**
             * Read through the lines and write them to the appropriate files.
             */
            br.lines().skip(1).forEach(l -> {
                WaAS_W5PRecord rec = new WaAS_W5PRecord(l);
                //WaAS_W5ID w5ID = new WaAS_W5ID(rec.getCASEW5());
                WaAS_W5ID w5ID = env.data.CASEW5_To_w5.get(rec.getCASEW5());
                if (w5_To_w4.containsKey(w5ID)) {
                    WaAS_W4ID w4ID = w5_To_w4.get(w5ID);
                    WaAS_W3ID w3ID = w4_To_w3.get(w4ID); // There is a strange case!
                    if (w3ID != null) {
                        WaAS_W2ID w2ID = w3_To_w2.get(w3ID);
                        WaAS_W1ID w1ID = w2_To_w1.get(w2ID);
                        write(w1_To_c, w1ID, cPWs, l);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            // Close the PrintWriters.
            closePWs(cPWs);
            cache(wave, cf, r);
            return r;
        }
    }

    /**
     * Loads subsets from a cache in generated data.
     *
     * @param nwaves
     * @param type
     * @return an Object[] r with size 5. r[] is a HashMap with keys that are
     * Integer CASEW1Each element is an Object[] ...
     */
    public Object[] loadSubsets(byte nwaves, String type) {
        Object[] r;
        r = new Object[nwaves];
        for (byte wave = 1; wave <= nwaves; wave++) {
            // Load Waves 1 to 5 inclusive.
            r[wave] = loadSubset(wave, type);
        }
        return r;
    }

    public Object[] loadSubset(byte wave, String type) {
        Object[] r;
        File f = getSubsetCacheFile(wave, type);
        if (f.exists()) {
            r = (Object[]) Generic_IO.readObject(f);
        } else {
            r = null;
        }
        return r;
    }
}
