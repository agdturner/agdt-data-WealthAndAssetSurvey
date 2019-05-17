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
        super(e, WaAS_Strings.s_person);
    }

    /**
     *
     * @param wave
     * @return
     */
    protected File getFile(byte wave) {
        return new File(files.getGeneratedWaASDir(),
                TYPE + WaAS_Strings.s_W + wave + WaAS_Files.DOT_DAT);
    }

    /**
     * Loads Wave 1 of the person WaAS for those records with CASEW1 in
     * CASEW1IDs. If the data are in a cache then the cache is loaded otherwise
     * the data are selected and the cache is written for next time.
     *
     * @param CASEW1IDs
     * @param nOC Number of collections.
     * @param wave
     * @return
     */
    public Object[] loadSubsetWave1(TreeSet<WaAS_W1ID> CASEW1IDs, int nOC,
            byte wave) {
        Object[] r;
        File cf = getFile(wave);
        if (cf.exists()) {
            r = (Object[]) Generic_IO.readObject(cf);
        } else {
            r = new Object[3];
            // Calculate how many subsets
            int cSize = getCSize(CASEW1IDs, nOC);
            /**
             * Initialise lookup to be used to identify which CASEW1 records are
             * in each collection.
             */
            TreeMap<WaAS_CollectionID, HashSet<WaAS_W1ID>> CIDToCASEW1 = new TreeMap<>();
            r[0] = CIDToCASEW1;
            /**
             * Initialise lookup to be used to identify which collection a
             * person record is in. The key is CASEW1, the value is the
             * CollectionID.
             */
            HashMap<WaAS_W1ID, WaAS_CollectionID> CASEW1ToCID = new HashMap<>();
            initialiseCASEW1ToCID(CASEW1ToCID, CASEW1IDs, cSize);
            r[1] = CASEW1ToCID;
            /**
             * Initialise collectionIDSets, collectionIDPrintWriters and
             * collectionIDFiles.
             */
            HashMap<WaAS_CollectionID, PrintWriter> cPWs = new HashMap<>();
            TreeMap<WaAS_CollectionID, File> cFs = new TreeMap<>();
            initialiseFilesAndPrintWriters(cFs, cPWs, nOC, wave);
            r[2] = cFs;
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
                    WaAS_W1ID w1ID = new WaAS_W1ID(CASEW1);
                    if (CASEW1ToCID.containsKey(w1ID)) {
                        WaAS_CollectionID cID = CASEW1ToCID.get(w1ID);
                        PrintWriter pw = cPWs.get(cID);
                        pw.println(l);
                        Generic_Collections.addToMap(CIDToCASEW1, cID, w1ID);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            // Close the PrintWriters.
            closePrintWriters(cPWs);
            cache(wave, cf, r);
        }
        return r;
    }

    /**
     * Initialise CASEW1ToCID lookup.
     *
     * @param CASEW1ToCID
     * @param CASEW1IDs
     * @param cSize
     */
    protected void initialiseCASEW1ToCID(HashMap<WaAS_W1ID, WaAS_CollectionID> CASEW1ToCID,
            TreeSet<WaAS_W1ID> CASEW1IDs, int cSize) {
        Iterator<WaAS_W1ID> ite = CASEW1IDs.iterator();
        short s = 0;
        WaAS_CollectionID cID = new WaAS_CollectionID(s);
        int i = 0;
        while (ite.hasNext()) {
            WaAS_W1ID CASEW1 = ite.next();
            CASEW1ToCID.put(CASEW1, cID);
            i++;
            if (i == cSize) {
                i = 0;
                s ++;
                cID = new WaAS_CollectionID(s);
            }
        }
    }

    /**
     * Initialise cIDs, cPWs and cFs.
     *
     * @param cFs
     * @param cPWs
     * @param nOC Number of collections.
     * @param wave
     */
    protected void initialiseFilesAndPrintWriters(TreeMap<WaAS_CollectionID, File> cFs,
            HashMap<WaAS_CollectionID, PrintWriter> cPWs, int nOC, byte wave) {
        for (short cID = 0; cID < nOC; cID++) {
            File f = new File(files.getGeneratedWaASSubsetsDir(),
                    WaAS_Strings.s_person + wave
                    + WaAS_Strings.symbol_underscore + cID + ".tab");
            WaAS_CollectionID collID = new WaAS_CollectionID(cID);
            cPWs.put(collID, Generic_IO.getPrintWriter(f, false));
            cFs.put(collID, f);
        }
    }

    /**
     * Reads the header from br and writes this out to the
     * collectionIDPrintWriters.
     *
     * @param br
     * @param CPWs
     */
    protected void addHeader(BufferedReader br, HashMap<WaAS_CollectionID, PrintWriter> CPWs) {
        try {
            String header = br.readLine();
            CPWs.keySet().stream().forEach(cID -> {
                PrintWriter pw = CPWs.get(cID);
                pw.println(header);
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
    protected void closePrintWriters(HashMap<WaAS_CollectionID, PrintWriter> cPWs) {
        cPWs.keySet().stream().forEach(cID -> {
            PrintWriter pw = cPWs.get(cID);
            pw.close();
        });
    }

    /**
     * Loads Wave 2 of the person WaAS for those records with CASEW2 in
     * CASEW2IDs.If this data are in a cache then the cache is loaded otherwise
     * the data are selected and the cache is written for next time.
     *
     * @param nOC Number of collections.
     * @param CASEW1ToCID
     * @param wave
     * @param W2ToW1
     * @return
     */
    public TreeMap<WaAS_CollectionID, File> loadSubsetWave2(int nOC,
            HashMap<WaAS_W1ID, WaAS_CollectionID> CASEW1ToCID, byte wave,
            TreeMap<WaAS_W2ID, WaAS_W1ID> W2ToW1) {
        File cf = getFile(wave);
        if (cf.exists()) {
            return (TreeMap<WaAS_CollectionID, File>) Generic_IO.readObject(cf);
        } else {
            TreeMap<WaAS_CollectionID, File> r = new TreeMap<>();
            /**
             * Initialise collectionIDSets and collectionIDPrintWriters.
             */
            HashMap<WaAS_CollectionID, PrintWriter> cPWs = new HashMap<>();
            initialiseFilesAndPrintWriters(r, cPWs, nOC, wave);
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
                WaAS_W2ID w2ID = new WaAS_W2ID(rec.getCASEW2());
                if (W2ToW1.containsKey(w2ID)) {
                    WaAS_W1ID CASEW1 = W2ToW1.get(w2ID);
                    write(CASEW1ToCID, CASEW1, cPWs, l);
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            // Close the PrintWriters.
            closePrintWriters(cPWs);
            cache(wave, cf, r);
            return r;
        }
    }

    /**
     *
     * @param CASEW1ToCID the map for looking up collection ID from CASEW1.
     * @param CASEW1 the key for the collection ID.
     * @param cPWs the collection PrintWriters indexed by the collection ID
     * looked up from CASEW1.
     * @param s the string to write
     */
    private void write(HashMap<WaAS_W1ID, WaAS_CollectionID> CASEW1ToCID, WaAS_W1ID CASEW1,
            HashMap<WaAS_CollectionID, PrintWriter> cPWs, String s) {
        if (CASEW1ToCID.containsKey(CASEW1)) {
            PrintWriter pw = cPWs.get(CASEW1ToCID.get(CASEW1));
            pw.println(s);
        }
    }

    /**
     *
     * @param c
     * @param numberOfCollections
     * @return (int) Math.ceil(c.size()/ (double) numberOfCollections)
     */
    protected int getCSize(Collection c, int numberOfCollections) {
        int n = c.size();
        return (int) Math.ceil((double) n / (double) numberOfCollections);
    }

    /**
     * Loads Wave 3 of the person WaAS for those records with CASEW3 in
     * CASEW3IDs.If this data are in a cache then the cache is loaded otherwise
     * the data are selected and the cache is written for next time.
     *
     * @param nOC Number of collections.
     * @param W1ToCID
     * @param wave
     * @param W2ToW1
     * @param W3ToW2
     * @return
     */
    public TreeMap<WaAS_CollectionID, File> loadSubsetWave3(int nOC,
            HashMap<WaAS_W1ID, WaAS_CollectionID> W1ToCID, byte wave,
            TreeMap<WaAS_W2ID, WaAS_W1ID> W2ToW1,
            TreeMap<WaAS_W3ID, WaAS_W2ID> W3ToW2) {
        File cf = getFile(wave);
        if (cf.exists()) {
            return (TreeMap<WaAS_CollectionID, File>) Generic_IO.readObject(cf);
        } else {
            TreeMap<WaAS_CollectionID, File> r = new TreeMap<>();
            /**
             * Initialise collectionIDSets and collectionIDPrintWriters.
             */
            HashMap<WaAS_CollectionID, PrintWriter> cPWs = new HashMap<>();
            initialiseFilesAndPrintWriters(r, cPWs, nOC, wave);
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
                WaAS_W3ID CASEW3 = new WaAS_W3ID(rec.getCASEW3());
                if (W3ToW2.containsKey(CASEW3)) {
                    WaAS_W2ID CASEW2 = W3ToW2.get(CASEW3);
                    WaAS_W1ID CASEW1 = W2ToW1.get(CASEW2);
                    write(W1ToCID, CASEW1, cPWs, l);
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            // Close the PrintWriters.
            closePrintWriters(cPWs);
            cache(wave, cf, r);
            return r;
        }
    }

    /**
     * Loads Wave 4 of the person WaAS for those records with CASEW4 in
     * CASEW4IDs.If this data are in a cache then the cache is loaded otherwise
     * the data are selected and the cache is written for next time.
     *
     * @param nOC Number of collections.
     * @param W1ToCID
     * @param wave
     * @param W2ToW1
     * @param W3ToW2
     * @param W4ToW3
     * @return
     */
    public TreeMap<WaAS_CollectionID, File> loadSubsetWave4(int nOC,
            HashMap<WaAS_W1ID, WaAS_CollectionID> W1ToCID, byte wave,
            TreeMap<WaAS_W2ID, WaAS_W1ID> W2ToW1,
            TreeMap<WaAS_W3ID, WaAS_W2ID> W3ToW2,
            TreeMap<WaAS_W4ID, WaAS_W3ID> W4ToW3) {
        File cf = getFile(wave);
        if (cf.exists()) {
            return (TreeMap<WaAS_CollectionID, File>) Generic_IO.readObject(cf);
        } else {
            TreeMap<WaAS_CollectionID, File> r = new TreeMap<>();
            /**
             * Initialise collectionIDSets and collectionIDPrintWriters.
             */
            HashMap<WaAS_CollectionID, PrintWriter> cPWs = new HashMap<>();
            initialiseFilesAndPrintWriters(r, cPWs, nOC, wave);
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
                WaAS_W4ID CASEW4 = new WaAS_W4ID(rec.getCASEW4());
                if (W4ToW3.containsKey(CASEW4)) {
                    WaAS_W3ID w3ID = W4ToW3.get(CASEW4);
                    WaAS_W2ID w2ID = W3ToW2.get(w3ID);
                    WaAS_W1ID w1ID = W2ToW1.get(w2ID);
                    write(W1ToCID, w1ID, cPWs, l);
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            // Close the PrintWriters.
            closePrintWriters(cPWs);
            cache(wave, cf, r);
            return r;
        }
    }

    /**
     * Loads Wave 5 of the person WaAS for those records with CASEW5 in
     * CASEW5IDs.If this data are in a cache then the cache is loaded otherwise
     * the data are selected and the cache is written for next time.
     *
     * @param nOC Number of collections.
     * @param W1ToCID
     * @param wave
     * @param W2ToW1
     * @param W3ToW2
     * @param W5ToW4
     * @param W4ToW3
     * @return
     */
    public TreeMap<WaAS_CollectionID, File> loadSubsetWave5(int nOC,
            HashMap<WaAS_W1ID, WaAS_CollectionID> W1ToCID, byte wave,
            TreeMap<WaAS_W2ID, WaAS_W1ID> W2ToW1,
            TreeMap<WaAS_W3ID, WaAS_W2ID> W3ToW2,
            TreeMap<WaAS_W4ID, WaAS_W3ID> W4ToW3,
            TreeMap<WaAS_W5ID, WaAS_W4ID> W5ToW4) {
        File cf = getFile(wave);
        if (cf.exists()) {
            return (TreeMap<WaAS_CollectionID, File>) Generic_IO.readObject(cf);
        } else {
            TreeMap<WaAS_CollectionID, File> r = new TreeMap<>();
            /**
             * Initialise collectionIDSets, collectionIDPrintWriters and
             * collectionIDFiles.
             */
            HashMap<WaAS_CollectionID, PrintWriter> cPWs = new HashMap<>();
            initialiseFilesAndPrintWriters(r, cPWs, nOC, wave);
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
                WaAS_W5ID CASEW5 = new WaAS_W5ID(rec.getCASEW5());
                if (W5ToW4.containsKey(CASEW5)) {
                    WaAS_W4ID w4ID = W5ToW4.get(CASEW5);
                    WaAS_W3ID w3ID = W4ToW3.get(w4ID); // There is a strange case!
                    if (w3ID != null) {
                        WaAS_W2ID w2ID = W3ToW2.get(w3ID);
                        WaAS_W1ID w1ID = W2ToW1.get(w2ID);
                        write(W1ToCID, w1ID, cPWs, l);
                    }
                }
            });
            // Close br
            Generic_IO.closeBufferedReader(br);
            // Close the PrintWriters.
            closePrintWriters(cPWs);
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
