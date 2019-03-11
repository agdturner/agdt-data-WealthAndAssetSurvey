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
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave1_PERSON_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave2_PERSON_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave3_PERSON_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave4_PERSON_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_Wave5_PERSON_Record;

/**
 *
 * @author geoagdt
 */
public class WaAS_PERSON_Handler extends WaAS_Handler {

    public WaAS_PERSON_Handler(WaAS_Environment e, File indir) {
        super(e, WaAS_Strings.s_person, indir);
    }

    /**
     * 
     * @param dir
     * @param wave
     * @return 
     */
    protected File getFile(File dir, byte wave) {
        return new File(dir, 
                TYPE + wave + WaAS_Strings.symbol_dot + WaAS_Strings.s_dat);
    }

    /**
     * Loads Wave 1 of the person WaAS for those records with CASEW1 in
     * CASEW1IDs.If this data are in a cache then the cache is loaded otherwise
     * the data are selected and the cache is written for next time.
     *
     * @param CASEW1IDs
     * @param nOC Number of collections.
     * @param wave
     * @param outdir
     * @return
     */
    public Object[] loadSubsetWave1(TreeSet<Short> CASEW1IDs,
            int nOC, byte wave, File outdir) {
        Object[] r;
        File cf = getFile(outdir, wave);
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
            TreeMap<Short, HashSet<Short>> CIDToCASEW1 = new TreeMap<>();
            r[0] = CIDToCASEW1;
            /**
             * Initialise lookup to be used to identify which collection a
             * person record is in. The key is CASEW1, the value is the
             * CollectionID.
             */
            HashMap<Short, Short> CASEW1ToCID = new HashMap<>();
            initialiseCASEW1ToCID(CASEW1ToCID, CASEW1IDs, cSize);
            r[1] = CASEW1ToCID;
            /**
             * Initialise collectionIDSets, collectionIDPrintWriters and
             * collectionIDFiles.
             */
            HashMap<Short, PrintWriter> cPWs = new HashMap<>();
            TreeMap<Short, File> cFs = new TreeMap<>();
            initialiseFilesAndPrintWriters(cFs, cPWs, nOC, wave, outdir);
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
                WaAS_Wave1_PERSON_Record rec = new WaAS_Wave1_PERSON_Record(l);
                short CASEW1 = rec.getCASEW1();
                if (CASEW1 > Short.MIN_VALUE) {
                    WaAS_ID ID = new WaAS_ID(CASEW1, CASEW1);
                    if (CASEW1ToCID.containsKey(CASEW1)) {
                        short cID = CASEW1ToCID.get(CASEW1);
                        PrintWriter pw = cPWs.get(cID);
                        pw.println(l);
                        Generic_Collections.addToMap(CIDToCASEW1, cID, CASEW1);
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
    protected void initialiseCASEW1ToCID(HashMap<Short, Short> CASEW1ToCID,
            TreeSet<Short> CASEW1IDs, int cSize) {
        Iterator<Short> ite = CASEW1IDs.iterator();
        short cID = 0;
        int i = 0;
        while (ite.hasNext()) {
            Short CASEW1 = ite.next();
            CASEW1ToCID.put(CASEW1, cID);
            i++;
            if (i == cSize) {
                i = 0;
                cID++;
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
     * @param outdir
     */
    protected void initialiseFilesAndPrintWriters(TreeMap<Short, File> cFs,
            HashMap<Short, PrintWriter> cPWs, int nOC, byte wave, File outdir) {
        for (short cID = 0; cID < nOC; cID++) {
            File f = new File(outdir, "person" + wave + "_" + cID + ".tab");
            cPWs.put(cID, Generic_IO.getPrintWriter(f, false));
            cFs.put(cID, f);
        }
    }

    /**
     * Reads the header from br and writes this out to the
     * collectionIDPrintWriters.
     *
     * @param br
     * @param CPWs
     */
    protected void addHeader(BufferedReader br, HashMap<Short, PrintWriter> CPWs) {
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
    protected void closePrintWriters(HashMap<Short, PrintWriter> cPWs) {
        cPWs.keySet().stream().forEach(collectionID -> {
            PrintWriter pw = cPWs.get(collectionID);
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
     * @param outdir
     * @param CASEW2ToCASEW1
     * @return
     */
    public TreeMap<Short, File> loadSubsetWave2(int nOC,
            HashMap<Short, Short> CASEW1ToCID, byte wave, File outdir,
            TreeMap<Short, Short> CASEW2ToCASEW1) {
        File cf = getFile(outdir, wave);
        if (cf.exists()) {
            return (TreeMap<Short, File>) Generic_IO.readObject(cf);
        } else {
            TreeMap<Short, File> r = new TreeMap<>();
            /**
             * Initialise collectionIDSets and collectionIDPrintWriters.
             */
            HashMap<Short, PrintWriter> cPWs = new HashMap<>();
            initialiseFilesAndPrintWriters(r, cPWs, nOC, wave, outdir);
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
                WaAS_Wave2_PERSON_Record rec = new WaAS_Wave2_PERSON_Record(l);
                short CASEW2 = rec.getCASEW2();
                if (CASEW2ToCASEW1.containsKey(CASEW2)) {
                    short CASEW1 = CASEW2ToCASEW1.get(CASEW2);
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
    private void write(HashMap<Short, Short> CASEW1ToCID, short CASEW1,
            HashMap<Short, PrintWriter> cPWs, String s) {
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
     * @param CASEW1ToCID
     * @param wave
     * @param outdir
     * @param CASEW2ToCASEW1
     * @param CASEW3ToCASEW2
     * @return
     */
    public TreeMap<Short, File> loadSubsetWave3(int nOC,
            HashMap<Short, Short> CASEW1ToCID, byte wave, File outdir,
            TreeMap<Short, Short> CASEW2ToCASEW1,
            TreeMap<Short, Short> CASEW3ToCASEW2) {
        File cf = getFile(outdir, wave);
        if (cf.exists()) {
            return (TreeMap<Short, File>) Generic_IO.readObject(cf);
        } else {
            TreeMap<Short, File> r = new TreeMap<>();
            /**
             * Initialise collectionIDSets and collectionIDPrintWriters.
             */
            HashMap<Short, PrintWriter> cPWs = new HashMap<>();
            initialiseFilesAndPrintWriters(r, cPWs, nOC, wave, outdir);
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
                WaAS_Wave3_PERSON_Record rec = new WaAS_Wave3_PERSON_Record(l);
                short CASEW3 = rec.getCASEW3();
                if (CASEW3ToCASEW2.containsKey(CASEW3)) {
                    short CASEW2 = CASEW3ToCASEW2.get(CASEW3);
                    short CASEW1 = CASEW2ToCASEW1.get(CASEW2);
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
     * Loads Wave 4 of the person WaAS for those records with CASEW4 in
     * CASEW4IDs.If this data are in a cache then the cache is loaded otherwise
     * the data are selected and the cache is written for next time.
     *
     * @param nOC Number of collections.
     * @param CASEW1ToCID
     * @param wave
     * @param outdir
     * @param CASEW2ToCASEW1
     * @param CASEW3ToCASEW2
     * @param CASEW4ToCASEW3
     * @return
     */
    public TreeMap<Short, File> loadSubsetWave4(int nOC,
            HashMap<Short, Short> CASEW1ToCID, byte wave, File outdir,
            TreeMap<Short, Short> CASEW2ToCASEW1,
            TreeMap<Short, Short> CASEW3ToCASEW2,
            TreeMap<Short, Short> CASEW4ToCASEW3) {
        File cf = getFile(outdir, wave);
        if (cf.exists()) {
            return (TreeMap<Short, File>) Generic_IO.readObject(cf);
        } else {
            TreeMap<Short, File> r = new TreeMap<>();
            /**
             * Initialise collectionIDSets and collectionIDPrintWriters.
             */
            HashMap<Short, PrintWriter> cPWs = new HashMap<>();
            initialiseFilesAndPrintWriters(r, cPWs, nOC, wave, outdir);
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
                WaAS_Wave4_PERSON_Record rec = new WaAS_Wave4_PERSON_Record(l);
                short CASEW4 = rec.getCASEW4();
                if (CASEW4ToCASEW3.containsKey(CASEW4)) {
                    short CASEW3 = CASEW4ToCASEW3.get(CASEW4);
                    short CASEW2 = CASEW3ToCASEW2.get(CASEW3);
                    short CASEW1 = CASEW2ToCASEW1.get(CASEW2);
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
     * Loads Wave 5 of the person WaAS for those records with CASEW5 in
     * CASEW5IDs.If this data are in a cache then the cache is loaded otherwise
     * the data are selected and the cache is written for next time.
     *
     * @param nOC Number of collections.
     * @param CASEW1ToCID
     * @param wave
     * @param outdir
     * @param CASEW2ToCASEW1
     * @param CASEW3ToCASEW2
     * @param CASEW5ToCASEW4
     * @param CASEW4ToCASEW3
     * @return
     */
    public TreeMap<Short, File> loadSubsetWave5(int nOC,
            HashMap<Short, Short> CASEW1ToCID, byte wave, File outdir,
            TreeMap<Short, Short> CASEW2ToCASEW1,
            TreeMap<Short, Short> CASEW3ToCASEW2,
            TreeMap<Short, Short> CASEW4ToCASEW3,
            TreeMap<Short, Short> CASEW5ToCASEW4) {
        File cf = getFile(outdir, wave);
        if (cf.exists()) {
            return (TreeMap<Short, File>) Generic_IO.readObject(cf);
        } else {
            TreeMap<Short, File> r = new TreeMap<>();
            /**
             * Initialise collectionIDSets, collectionIDPrintWriters and
             * collectionIDFiles.
             */
            HashMap<Short, PrintWriter> cPWs = new HashMap<>();
            initialiseFilesAndPrintWriters(r, cPWs, nOC, wave, outdir);
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
                WaAS_Wave5_PERSON_Record rec = new WaAS_Wave5_PERSON_Record(l);
                short CASEW5 = rec.getCASEW5();
                if (CASEW5ToCASEW4.containsKey(CASEW5)) {
                    short CASEW4 = CASEW5ToCASEW4.get(CASEW5);
                    short CASEW3 = CASEW4ToCASEW3.get(CASEW4);
                    short CASEW2 = CASEW3ToCASEW2.get(CASEW3);
                    short CASEW1 = CASEW2ToCASEW1.get(CASEW2);
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
     * Loads subsets from a cache in generated data.
     *
     * @param nwaves
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
        File f = getCachSubsetFile(wave, type);
        if (f.exists()) {
            r = (Object[]) Generic_IO.readObject(f);
        } else {
            r = null;
        }
        return r;
    }
}
