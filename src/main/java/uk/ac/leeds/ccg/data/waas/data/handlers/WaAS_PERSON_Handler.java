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
package uk.ac.leeds.ccg.data.waas.data.handlers;

import uk.ac.leeds.ccg.data.waas.data.subset.WaAS_DataSubsetW2;
import uk.ac.leeds.ccg.data.waas.data.subset.WaAS_DataSubsetW4;
import uk.ac.leeds.ccg.data.waas.data.subset.WaAS_DataSubsetW5;
import uk.ac.leeds.ccg.data.waas.data.subset.WaAS_DataSubsetW3;
import uk.ac.leeds.ccg.data.waas.data.subset.WaAS_DataSubsetW1;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_W4ID;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_W1ID;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_W5ID;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_W2ID;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_W3ID;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import uk.ac.leeds.ccg.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.data.waas.core.WaAS_Strings;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_CollectionID;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_RecordID;
import uk.ac.leeds.ccg.generic.util.Generic_Collections;
import uk.ac.leeds.ccg.data.waas.data.person.WaAS_W1PRecord;
import uk.ac.leeds.ccg.data.waas.data.person.WaAS_W2PRecord;
import uk.ac.leeds.ccg.data.waas.data.person.WaAS_W3PRecord;
import uk.ac.leeds.ccg.data.waas.data.person.WaAS_W4PRecord;
import uk.ac.leeds.ccg.data.waas.data.person.WaAS_W5PRecord;
import uk.ac.leeds.ccg.generic.io.Generic_IO;
import uk.ac.leeds.ccg.generic.io.Generic_Path;

/**
 *
 * @author geoagdt
 */
public class WaAS_PERSON_Handler extends WaAS_Handler {

    public WaAS_PERSON_Handler(WaAS_Environment e) {
        super(e);
    }

    @Override
    public String getType() {
        return WaAS_Strings.s_person;
    }

    /**
     * Loads Wave 1 of the person WaAS for those records with CASEW1 in
     * CASEW1IDs.If the data are in a cache then the cache is loaded otherwise
     * the data are selected and the cache is written for next time.
     *
     * @param w1IDs
     * @return
     * @throws java.io.IOException If encountered.
     * @throws java.lang.ClassNotFoundException If encountered.
     */
    public WaAS_DataSubsetW1 loadDataSubsetW1(Set<WaAS_W1ID> w1IDs) 
            throws IOException, ClassNotFoundException {
        byte w = W1;
        Path cf = getFile(w);
        if (Files.exists(cf)) {
            return (WaAS_DataSubsetW1) Generic_IO.readObject(cf);
        } else {
            // Calculate the number of things in a normal collection
            int cSize = getCSize(w1IDs, we.data.nOC);
            WaAS_DataSubsetW1 r = new WaAS_DataSubsetW1(we, cSize, w1IDs);
            Map<WaAS_CollectionID, PrintWriter> cPWs = initPWs(r.cFs);
            /**
             * Read through the lines and figure out which lines should be put
             * in which collection.
             */
            Path f = getInputFile(w);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            /**
             * Read and write header.
             */
            addHeader(br, cPWs);
            /**
             * Read through the lines and write them to the appropriate files.
             */
            br.readLine(); // skip header
            String line = br.readLine();
            int ln = 0;
            int i = 0;
            while (line != null) {
                try {
                    WaAS_W1PRecord rec = new WaAS_W1PRecord(new WaAS_RecordID(i), line);
                    short CASEW1 = rec.getCASEW1();
                    if (CASEW1 > Short.MIN_VALUE) {
                        //WaAS_W1ID w1ID = new WaAS_W1ID(CASEW1);
                        WaAS_W1ID w1ID = we.data.CASEW1_To_w1.get(CASEW1);
                        if (r.w1_To_c.containsKey(w1ID)) {
                            WaAS_CollectionID cID = r.w1_To_c.get(w1ID);
                            PrintWriter pw = cPWs.get(cID);
                            if (pw == null) {
                                env.log("cID " + cID + " wID " + w1ID);
                            }
                            pw.println(line);
                            Generic_Collections.addToMap(r.c_To_w1, cID, w1ID);
                        }
                    }
                    i++;
                    line = br.readLine();
                } catch (Exception ex) {
                we.logLineNotLoading(ex, ln);
                }
                ln++;
            }
            // Close br
            io.closeBufferedReader(br);
            // Close the PrintWriters.
            closePWs(cPWs);
            cache(w, cf, r);
            return r;
        }
    }

    /**
     * Initialise cIDs, cPWs and cFs.
     *
     * @param cFs
     * @return
     * @throws java.io.IOException If one is encountered.
     */
    protected Map<WaAS_CollectionID, PrintWriter> initPWs(
            Map<WaAS_CollectionID, Generic_Path> cFs) throws IOException {
        Map<WaAS_CollectionID, PrintWriter> r = new HashMap<>();
        Iterator<WaAS_CollectionID> ite = cFs.keySet().iterator();
        while (ite.hasNext()) {
            WaAS_CollectionID cID = ite.next();
            //env.log("Init PW with cID " + cID);
            Path f = cFs.get(cID).getPath();
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
            Map<WaAS_CollectionID, PrintWriter> cPWs) {
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
    protected void closePWs(Map<WaAS_CollectionID, PrintWriter> cPWs) {
        cPWs.keySet().stream().forEach(cID -> {
            cPWs.get(cID).close();
        });
    }

    /**
     * Loads Wave 2 of the person WaAS for those records with CASEW2 in
     * CASEW2IDs.If this data are in a cache then the cache is loaded otherwise
     * the data are selected and the cache is written for next time.
     *
     * @param sW1
     * @param w2_To_w1
     * @return
     * @throws java.io.IOException If encountered.
     * @throws java.lang.ClassNotFoundException If encountered.
     */
    public WaAS_DataSubsetW2 loadDataSubsetW2(WaAS_DataSubsetW1 sW1,
            Map<WaAS_W2ID, WaAS_W1ID> w2_To_w1) throws IOException, 
            ClassNotFoundException {
        byte w = W2;
        Path cf = getFile(w);
        if (Files.exists(cf)) {
            return (WaAS_DataSubsetW2) Generic_IO.readObject(cf);
        } else {
            Set<WaAS_W2ID> w2IDs = new TreeSet<>();
            w2IDs.addAll(w2_To_w1.keySet());
            WaAS_DataSubsetW2 r = new WaAS_DataSubsetW2(we, sW1.cSize, w2IDs);
            Map<WaAS_CollectionID, PrintWriter> cPWs = initPWs(r.cFs);
            /**
             * Read through the lines and figure out which lines should be put
             * in which collection.
             */
            Path f = getInputFile(w);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            /**
             * Read and write header.
             */
            addHeader(br, cPWs);
            /**
             * Read through the lines and write them to the appropriate files.
             */
            br.readLine(); // skip header
            String line = br.readLine();
            int ln = 0;
            int i = 0;
            while (line != null) {
                try {
                    WaAS_W2PRecord rec = new WaAS_W2PRecord(new WaAS_RecordID(i), line);
                    //WaAS_W2ID w2ID = new WaAS_W2ID(rec.getCASEW2());
                    WaAS_W2ID w2ID = we.data.CASEW2_To_w2.get(rec.getCASEW2());
                    if (w2_To_w1.containsKey(w2ID)) {
                        WaAS_W1ID w1ID = w2_To_w1.get(w2ID);
                        write(sW1.w1_To_c, w1ID, cPWs, line);
                    }
                    i++;
                    line = br.readLine();
                } catch (Exception ex) {
                we.logLineNotLoading(ex, ln);
                }
                ln++;
            }
            // Close br
            io.closeBufferedReader(br);
            // Close the PrintWriters.
            closePWs(cPWs);
            cache(w, cf, r);
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
    private void write(Map<WaAS_W1ID, WaAS_CollectionID> w1_To_c,
            WaAS_W1ID w1ID, Map<WaAS_CollectionID, PrintWriter> cPWs,
            String s) {
        if (w1_To_c.containsKey(w1ID)) {
            cPWs.get(w1_To_c.get(w1ID)).println(s);
        }
    }

    /**
     * Loads Wave 3 of the person WaAS for those records with CASEW3 in
     * CASEW3IDs.If this data are in a cache then the cache is loaded otherwise
     * the data are selected and the cache is written for next time.
     *
     * @param sW2
     * @param w1_To_c
     * @param w2_To_w1
     * @param w3_To_w2
     * @return
     * @throws java.io.IOException If encountered.
     * @throws java.lang.ClassNotFoundException If encountered.
     */
    public WaAS_DataSubsetW3 loadDataSubsetW3(WaAS_DataSubsetW2 sW2,
            Map<WaAS_W1ID, WaAS_CollectionID> w1_To_c,
            Map<WaAS_W2ID, WaAS_W1ID> w2_To_w1,
            Map<WaAS_W3ID, WaAS_W2ID> w3_To_w2) throws IOException,
            ClassNotFoundException {
        byte w = W3;
        Path cf = getFile(w);
        if (Files.exists(cf)) {
            return (WaAS_DataSubsetW3) Generic_IO.readObject(cf);
        } else {
            Set<WaAS_W3ID> w3IDs = new TreeSet<>();
            w3IDs.addAll(w3_To_w2.keySet());
            WaAS_DataSubsetW3 r = new WaAS_DataSubsetW3(we, sW2.cSize, w3IDs);
            Map<WaAS_CollectionID, PrintWriter> cPWs = initPWs(r.cFs);

            /**
             * Read through the lines and figure out which lines should be put
             * in which collection.
             */
            Path f = getInputFile(w);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            /**
             * Read and write header.
             */
            addHeader(br, cPWs);

            /**
             * Read through the lines and write them to the appropriate files.
             */
            br.readLine(); // skip header
            String line = br.readLine();
            int ln = 0;
            int i = 0;
            while (line != null) {
                try {
                    WaAS_W3PRecord rec = new WaAS_W3PRecord(new WaAS_RecordID(i), line);
                    //WaAS_W3ID w3ID = new WaAS_W3ID(rec.getCASEW3());
                    WaAS_W3ID w3ID = we.data.CASEW3_To_w3.get(rec.getCASEW3());
                    if (w3_To_w2.containsKey(w3ID)) {
                        WaAS_W2ID w2ID = w3_To_w2.get(w3ID);
                        WaAS_W1ID w1ID = w2_To_w1.get(w2ID);
                        write(w1_To_c, w1ID, cPWs, line);
                    }
                    i++;
                    line = br.readLine();
                } catch (Exception ex) {
                we.logLineNotLoading(ex, ln);
                }
                ln++;
            }
            // Close br
            io.closeBufferedReader(br);
            // Close the PrintWriters.
            closePWs(cPWs);
            cache(w, cf, r);
            return r;
        }
    }

    /**
     * Loads Wave 4 of the person WaAS for those records with CASEW4 in
     * CASEW4IDs.If this data are in a cache then the cache is loaded otherwise
     * the data are selected and the cache is written for next time.
     *
     * @param sW3
     * @param w1_To_c
     * @param w2_To_w1
     * @param w3_To_w2
     * @param w4_To_w3
     * @return
     * @throws java.io.IOException If encountered.
     * @throws java.lang.ClassNotFoundException If encountered.
     */
    public WaAS_DataSubsetW4 loadDataSubsetW4(WaAS_DataSubsetW3 sW3,
            Map<WaAS_W1ID, WaAS_CollectionID> w1_To_c,
            Map<WaAS_W2ID, WaAS_W1ID> w2_To_w1,
            Map<WaAS_W3ID, WaAS_W2ID> w3_To_w2,
            Map<WaAS_W4ID, WaAS_W3ID> w4_To_w3) throws IOException, 
            ClassNotFoundException {
        byte w = W4;
        Path cf = getFile(w);
        if (Files.exists(cf)) {
            return (WaAS_DataSubsetW4) Generic_IO.readObject(cf);
        } else {
            Set<WaAS_W4ID> w4IDs = new TreeSet<>();
            w4IDs.addAll(w4_To_w3.keySet());
            WaAS_DataSubsetW4 r = new WaAS_DataSubsetW4(we, sW3.cSize, w4IDs);
            Map<WaAS_CollectionID, PrintWriter> cPWs = initPWs(r.cFs);
            /**
             * Read through the lines and figure out which lines should be put
             * in which collection.
             */
            Path f = getInputFile(w);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            /**
             * Read and write header.
             */
            addHeader(br, cPWs);
            /**
             * Read through the lines and write them to the appropriate files.
             */
            br.readLine(); // skip header
            String line = br.readLine();
            int ln = 0;
            int i = 0;
            while (line != null) {
                try {
                    WaAS_W4PRecord rec = new WaAS_W4PRecord(new WaAS_RecordID(i), line);
                    //WaAS_W4ID w4ID = new WaAS_W4ID(rec.getCASEW4());
                    WaAS_W4ID w4ID = we.data.CASEW4_To_w4.get(rec.getCASEW4());
                    if (w4ID != null) {
                        if (w4_To_w3.containsKey(w4ID)) {
                            WaAS_W3ID w3ID = w4_To_w3.get(w4ID);
                            WaAS_W2ID w2ID = w3_To_w2.get(w3ID);
                            WaAS_W1ID w1ID = w2_To_w1.get(w2ID);
                            write(w1_To_c, w1ID, cPWs, line);
                        }
                    }
                    i++;
                    line = br.readLine();
                } catch (Exception ex) {
                we.logLineNotLoading(ex, ln);
                }
                ln++;
            }
            // Close br
            io.closeBufferedReader(br);
            // Close the PrintWriters.
            closePWs(cPWs);
            cache(w, cf, r);
            return r;
        }
    }

    /**
     * Loads Wave 5 of the person WaAS for those records with CASEW5 in
     * CASEW5IDs. If this data are in a cache then the cache is loaded otherwise
     * the data are selected and the cache is written for next time.
     *
     * @param sW4
     * @param w1_To_c
     * @param w2_To_w1
     * @param w3_To_w2
     * @param w5_To_w4
     * @param w4_To_w3
     * @return
     * @throws java.io.IOException If encountered.
     * @throws java.lang.ClassNotFoundException If encountered.
     */
    public WaAS_DataSubsetW5 loadDataSubsetW5(WaAS_DataSubsetW4 sW4,
            Map<WaAS_W1ID, WaAS_CollectionID> w1_To_c,
            Map<WaAS_W2ID, WaAS_W1ID> w2_To_w1,
            Map<WaAS_W3ID, WaAS_W2ID> w3_To_w2,
            Map<WaAS_W4ID, WaAS_W3ID> w4_To_w3,
            Map<WaAS_W5ID, WaAS_W4ID> w5_To_w4) throws IOException, 
            ClassNotFoundException {
        byte w = W5;
        Path cf = getFile(w);
        if (Files.exists(cf)) {
            return (WaAS_DataSubsetW5) Generic_IO.readObject(cf);
        } else {
            Set<WaAS_W5ID> w5IDs = new TreeSet<>();
            w5IDs.addAll(w5_To_w4.keySet());
            WaAS_DataSubsetW5 r = new WaAS_DataSubsetW5(we, sW4.cSize, w5IDs);
            Map<WaAS_CollectionID, PrintWriter> cPWs = initPWs(r.cFs);
            /**
             * Read through the lines and figure out which lines should be put
             * in which collection.
             */
            Path f = getInputFile(w);
            BufferedReader br = Generic_IO.getBufferedReader(f);
            /**
             * Read and write header.
             */
            addHeader(br, cPWs);
            /**
             * Read through the lines and write them to the appropriate files.
             */
            br.readLine(); // skip header
            String line = br.readLine();
            int ln = 0;
            int i = 0;
            while (line != null) {
                try {
                    WaAS_W5PRecord rec = new WaAS_W5PRecord(new WaAS_RecordID(i), line);
                    //WaAS_W5ID w5ID = new WaAS_W5ID(rec.getCASEW5());
                    WaAS_W5ID w5ID = we.data.CASEW5_To_w5.get(rec.getCASEW5());
                    if (w5_To_w4.containsKey(w5ID)) {
                        WaAS_W4ID w4ID = w5_To_w4.get(w5ID);
                        WaAS_W3ID w3ID = w4_To_w3.get(w4ID); // There is a strange case!
                        if (w3ID != null) {
                            WaAS_W2ID w2ID = w3_To_w2.get(w3ID);
                            WaAS_W1ID w1ID = w2_To_w1.get(w2ID);
                            write(w1_To_c, w1ID, cPWs, line);
                        }
                    }
                    i++;
                    line = br.readLine();
                } catch (Exception ex) {
                we.logLineNotLoading(ex, ln);
                }
                ln++;
            }
            // Close br
            io.closeBufferedReader(br);
            // Close the PrintWriters.
            closePWs(cPWs);
            cache(w, cf, r);
            return r;
        }
    }

    /**
     * Loads subsets from a cache in generated data.
     *
     * @param nwaves
     * @param type
     * @return an Object[] r with size 5. r[] is a Map with keys that are
     * Integer CASEW1Each element is an Object[] ...
     * @throws java.io.IOException If encountered.
     * @throws java.lang.ClassNotFoundException If encountered.
     */
    public Object[] loadDataSubsets(byte nwaves, String type) 
            throws IOException, ClassNotFoundException {
        Object[] r;
        r = new Object[nwaves];
        for (byte wave = 1; wave <= nwaves; wave++) {
            // Load Waves 1 to 5 inclusive.
            r[wave] = loadDataSubset(wave, type);
        }
        return r;
    }

    /**
     * 
     * @param wave
     * @param type
     * @return
     * @throws java.io.IOException If encountered.
     * @throws java.lang.ClassNotFoundException If encountered.
     */
    public Object[] loadDataSubset(byte wave, String type) throws IOException, 
            ClassNotFoundException {
        Object[] r;
        Path f = getSubsetCacheFile(wave, type);
        if (Files.exists(f)) {
            r = (Object[]) Generic_IO.readObject(f);
        } else {
            r = null;
        }
        return r;
    }
}
