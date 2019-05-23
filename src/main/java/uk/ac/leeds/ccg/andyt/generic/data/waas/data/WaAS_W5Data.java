/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.leeds.ccg.andyt.generic.data.waas.data;

import java.io.Serializable;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W5HRecord;

/**
 *
 * @author geoagdt
 */
public class WaAS_W5Data implements Serializable {

        /**
         * Keys are CASEW5 and values are WaAS_Wave5_HHOLD_Records
         */
        public TreeMap<WaAS_W5ID, WaAS_W5HRecord> lookup;
        /**
         * CASEW1 values in Wave 5 records.
         */
        public final TreeSet<WaAS_W1ID> w1IDsInW5;
        /**
         * CASEW2 values in Wave 5 records.
         */
        public final TreeSet<WaAS_W2ID> w2IDsInW5;
        /**
         * w3ID values in Wave 5 records.
         */
        public final TreeSet<WaAS_W3ID> W3InW5;
        /**
         * CASEW4 values in Wave 5 records.
         */
        public final TreeSet<WaAS_W4ID> W4InW5;
        /**
         * All CASEW5 values.
         */
        public final TreeSet<WaAS_W5ID> AllW5;
        /**
         * CASEW5 values for records that have CASEW4, w3ID, CASEW2 and CASEW1
         * values.
         */
        public final TreeSet<WaAS_W5ID> W5InW1W2W3W4;
        /**
         * Keys are CASEW5 and values are CASEW4.
         */
        public final TreeMap<WaAS_W5ID, WaAS_W4ID> W5ToW4;
        /**
         * Keys are CASEW4 and values are sets of CASEW5 (which is normally
         * expected to contain just one CASEW5).
         */
        public final TreeMap<WaAS_W4ID, HashSet<WaAS_W5ID>> W4ToW5;

        public WaAS_W5Data() {
            lookup = new TreeMap<>();
            w1IDsInW5 = new TreeSet<>();
            w2IDsInW5 = new TreeSet<>();
            W3InW5 = new TreeSet<>();
            W4InW5 = new TreeSet<>();
            AllW5 = new TreeSet<>();
            W5InW1W2W3W4 = new TreeSet<>();
            W5ToW4 = new TreeMap<>();
            W4ToW5 = new TreeMap<>();
        }
    }