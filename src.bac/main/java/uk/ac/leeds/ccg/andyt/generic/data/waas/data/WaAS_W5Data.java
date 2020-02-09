/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.leeds.ccg.andyt.generic.data.waas.data;

import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W5Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W4ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W1ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W5ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W2ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W3ID;
import java.io.Serializable;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 *
 * @author geoagdt
 */
public class WaAS_W5Data implements Serializable {

        /**
         * Keys are CASEW5 IDs and values are WaAS_W5Records
         */
        public final TreeMap<WaAS_W5ID, WaAS_W5Record> lookup;
        /**
         * CASEW1 in Wave 5 records.
         */
        public final TreeSet<WaAS_W1ID> w1_In_w5;
        /**
         * CASEW2 in Wave 5 records.
         */
        public final TreeSet<WaAS_W2ID> w2_In_w5;
        /**
         * CASEW3 in Wave 5 records.
         */
        public final TreeSet<WaAS_W3ID> w3_In_w5;
        /**
         * CASEW4 in Wave 5 records.
         */
        public final TreeSet<WaAS_W4ID> w4_In_w5;
        /**
         * All CASEW5 IDs.
         */
        public final TreeSet<WaAS_W5ID> all;
        /**
         * CASEW5 values for records that have CASEW4, w3ID, CASEW2 and CASEW1
         * values.
         */
        public final TreeSet<WaAS_W5ID> w5_In_w1w2w3w4;
        /**
         * Keys are CASEW5 IDs and values are CASEW4 IDs.
         */
        public final TreeMap<WaAS_W5ID, WaAS_W4ID> w5_To_w4;
        /**
         * Keys are CASEW4 IDs and values are sets of CASEW5 IDs (normally size one).
         */
        public final TreeMap<WaAS_W4ID, HashSet<WaAS_W5ID>> w4_To_w5;

        public WaAS_W5Data() {
            lookup = new TreeMap<>();
            w1_In_w5 = new TreeSet<>();
            w2_In_w5 = new TreeSet<>();
            w3_In_w5 = new TreeSet<>();
            w4_In_w5 = new TreeSet<>();
            all = new TreeSet<>();
            w5_In_w1w2w3w4 = new TreeSet<>();
            w5_To_w4 = new TreeMap<>();
            w4_To_w5 = new TreeMap<>();
        }
    }