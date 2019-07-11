/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.leeds.ccg.andyt.generic.data.waas.data;

import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W4ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W1ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W5ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W2ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W3ID;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

/**
 *
 * @author geoagdt
 */
public class WaAS_GORSubsetsAndLookups {

    public HashMap<Byte, HashSet<WaAS_W1ID>> gor_To_w1;
    public HashMap<Byte, HashSet<WaAS_W2ID>> gor_To_w2;
    public HashMap<Byte, HashSet<WaAS_W3ID>> gor_To_w3;
    public HashMap<Byte, HashSet<WaAS_W4ID>> gor_To_w4;
    public HashMap<Byte, HashSet<WaAS_W5ID>> gor_to_w5;
    public HashMap<WaAS_W1ID, Byte> w1_To_gor;
    public HashMap<WaAS_W2ID, Byte> w2_To_gor;
    public HashMap<WaAS_W3ID, Byte> w3_To_gor;
    public HashMap<WaAS_W4ID, Byte> w4_To_gor;
    public HashMap<WaAS_W5ID, Byte> w5_To_gor;

    public WaAS_GORSubsetsAndLookups(ArrayList<Byte> gors) {
        gor_To_w1 = new HashMap<>();
        gor_To_w2 = new HashMap<>();
        gor_To_w3 = new HashMap<>();
        gor_To_w4 = new HashMap<>();
        gor_to_w5 = new HashMap<>();
        Iterator<Byte> ite = gors.iterator();
        while (ite.hasNext()) {
            byte gor = ite.next();
            gor_To_w1.put(gor, new HashSet<>());
            gor_To_w2.put(gor, new HashSet<>());
            gor_To_w3.put(gor, new HashSet<>());
            gor_To_w4.put(gor, new HashSet<>());
            gor_to_w5.put(gor, new HashSet<>());
        }
        w1_To_gor = new HashMap<>();
        w2_To_gor = new HashMap<>();
        w3_To_gor = new HashMap<>();
        w4_To_gor = new HashMap<>();
        w5_To_gor = new HashMap<>();
    }
}
