/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.leeds.ccg.andyt.generic.data.waas.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

/**
 *
 * @author geoagdt
 */
public class WaAS_GORSubsetsAndLookups {

    public HashMap<Byte, HashSet<WaAS_W1ID>> GOR2W1IDSet;
    public HashMap<Byte, HashSet<WaAS_W2ID>> GOR2W2IDSet;
    public HashMap<Byte, HashSet<WaAS_W3ID>> GOR2W3IDSet;
    public HashMap<Byte, HashSet<WaAS_W4ID>> GOR2W4IDSet;
    public HashMap<Byte, HashSet<WaAS_W5ID>> GOR2W5IDSet;
    public HashMap<WaAS_W1ID, Byte> W1ID2GOR;
    public HashMap<WaAS_W2ID, Byte> W2ID2GOR;
    public HashMap<WaAS_W3ID, Byte> W3ID2GOR;
    public HashMap<WaAS_W4ID, Byte> W4ID2GOR;
    public HashMap<WaAS_W5ID, Byte> W5ID2GOR;

    public WaAS_GORSubsetsAndLookups(ArrayList<Byte> gors) {
        GOR2W1IDSet = new HashMap<>();
        GOR2W2IDSet = new HashMap<>();
        GOR2W3IDSet = new HashMap<>();
        GOR2W4IDSet = new HashMap<>();
        GOR2W5IDSet = new HashMap<>();
        Iterator<Byte> ite = gors.iterator();
        while (ite.hasNext()) {
            byte gor = ite.next();
            GOR2W1IDSet.put(gor, new HashSet<>());
            GOR2W2IDSet.put(gor, new HashSet<>());
            GOR2W3IDSet.put(gor, new HashSet<>());
            GOR2W4IDSet.put(gor, new HashSet<>());
            GOR2W5IDSet.put(gor, new HashSet<>());
        }
        W1ID2GOR = new HashMap<>();
        W2ID2GOR = new HashMap<>();
        W3ID2GOR = new HashMap<>();
        W4ID2GOR = new HashMap<>();
        W5ID2GOR = new HashMap<>();
    }
}
