/*
 * Copyright 2019 Centre for Computational Geography, University of Leeds.
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
package uk.ac.leeds.ccg.data.waas.data;

import java.io.Serializable;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_W4ID;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_W1ID;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_W5ID;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_W2ID;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_W3ID;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * WaAS_GORSubsetsAndLookups
 * 
 * @author Andy Turner
 * @version 1.0.0
 */
public class WaAS_GORSubsetsAndLookups implements Serializable {

    public Map<Byte, Set<WaAS_W1ID>> gor_To_w1;
    public Map<Byte, Set<WaAS_W2ID>> gor_To_w2;
    public Map<Byte, Set<WaAS_W3ID>> gor_To_w3;
    public Map<Byte, Set<WaAS_W4ID>> gor_To_w4;
    public Map<Byte, Set<WaAS_W5ID>> gor_to_w5;
    public Map<WaAS_W1ID, Byte> w1_To_gor;
    public Map<WaAS_W2ID, Byte> w2_To_gor;
    public Map<WaAS_W3ID, Byte> w3_To_gor;
    public Map<WaAS_W4ID, Byte> w4_To_gor;
    public Map<WaAS_W5ID, Byte> w5_To_gor;

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
