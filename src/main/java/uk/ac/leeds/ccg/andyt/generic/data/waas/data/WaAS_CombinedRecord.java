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

import java.util.HashMap;

/**
 *
 * @author geoagdt
 */
public class WaAS_CombinedRecord extends WaAS_ID {

    public WaAS_W1Record w1Record;

    /**
     * Keys are CASEW2
     */
    public HashMap<WaAS_W2ID, WaAS_W2Record> w2Records;

    /**
     * Keys are CASEW2, values keys are CASEW3.
     */
    public HashMap<WaAS_W2ID, HashMap<WaAS_W3ID, WaAS_W3Record>> w3Records;

    /**
     * Keys are CASEW2, values keys are CASEW3, next value keys are CASEW4.
     */
    public HashMap<WaAS_W2ID, HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, WaAS_W4Record>>> w4Records;

    /**
     * Keys are CASEW2, values keys are CASEW3, next value keys are CASEW4, next
     * value keys are CASEW5.
     */
    public HashMap<WaAS_W2ID, HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>>>> w5Records;

    public WaAS_CombinedRecord(WaAS_W1ID CASEW1) {
        super(CASEW1.getID());
        w1Record = new WaAS_W1Record(CASEW1.getID());
        w2Records = new HashMap<>();
        w3Records = new HashMap<>();
        w4Records = new HashMap<>();
        w5Records = new HashMap<>();
    }
}
