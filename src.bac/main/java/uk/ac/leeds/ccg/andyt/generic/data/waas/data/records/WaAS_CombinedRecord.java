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
package uk.ac.leeds.ccg.andyt.generic.data.waas.data.records;

import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W4ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W5ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W2ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W3ID;
import java.util.HashMap;
import uk.ac.leeds.ccg.andyt.data.Data_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W1ID;

/**
 *
 * @author geoagdt
 */
public class WaAS_CombinedRecord extends Data_Record {

    public final WaAS_W1Record w1Rec;

    /**
     * Keys are CASEW2
     */
    public final HashMap<WaAS_W2ID, WaAS_W2Record> w2Recs;

    /**
     * Keys are CASEW2, values keys are CASEW3.
     */
    public final HashMap<WaAS_W2ID, HashMap<WaAS_W3ID, WaAS_W3Record>> w3Recs;

    /**
     * Keys are CASEW2, values keys are CASEW3, next value keys are CASEW4.
     */
    public final HashMap<WaAS_W2ID, HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, WaAS_W4Record>>> w4Recs;

    /**
     * Keys are CASEW2, values keys are CASEW3, next value keys are CASEW4, next
     * value keys are CASEW5.
     */
    public final HashMap<WaAS_W2ID, HashMap<WaAS_W3ID, HashMap<WaAS_W4ID, HashMap<WaAS_W5ID, WaAS_W5Record>>>> w5Recs;

    public WaAS_CombinedRecord(WaAS_W1Record w1Rec) {
        super(w1Rec.ID);
        this.w1Rec = w1Rec;
        w2Recs = new HashMap<>();
        w3Recs = new HashMap<>();
        w4Recs = new HashMap<>();
        w5Recs = new HashMap<>();
    }

    @Override
    public WaAS_W1ID getID() {
        return (WaAS_W1ID) ID;
    }
}
