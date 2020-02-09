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
package uk.ac.leeds.ccg.data.waas.data.records;

import uk.ac.leeds.ccg.data.waas.data.id.WaAS_W1ID;
import java.util.ArrayList;
import uk.ac.leeds.ccg.data.Data_Record;
import uk.ac.leeds.ccg.data.waas.data.hhold.WaAS_W1HRecord;
import uk.ac.leeds.ccg.data.waas.data.person.WaAS_W1PRecord;

/**
 * WaAS_W1Record
 * 
 * @author Andy Turner
 * @version 1.0.0
 */
public class WaAS_W1Record extends Data_Record {

    private final WaAS_W1HRecord hr;

    private final ArrayList<WaAS_W1PRecord> prs;

    /**
     * Defaults hr to null and prs to a new ArrayList<>().
     *
     * @param w1ID
     */
    public WaAS_W1Record(WaAS_W1ID w1ID) {
        this(w1ID, null, new ArrayList<>());
    }

    public WaAS_W1Record(WaAS_W1ID w1ID, WaAS_W1HRecord hr) {
        this(w1ID, hr, new ArrayList<>());
    }

    public WaAS_W1Record(WaAS_W1ID w1ID, WaAS_W1HRecord hr, 
            ArrayList<WaAS_W1PRecord> prs) {
        super(w1ID);
        this.hr = hr;
        this.prs = prs;
    }

    /**
     * @return the hr
     */
    public WaAS_W1HRecord getHr() {
        return hr;
    }

    /**
     * @return the prs
     */
    public ArrayList<WaAS_W1PRecord> getPrs() {
        return prs;
    }

    @Override
    public WaAS_W1ID getID() {
        return (WaAS_W1ID) id;
    }
}
