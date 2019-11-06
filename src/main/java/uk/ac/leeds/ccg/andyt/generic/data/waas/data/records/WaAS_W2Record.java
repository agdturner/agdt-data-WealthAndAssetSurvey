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

import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W2ID;
import java.util.ArrayList;
import uk.ac.leeds.ccg.andyt.data.Data_Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W2HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_W2PRecord;

/**
 *
 * @author geoagdt
 */
public class WaAS_W2Record extends Data_Record {

    private final WaAS_W2HRecord hr;

    private final ArrayList<WaAS_W2PRecord> prs;

    /**
     * Defaults hr to null and prs to a new ArrayList<>().
     *
     * @param w2ID
     */
    public WaAS_W2Record(WaAS_W2ID w2ID) {
        this(w2ID, null, new ArrayList<>());
    }

    public WaAS_W2Record(WaAS_W2ID w2ID, WaAS_W2HRecord hr) {
        this(w2ID, hr, new ArrayList<>());
    }

    public WaAS_W2Record(WaAS_W2ID w2ID, WaAS_W2HRecord hr, 
            ArrayList<WaAS_W2PRecord> prs) {
        super(w2ID);
        this.hr = hr;
        this.prs = prs;
    }

    /**
     * @return the hr
     */
    public WaAS_W2HRecord getHr() {
        return hr;
    }

    /**
     * @return the prs
     */
    public ArrayList<WaAS_W2PRecord> getPrs() {
        return prs;
    }

    @Override
    public WaAS_W2ID getID() {
        return (WaAS_W2ID) ID;
    }
}