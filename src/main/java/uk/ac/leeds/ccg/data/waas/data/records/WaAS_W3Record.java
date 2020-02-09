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

import uk.ac.leeds.ccg.data.waas.data.id.WaAS_W3ID;
import java.util.ArrayList;
import uk.ac.leeds.ccg.data.Data_Record;
import uk.ac.leeds.ccg.data.waas.data.hhold.WaAS_W3HRecord;
import uk.ac.leeds.ccg.data.waas.data.person.WaAS_W3PRecord;

/**
 *
 * @author Andy Turner
 * @version 1.0.0
 */
public class WaAS_W3Record extends Data_Record {

    private final WaAS_W3HRecord hr;

    private final ArrayList<WaAS_W3PRecord> prs;

    /**
     * Defaults hr to null and prs to a new ArrayList<>().
     *
     * @param w3ID
     */
    public WaAS_W3Record(WaAS_W3ID w3ID) {
        this(w3ID, null, new ArrayList<>());
    }

    public WaAS_W3Record(WaAS_W3ID w3ID, WaAS_W3HRecord hr) {
        this(w3ID, hr, new ArrayList<>());
    }

    public WaAS_W3Record(WaAS_W3ID w3ID, WaAS_W3HRecord hr, 
            ArrayList<WaAS_W3PRecord> prs) {
        super(w3ID);
        this.hr = hr;
        this.prs = prs;
    }

    /**
     * @return the hr
     */
    public WaAS_W3HRecord getHr() {
        return hr;
    }

    /**
     * @return the prs
     */
    public ArrayList<WaAS_W3PRecord> getPrs() {
        return prs;
    }

    @Override
    public WaAS_W3ID getID() {
        return (WaAS_W3ID) id;
    }
}
