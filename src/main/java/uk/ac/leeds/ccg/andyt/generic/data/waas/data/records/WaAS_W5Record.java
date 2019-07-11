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

import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W5ID;
import java.util.ArrayList;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Object;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W5HRecord;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.WaAS_W5PRecord;

/**
 *
 * @author geoagdt
 */
public class WaAS_W5Record extends WaAS_Object {

    public final WaAS_W5ID w5ID;

    private final WaAS_W5HRecord hhold;

    private final ArrayList<WaAS_W5PRecord> people;

    public WaAS_W5Record(WaAS_Environment e, WaAS_W5ID w5ID) {
        super(e);
        this.w5ID = w5ID;
        hhold = null;
        people = new ArrayList<>();
    }

    public WaAS_W5Record(WaAS_Environment e,
            WaAS_W5ID w5ID, WaAS_W5HRecord hhold) {
        this(e, w5ID, hhold, new ArrayList<>());
    }

    public WaAS_W5Record(WaAS_Environment e, WaAS_W5ID w5ID,
            WaAS_W5HRecord hhold, ArrayList<WaAS_W5PRecord> people) {
        super(e);
        this.w5ID = w5ID;
        this.hhold = hhold;
        this.people = people;
    }

    /**
     * @return the hhold
     */
    public WaAS_W5HRecord getHhold() {
        return hhold;
    }

    /**
     * @return the people
     */
    public ArrayList<WaAS_W5PRecord> getPeople() {
        return people;
    }
}
