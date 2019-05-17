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

import java.io.Serializable;

/**
 *
 * @author geoagdt
 */
public class WaAS_ID2 implements Comparable, Serializable {

    private final WaAS_W1ID CASEW1;
    private final WaAS_ID CASEWX;

    public WaAS_ID2(WaAS_W1ID CASEW1, WaAS_ID CASEWX) {
        this.CASEW1 = CASEW1;
        this.CASEWX = CASEWX;
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof WaAS_ID2) {
            WaAS_ID2 o2 = (WaAS_ID2) o;
            if (CASEW1.ID > o2.CASEW1.ID) {
                return 2;
            } else {
                if (CASEW1.ID < o2.CASEW1.ID) {
                    return -2;
                }
                if (CASEWX.ID > o2.CASEWX.ID) {
                    return 1;
                } else {
                    if (CASEWX.ID < o2.CASEWX.ID) {
                        return -1;
                    }
                }
                return 0;
            }
        }
        return -3;
    }

    /**
     * @return the CASEW1
     */
    public WaAS_W1ID getCASEW1() {
        return CASEW1;
    }

    /**
     * @return the CASEWX
     */
    public WaAS_ID getCASEWX() {
        return CASEWX;
    }

    @Override
    public String toString() {
        return "CASEW1 " + CASEW1.ID + " CASEWX " + CASEWX.ID;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof WaAS_ID2) {
            WaAS_ID2 o2;
            o2 = (WaAS_ID2) o;
            if (CASEW1.ID == o2.CASEW1.ID) {
                if (CASEWX.ID == o2.CASEWX.ID) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 59 * hash + this.CASEW1.ID;
        hash = 59 * hash + this.CASEWX.ID;
        return hash;
    }

}
