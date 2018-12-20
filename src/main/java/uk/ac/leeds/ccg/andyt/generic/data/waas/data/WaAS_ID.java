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
public class WaAS_ID implements Comparable, Serializable {

    private final short CASEW1;
    private final short CASEWX;

    public WaAS_ID(short CASEW1, short CASEWX) {
        this.CASEW1 = CASEW1;
        this.CASEWX = CASEWX;
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof WaAS_ID) {
            WaAS_ID o2 = (WaAS_ID) o;
            if (CASEW1 > o2.CASEW1) {
                return 2;
            } else {
                if (CASEW1 < o2.CASEW1) {
                    return -2;
                }
                if (CASEWX > o2.CASEWX) {
                    return 1;
                } else {
                    if (CASEWX < o2.CASEWX) {
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
    public short getCASEW1() {
        return CASEW1;
    }

    /**
     * @return the CASEWX
     */
    public short getCASEWX() {
        return CASEWX;
    }

    @Override
    public String toString() {
        return "CASEW1 " + CASEW1 + " CASEWX " + CASEWX;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof WaAS_ID) {
            WaAS_ID o2;
            o2 = (WaAS_ID) o;
            if (CASEW1 == o2.CASEW1) {
                if (CASEWX == o2.CASEWX) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 59 * hash + this.CASEW1;
        hash = 59 * hash + this.CASEWX;
        return hash;
    }

}
