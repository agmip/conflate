/*
 * Copyright (c) 2013, AgMIP All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 *
 *  Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 *  Neither the name of the AgMIP nor the names of its
 *   contributors may be used to endorse or promote products derived from this
 *   software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.agmip.ws.conflate.api;

import org.agmip.ace.util.GeoPoint;

public class NaviPoint {
    private final String lat;
    private final String lng;
    private final String geohash;
    private final String countryISO;
    private final String adm0;
    private final String adm1;
    private final String adm2;
    private final String error;


    public NaviPoint() {
        this.lat        = null;
        this.lng        = null;
        this.geohash    = null;
        this.countryISO = null;
        this.adm0       = null;
        this.adm1       = null;
        this.adm2       = null;
        this.error      = null;
    }

    public NaviPoint(String lat, String lng, String geohash, String countryISO,
                     String adm0, String adm1, String adm2) {
        this.lat        = lat;
        this.lng        = lng;
        this.geohash    = geohash;
        this.countryISO = countryISO;
        this.adm0       = adm0;
        this.adm1       = adm1;
        this.adm2       = adm2;
        this.error      = null;
    }

    public NaviPoint(String lat, String lng, String countryISO,
                     String adm0, String adm1, String adm2) {
        this.lat        = lat;
        this.lng        = lng;
        this.countryISO = countryISO;
        this.adm0       = adm0;
        this.adm1       = adm1;
        this.adm2       = adm2;
        this.error      = null;

        this.geohash = GeoPoint.calculateGeoHash(lat, lng);
    }

    public NaviPoint(String error) {
        this.lat        = null;
        this.lng        = null;
        this.geohash    = null;
        this.countryISO = null;
        this.adm0       = null;
        this.adm1       = null;
        this.adm2       = null;
        this.error      = error;
    }

    public String getLat() {
        return this.lat;
    }

    public String getLng() {
        return this.lng;
    }

    public String getGeohash() {
        return this.geohash;
    }

    public String getCountryISO() {
        return this.countryISO;
    }

    public String getAdm0() {
        return this.adm0;
    }

    public String getAdm1() {
        return this.adm1;
    }

    public String getAdm2() {
        return this.adm2;
    }

    public String getError() {
        return this.error;
    }
}
