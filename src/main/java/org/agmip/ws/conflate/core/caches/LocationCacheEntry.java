package org.agmip.ws.conflate.core.caches;

import org.agmip.ace.util.GeoPoint;

public class LocationCacheEntry {
    private GeoPoint point;
    private int count;


    public LocationCacheEntry() {
        this.point = new GeoPoint();
        this.count = 0;
    }

    public LocationCacheEntry(GeoPoint point, int count) {
        this.point = point;
        this.count = count;
    }

    public LocationCacheEntry(GeoPoint point) {
        this.point = point;
        this.count = 1;
    }

    public LocationCacheEntry(String geohash, int count) {
        this.point = new GeoPoint(geohash);
        this.count = count;
    }

    public LocationCacheEntry(String geohash) {
        this.point = new GeoPoint(geohash);
        this.count = 1;
    }

    public LocationCacheEntry(String lat, String lng, int count) {
        this.point = new GeoPoint(lat, lng);
        this.count = count;
    }

    public LocationCacheEntry(String lat, String lng) {
        this.point = new GeoPoint(lat, lng);
        this.count = 1;
    }


    public GeoPoint getPoint() {
        return this.point;
    }

    public int getCount() {
        return this.count;
    }

    public void incrementCount() {
        this.count++;
    }

    public void decrementCount() {
        this.count--;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "Key: "+this.point.getGeoHash()+" Count:"+this.count;
    }
}
