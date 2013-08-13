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
        this.count = 0;
    }

    public LocationCacheEntry(String geohash, int count) {
        this.point = new GeoPoint(geohash);
        this.count = count;
    }

    public LocationCacheEntry(String geohash) {
        this.point = new GeoPoint(geohash);
        this.count = 0;
    }

    public LocationCacheEntry(String lat, String lng, int count) {
        this.point = new GeoPoint(lat, lng);
        this.count = count;
    }

    public LocationCacheEntry(String lat, String lng) {
        this.point = new GeoPoint(lat, lng);
        this.count = 0;
    }

    public LocationCacheEntry(double lat, double lng, int count) {
        this.point = new GeoPoint(lat, lng);
        this.count = count;
    }

    public LocationCacheEntry(double lat, double lng) {
        this.point = new GeoPoint(lat, lng);
        this.count = 0;
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

    @Override
    public String toString() {
        return "Key: "+this.point.getGeoHash()+" Count:"+this.count;
    }
}
