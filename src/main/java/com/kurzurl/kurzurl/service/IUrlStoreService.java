package com.kurzurl.kurzurl.service;


/**
 * @author nagmaansari
 *
 */
public interface IUrlStoreService {
    String findUrlById(String id);

    void storeUrl(String id, String url);
}
