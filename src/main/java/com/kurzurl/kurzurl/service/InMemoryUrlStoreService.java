package com.kurzurl.kurzurl.service;

import org.springframework.stereotype.Service;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author nagmaansari
 *
 */
@Service
public class InMemoryUrlStoreService implements IUrlStoreService{
    private Map<String, String> urlByIdMap = new ConcurrentHashMap<>();

    @Override
    public String findUrlById(String id) {
        return urlByIdMap.get(id);
    }

    @Override
    public void storeUrl(String id, String url) {
        urlByIdMap.put(id, url);
    }
}
