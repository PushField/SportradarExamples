package com.sportradar.unifiedodds.example.examples;

import com.pushtechnology.gateway.framework.convertors.PayloadConvertor;

public abstract class Sportradar_to_JSON implements PayloadConvertor {

    private String name = "Sportradar_to_JSON";

    @Override
    public String getName() {
        return name;
    }

    /*public Object getDiffusionType() {
        return (Object) JSON;
    }*/
}
