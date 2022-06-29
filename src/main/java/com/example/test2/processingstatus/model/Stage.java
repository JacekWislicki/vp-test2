package com.example.test2.processingstatus.model;

public enum Stage {

    SPIL_VEHICLE_CREATION_TQS,
    SPIL_VEHICLE_CREATION_INPUT_DECORATOR,
    SPIL_VEHICLE_CREATION_INPUT_ROUTER,
    SPIL_VEHICLE_CREATION_INPUT_VALIDATOR,
    SPIL_VEHICLE_CREATION_INPUT_FILTER,

    ONREQUEST_VEHICLE_CREATION_S3,
    ONREQUEST_VEHICLE_CREATION_INPUT_DECORATOR,
    ONREQUEST_VEHICLE_CREATION_INPUT_VALIDATOR,

    DOMTOM_VEHICLE_CREATION_INPUT_DECORATOR,
    DOMTOM_VEHICLE_CREATION_INPUT_VALIDATOR,

    //TODO rename VC enriched common flow
    VEHICLE_CREATION_NMSC_ENRICHMENT,
    VEHICLE_CREATION_PRODUCT_ENRICHMENT,
    VEHICLE_CREATION_CWS_ENRICHMENT,
    VEHICLE_CREATION_WLTP_ENRICHMENT,
    VEHICLE_CREATION_CCIS_ENRICHMENT,
    VEHICLE_CREATION_RECYCLER,

    COC_TVV_S3,
    COC_TVV_PULSAR,

    TEST_COC_TVV_S3,
    TEST_SPIL_VEHICLE_CREATION_S3
}