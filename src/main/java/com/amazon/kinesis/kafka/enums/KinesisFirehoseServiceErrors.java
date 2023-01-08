package com.amazon.kinesis.kafka.enums;

import java.util.*;

public enum KinesisFirehoseServiceErrors {
    UNAVAILABLE("ServiceUnavailableException"),
    THROTTLED("ThrottlingException"),
    INTERNAL_FAILURE("InternalFailure");

    String code;

    KinesisFirehoseServiceErrors(String errorCode) {
        this.code = errorCode;
    }

    private static final Map<String, KinesisFirehoseServiceErrors> nameToErrorMap = new LinkedHashMap<>();

    private static final List<KinesisFirehoseServiceErrors> retryable = new ArrayList<>();

    static {
        for (KinesisFirehoseServiceErrors error : KinesisFirehoseServiceErrors.values()) {
            nameToErrorMap.put(error.code, error);
        }
        retryable.add(INTERNAL_FAILURE);
        retryable.add(THROTTLED);
    }

    public static KinesisFirehoseServiceErrors lookup(String errorCode) {
        return nameToErrorMap.get(errorCode);
    }

    public static boolean isRetryable(String errorCode) {
        return retryable.contains(lookup(errorCode));
    }
}
