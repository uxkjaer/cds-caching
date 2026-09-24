namespace plugin.cds_caching;

entity Caches {
    key name              : String         @title: 'Name';
        config            : String         @title: 'Configuration';
        metricsEnabled    : Boolean default false @title: 'Metrics Enabled';
        keyMetricsEnabled : Boolean default false @title: 'Key Metrics Enabled';
        tagMetricsEnabled : Boolean default false @title: 'Tag Metrics Enabled';
        metrics           : Composition of many Metrics
                                on metrics.cache = $self.name;
        keyMetrics        : Composition of many KeyMetrics
                                on keyMetrics.cache = $self.name;
        tagMetrics        : Composition of many TagMetrics
                                on tagMetrics.cache = $self.name;
}

entity Metrics {
    key ID                    : String; // e.g., 'daily:2024-03-20' or 'hourly:2024-03-20-15'
    key cache                 : String @title: 'Cache';
        timestamp             : DateTime @title: 'Timestamp';
        period                : String @title: 'Period' enum {
            hourly;
            daily;
            monthly;
        };

        // Read-through metrics
        hits                  : Integer default 0    @title: 'Hits';
        misses                : Integer default 0    @title: 'Misses';
        errors                : Integer default 0    @title: 'Errors';
        totalRequests         : Integer default 0    @title: 'Total Requests';
        // Read-through latency
        avgHitLatency         : Decimal(15,2) @title: 'Avg Hit Latency (ms)';
        minHitLatency         : Decimal(15,2) @title: 'Min Hit Latency (ms)';
        maxHitLatency         : Decimal(15,2) @title: 'Max Hit Latency (ms)';
        avgMissLatency        : Decimal(15,2) @title: 'Avg Miss Latency (ms)';
        minMissLatency        : Decimal(15,2) @title: 'Min Miss Latency (ms)';
        maxMissLatency        : Decimal(15,2) @title: 'Max Miss Latency (ms)';
        avgReadThroughLatency : Decimal(15,2) @title: 'Avg RT Latency (ms)';

        // Read-through performance
        hitRatio              : Decimal(15,2) @title: 'Hit Ratio %';
        throughput            : Decimal(15,2) @title: 'Throughput (req/s)';
        errorRate             : Decimal(15,2) @title: 'Error Rate %';
        cacheEfficiency       : Decimal(15,2) @title: 'Cache Efficiency (x)';

        // Native function metrics
        nativeSets            : Integer default 0    @title: 'Sets';
        nativeGets            : Integer default 0    @title: 'Gets';
        nativeDeletes         : Integer default 0    @title: 'Deletes';
        nativeClears          : Integer default 0    @title: 'Clears';
        nativeDeleteByTags    : Integer default 0    @title: 'Delete By Tags';
        nativeErrors          : Integer default 0    @title: 'Native Errors';
        totalNativeOperations : Integer default 0    @title: 'Total Native Ops';
        nativeThroughput      : Decimal(15,2) @title: 'Native Throughput (ops/s)';
        nativeErrorRate       : Decimal(15,2) @title: 'Native Error Rate %';

        // Common metrics
        memoryUsage           : Integer @title: 'Memory Usage (bytes)';
        itemCount             : Integer @title: 'Item Count';
        uptimeMs              : Integer @title: 'Uptime (ms)';
}

entity KeyMetrics {
    key ID                    : String;
    key cache                 : String;
    key keyName               : String;
        lastAccess            : DateTime;
        period                : String enum {
            current;
            hourly;
            daily;
        };

        // Operation type tracking
        operationType         : String enum {
            read_through;
            native;
            mixed;
        };

        // Read-through metrics (hits and misses only)
        hits                  : Integer default 0;
        misses                : Integer default 0;
        errors                : Integer default 0;
        totalRequests         : Integer default 0;
        hitRatio              : Decimal(15,2); // hit ratio as percentage
        cacheEfficiency       : Decimal(15,2); // ratio of miss latency to hit latency

        // Read-through latency metrics
        avgHitLatency         : Decimal(15,2); // average hit latency in milliseconds
        minHitLatency         : Decimal(15,2); // minimum hit latency
        maxHitLatency         : Decimal(15,2); // maximum hit latency
        avgMissLatency        : Decimal(15,2); // average miss latency in milliseconds
        minMissLatency        : Decimal(15,2); // minimum miss latency
        maxMissLatency        : Decimal(15,2); // maximum miss latency
        avgReadThroughLatency : Decimal(15,2); // average read through latency in milliseconds

        // Read-through performance metrics
        throughput            : Decimal(15,2); // requests per second
        errorRate             : Decimal(15,2); // error rate as percentage

        // Native function metrics (counts only)
        nativeHits            : Integer default 0;
        nativeMisses          : Integer default 0;
        nativeSets            : Integer default 0;
        nativeDeletes         : Integer default 0;
        nativeClears          : Integer default 0;
        nativeDeleteByTags    : Integer default 0;
        nativeErrors          : Integer default 0;
        totalNativeOperations : Integer default 0;
        // Native function performance metrics
        nativeThroughput      : Decimal(15,2); // native operations per second
        nativeErrorRate       : Decimal(15,2); // native operation error rate

        // Metadata fields
        dataType              : String;
        operation             : String; // concatenated cache service operations (e.g., SET, GET, WRAP, RUN...)
        metadata              : LargeString; // JSON string for additional metadata
        // Enhanced context information
        context               : String; // JSON string with detailed context
        query                 : LargeString; // CQL query text if applicable
        subject               : LargeString; // JSON string with subject information
        target                : String; // Target information
        tenant                : String; // Tenant information
        user                  : String; // User information
        locale                : String; // Locale information
        timestamp             : DateTime; // When this key was first accessed
        cacheOptions          : String; // JSON string with cache options
}

/**
 * Per-tag metrics. Opt-in via `tagMetricsEnabled` / `metrics.tagMetricsEnabled`.
 *
 * An entry can carry several tags, so a single hit increments every matching tag
 * row. Tag totals can therefore exceed cache-level `Metrics` totals — that is
 * expected, not a bug. Query by the resolved tag string (exact match), e.g.
 * `tag eq 'federation:Airports'`.
 */
entity TagMetrics {
    key ID                    : String;
    key cache                 : String;
    key tag                   : String;
        lastAccess            : DateTime;
        period                : String enum {
            current;
            hourly;
            daily;
        };

        // Read-through metrics (hits and misses only)
        hits                  : Integer default 0;
        misses                : Integer default 0;
        errors                : Integer default 0;
        totalRequests         : Integer default 0;
        hitRatio              : Decimal(15,2);
        cacheEfficiency       : Decimal(15,2);

        // Read-through latency metrics
        avgHitLatency         : Decimal(15,2);
        minHitLatency         : Decimal(15,2);
        maxHitLatency         : Decimal(15,2);
        avgMissLatency        : Decimal(15,2);
        minMissLatency        : Decimal(15,2);
        maxMissLatency        : Decimal(15,2);
        avgReadThroughLatency : Decimal(15,2);

        // Read-through performance metrics
        throughput            : Decimal(15,2);
        errorRate             : Decimal(15,2);

        // Native function metrics (counts only)
        nativeHits            : Integer default 0;
        nativeMisses          : Integer default 0;
        nativeSets            : Integer default 0;
        nativeDeletes         : Integer default 0;
        nativeErrors          : Integer default 0;
        totalNativeOperations : Integer default 0;
        nativeThroughput      : Decimal(15,2);
        nativeErrorRate       : Decimal(15,2);

        timestamp             : DateTime;
}
