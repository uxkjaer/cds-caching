using from './db/statistics';

context plugin.cds_caching {

    type CacheEntry {
        entryKey  : String;
        value     : String;
        timestamp : DateTime;
        tags      : array of String;
    }

    /**
     * Management API for cache entries and metrics.
     *
     * Guarded with `authenticated-user` by default, because these operations can
     * read and flush cache contents. Override in your own model to require a
     * dedicated role instead:
     *
     *   annotate plugin.cds_caching.CachingApiService with @requires: 'CacheAdmin';
     */
    @impl    : 'cds-caching/srv/caching-api-service'
    @requires: 'authenticated-user'
    service CachingApiService {

        // Writes go through the bound actions below, never through CRUD, so that
        // config rows (metricsEnabled, config) cannot be tampered with directly.
        @readonly
        entity Caches     as projection on plugin.cds_caching.Caches
            actions {

                function getEntries(top: Integer, skip: Integer) returns array of CacheEntry;
                function getEntry(key: String)                   returns CacheEntry;

                action   setEntry(key: String, value: String, ttl: Integer) returns Boolean;
                action   deleteEntry(key: String)                           returns Boolean;
                action   clear()                                            returns Boolean;
                action   clearTagMetrics()                                  returns Boolean;
                action   setTagMetricsEnabled(enabled: Boolean)             returns Boolean;
                @Common.SideEffects: {
                    $Type         : 'Common.SideEffectsType',
                    TargetEntities: [in.Metrics]
                }
                action   clearMetrics()                                     returns Boolean;
                @Common.SideEffects: {
                    $Type         : 'Common.SideEffectsType',
                    TargetEntities: [in.keyMetrics]
                }
                action   clearKeyMetrics()                                  returns Boolean;
                @Common.SideEffects: {
                    $Type         : 'Common.SideEffectsType',
                    TargetEntities: [in]
                }
                action   toggleMetrics(enabled: Boolean)                    returns Boolean;
                @Common.SideEffects: {
                    $Type         : 'Common.SideEffectsType',
                    TargetEntities: [in]
                }
                action   toggleKeyMetrics(enabled: Boolean)                 returns Boolean;
            };

        @readonly
        entity Metrics    as projection on plugin.cds_caching.Metrics;

        @readonly
        entity KeyMetrics as projection on plugin.cds_caching.KeyMetrics;

        @readonly
        entity TagMetrics as projection on plugin.cds_caching.TagMetrics;

    }
}

extend projection plugin.cds_caching.CachingApiService.Caches with {
    case when metricsEnabled    = true then 3 else 1 end as metricsStatus    : Integer,
    case when keyMetricsEnabled = true then 3 else 1 end as keyMetricsStatus : Integer
}
