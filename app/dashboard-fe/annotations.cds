using { plugin.cds_caching.CachingApiService as S } from '../../index';

// List Report
annotate S.Caches with @(
    UI.SelectionFields: [name, metricsEnabled, keyMetricsEnabled],
    UI.LineItem: [
        { $Type: 'UI.DataField', Value: name,              Label: 'Name' },
        { $Type: 'UI.DataField', Value: metricsEnabled,    Label: 'Metrics Enabled' },
        { $Type: 'UI.DataField', Value: keyMetricsEnabled, Label: 'Key Metrics Enabled' }
    ]
);

// Object Page header
annotate S.Caches with @(
    UI.HeaderInfo: {
        $Type         : 'UI.HeaderInfoType',
        TypeName      : 'Cache',
        TypeNamePlural: 'Caches',
        Title         : { $Type: 'UI.DataField', Value: name }
    },
    UI.DataPoint #metricsStatus: {
        Value      : metricsEnabled,
        Title      : 'Metrics',
        Criticality: metricsStatus
    },
    UI.DataPoint #keyMetricsStatus: {
        Value      : keyMetricsEnabled,
        Title      : 'Key Metrics',
        Criticality: keyMetricsStatus
    },
    UI.HeaderFacets: [
        {
            $Type : 'UI.ReferenceFacet',
            ID    : 'MetricsStatus',
            Target: '@UI.DataPoint#metricsStatus'
        },
        {
            $Type : 'UI.ReferenceFacet',
            ID    : 'KeyMetricsStatus',
            Target: '@UI.DataPoint#keyMetricsStatus'
        }
    ],
    UI.Identification: [
        { $Type: 'UI.DataFieldForAction', Action: 'plugin.cds_caching.CachingApiService.toggleMetrics',    Label: 'Toggle Metrics' },
        { $Type: 'UI.DataFieldForAction', Action: 'plugin.cds_caching.CachingApiService.toggleKeyMetrics', Label: 'Toggle Key Metrics' },
        { $Type: 'UI.DataFieldForAction', Action: 'plugin.cds_caching.CachingApiService.clear',            Label: 'Clear Cache' },
        { $Type: 'UI.DataFieldForAction', Action: 'plugin.cds_caching.CachingApiService.clearMetrics',     Label: 'Clear Metrics' },
        { $Type: 'UI.DataFieldForAction', Action: 'plugin.cds_caching.CachingApiService.clearKeyMetrics',  Label: 'Clear Key Metrics' }
    ],
    UI.Facets: []
);

// ─── METRICS TABLE ───────────────────────────────────────────────────────────

annotate S.Metrics with @(
    UI.HeaderInfo: {
        TypeName      : 'Metric',
        TypeNamePlural: 'Metrics',
        Title         : { Value: ID }
    },
    UI.LineItem: [
        { $Type: 'UI.DataField', Value: timestamp,             Label: 'Timestamp' },
        { $Type: 'UI.DataField', Value: period,                Label: 'Period' },
        { $Type: 'UI.DataField', Value: hits,                  Label: 'Hits' },
        { $Type: 'UI.DataField', Value: misses,                Label: 'Misses' },
        { $Type: 'UI.DataField', Value: errors,                Label: 'Errors' },
        { $Type: 'UI.DataField', Value: totalRequests,         Label: 'Total Requests' },
        { $Type: 'UI.DataField', Value: hitRatio,              Label: 'Hit Ratio %' },
        { $Type: 'UI.DataField', Value: throughput,            Label: 'Throughput' },
        { $Type: 'UI.DataField', Value: errorRate,             Label: 'Error Rate %' },
        { $Type: 'UI.DataField', Value: cacheEfficiency,       Label: 'Cache Efficiency' },
        { $Type: 'UI.DataField', Value: avgReadThroughLatency, Label: 'Avg RT Latency (ms)' },
        { $Type: 'UI.DataField', Value: avgHitLatency,         Label: 'Avg Hit (ms)' },
        { $Type: 'UI.DataField', Value: minHitLatency,         Label: 'Min Hit (ms)' },
        { $Type: 'UI.DataField', Value: maxHitLatency,         Label: 'Max Hit (ms)' },
        { $Type: 'UI.DataField', Value: avgMissLatency,        Label: 'Avg Miss (ms)' },
        { $Type: 'UI.DataField', Value: minMissLatency,        Label: 'Min Miss (ms)' },
        { $Type: 'UI.DataField', Value: maxMissLatency,        Label: 'Max Miss (ms)' },
        { $Type: 'UI.DataField', Value: nativeSets,            Label: 'Sets' },
        { $Type: 'UI.DataField', Value: nativeGets,            Label: 'Gets' },
        { $Type: 'UI.DataField', Value: nativeDeletes,         Label: 'Deletes' },
        { $Type: 'UI.DataField', Value: nativeClears,          Label: 'Clears' },
        { $Type: 'UI.DataField', Value: nativeDeleteByTags,    Label: 'Delete By Tags' },
        { $Type: 'UI.DataField', Value: nativeErrors,          Label: 'Native Errors' },
        { $Type: 'UI.DataField', Value: totalNativeOperations, Label: 'Total Native Ops' },
        { $Type: 'UI.DataField', Value: nativeThroughput,      Label: 'Native Throughput' },
        { $Type: 'UI.DataField', Value: nativeErrorRate,       Label: 'Native Error Rate %' }
    ]
);

// ─── KEY METRICS TABLE ────────────────────────────────────────────────────────

annotate S.KeyMetrics with @(
    UI.HeaderInfo: {
        TypeName      : 'Key Metric',
        TypeNamePlural: 'Key Metrics',
        Title         : { Value: keyName }
    },
    UI.LineItem: [
        { $Type: 'UI.DataField', Value: keyName,          Label: 'Key Name' },
        { $Type: 'UI.DataField', Value: operation,        Label: 'Operation' },
        { $Type: 'UI.DataField', Value: dataType,         Label: 'Type' },
        { $Type: 'UI.DataField', Value: operationType,    Label: 'Operation Type' },
        { $Type: 'UI.DataField', Value: target,           Label: 'Target' },
        { $Type: 'UI.DataField', Value: hits,             Label: 'Hits' },
        { $Type: 'UI.DataField', Value: misses,           Label: 'Misses' },
        { $Type: 'UI.DataField', Value: totalRequests,    Label: 'Total' },
        { $Type: 'UI.DataField', Value: hitRatio,         Label: 'Hit Ratio %' },
        { $Type: 'UI.DataField', Value: cacheEfficiency,  Label: 'Cache Efficiency' },
        { $Type: 'UI.DataField', Value: avgHitLatency,    Label: 'Avg Hit (ms)' },
        { $Type: 'UI.DataField', Value: minHitLatency,    Label: 'Min Hit (ms)' },
        { $Type: 'UI.DataField', Value: maxHitLatency,    Label: 'Max Hit (ms)' },
        { $Type: 'UI.DataField', Value: avgMissLatency,   Label: 'Avg Miss (ms)' },
        { $Type: 'UI.DataField', Value: minMissLatency,   Label: 'Min Miss (ms)' },
        { $Type: 'UI.DataField', Value: maxMissLatency,   Label: 'Max Miss (ms)' },
        { $Type: 'UI.DataField', Value: nativeHits,       Label: 'Native Hits' },
        { $Type: 'UI.DataField', Value: nativeMisses,     Label: 'Native Misses' },
        { $Type: 'UI.DataField', Value: nativeSets,       Label: 'Native Sets' },
        { $Type: 'UI.DataField', Value: nativeDeletes,    Label: 'Native Deletes' },
        { $Type: 'UI.DataField', Value: tenant,           Label: 'Tenant' },
        { $Type: 'UI.DataField', Value: user,             Label: 'User' },
        { $Type: 'UI.DataField', Value: locale,           Label: 'Locale' },
        { $Type: 'UI.DataField', Value: lastAccess,       Label: 'Last Access' },
        { $Type: 'UI.DataField', Value: timestamp,        Label: 'First Access' }
    ]
);

// ─── METRICS SUB-OBJECT PAGE — MICROCHARTS ───────────────────────────────────

annotate S.Metrics with @(

    UI.DataPoint #hitRatio: {
        Value      : hitRatio,
        Title      : 'Hit Ratio',
        TargetValue: 100
    },
    UI.DataPoint #errorRate: {
        Value: errorRate,
        Title: 'Error Rate'
    },
    UI.DataPoint #throughput: {
        Value: throughput,
        Title: 'Throughput'
    },
    UI.DataPoint #avgHitLatency: {
        Value: avgHitLatency,
        Title: 'Avg Hit Latency'
    },
    UI.DataPoint #avgMissLatency: {
        Value: avgMissLatency,
        Title: 'Avg Miss Latency'
    },

    UI.Chart #hitRatioChart: {
        $Type            : 'UI.ChartDefinitionType',
        Title            : 'Hit Ratio',
        ChartType        : #Donut,
        Measures         : [hitRatio],
        MeasureAttributes: [{
            $Type    : 'UI.ChartMeasureAttributeType',
            Measure  : hitRatio,
            Role     : #Axis1,
            DataPoint: '@UI.DataPoint#hitRatio'
        }]
    },
    UI.Chart #throughputChart: {
        $Type            : 'UI.ChartDefinitionType',
        Title            : 'Throughput',
        ChartType        : #Column,
        Measures         : [throughput],
        Dimensions       : [timestamp],
        MeasureAttributes: [{
            $Type    : 'UI.ChartMeasureAttributeType',
            Measure  : throughput,
            Role     : #Axis1,
            DataPoint: '@UI.DataPoint#throughput'
        }]
    },
    UI.Chart #errorRateChart: {
        $Type            : 'UI.ChartDefinitionType',
        Title            : 'Error Rate',
        ChartType        : #Bullet,
        Measures         : [errorRate],
        MeasureAttributes: [{
            $Type    : 'UI.ChartMeasureAttributeType',
            Measure  : errorRate,
            Role     : #Axis1,
            DataPoint: '@UI.DataPoint#errorRate'
        }]
    },
    UI.Chart #latencyChart: {
        $Type            : 'UI.ChartDefinitionType',
        Title            : 'Hit vs Miss Latency',
        ChartType        : #Bar,
        Measures         : [avgHitLatency, avgMissLatency],
        MeasureAttributes: [
            {
                $Type    : 'UI.ChartMeasureAttributeType',
                Measure  : avgHitLatency,
                Role     : #Axis1,
                DataPoint: '@UI.DataPoint#avgHitLatency'
            },
            {
                $Type    : 'UI.ChartMeasureAttributeType',
                Measure  : avgMissLatency,
                Role     : #Axis1,
                DataPoint: '@UI.DataPoint#avgMissLatency'
            }
        ]
    },

    UI.HeaderFacets: [
        { $Type: 'UI.ReferenceFacet', ID: 'HitRatioChart',   Target: '@UI.Chart#hitRatioChart',   Label: 'Hit Ratio' },
        { $Type: 'UI.ReferenceFacet', ID: 'ThroughputChart', Target: '@UI.Chart#throughputChart', Label: 'Throughput' },
        { $Type: 'UI.ReferenceFacet', ID: 'ErrorRateChart',  Target: '@UI.Chart#errorRateChart',  Label: 'Error Rate' },
        { $Type: 'UI.ReferenceFacet', ID: 'LatencyChart',    Target: '@UI.Chart#latencyChart',    Label: 'Latency' }
    ],

    UI.Facets: [
        {
            $Type : 'UI.CollectionFacet',
            ID    : 'ReadThroughSection',
            Label : 'Read-Through Performance',
            Facets: [{ $Type: 'UI.ReferenceFacet', Target: '@UI.FieldGroup#ReadThrough' }]
        },
        {
            $Type : 'UI.CollectionFacet',
            ID    : 'NativeSection',
            Label : 'Native Function Performance',
            Facets: [{ $Type: 'UI.ReferenceFacet', Target: '@UI.FieldGroup#NativeOps' }]
        }
    ],

    UI.FieldGroup #ReadThrough: {
        Data: [
            { $Type: 'UI.DataField', Value: hits },
            { $Type: 'UI.DataField', Value: misses },
            { $Type: 'UI.DataField', Value: errors },
            { $Type: 'UI.DataField', Value: totalRequests },
            { $Type: 'UI.DataField', Value: hitRatio },
            { $Type: 'UI.DataField', Value: throughput },
            { $Type: 'UI.DataField', Value: errorRate },
            { $Type: 'UI.DataField', Value: cacheEfficiency },
            { $Type: 'UI.DataField', Value: avgReadThroughLatency },
            { $Type: 'UI.DataField', Value: avgHitLatency },
            { $Type: 'UI.DataField', Value: minHitLatency },
            { $Type: 'UI.DataField', Value: maxHitLatency },
            { $Type: 'UI.DataField', Value: avgMissLatency },
            { $Type: 'UI.DataField', Value: minMissLatency },
            { $Type: 'UI.DataField', Value: maxMissLatency }
        ]
    },

    UI.FieldGroup #NativeOps: {
        Data: [
            { $Type: 'UI.DataField', Value: nativeSets },
            { $Type: 'UI.DataField', Value: nativeGets },
            { $Type: 'UI.DataField', Value: nativeDeletes },
            { $Type: 'UI.DataField', Value: nativeClears },
            { $Type: 'UI.DataField', Value: nativeDeleteByTags },
            { $Type: 'UI.DataField', Value: nativeErrors },
            { $Type: 'UI.DataField', Value: totalNativeOperations },
            { $Type: 'UI.DataField', Value: nativeThroughput },
            { $Type: 'UI.DataField', Value: nativeErrorRate }
        ]
    }
);
