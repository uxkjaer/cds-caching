using { plugin.cds_caching.CachingApiService as S } from '../../index';

// List Report
annotate S.Caches with @(
    Common.SemanticKey: [name],
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
    Common.SemanticKey: [ID, cache],
    UI.HeaderInfo: {
        TypeName      : 'Metric',
        TypeNamePlural: 'Metrics',
        Title         : { Value: timestamp }
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

// ─── METRICS SUB-OBJECT PAGE ─────────────────────────────────────────────────

annotate S.Metrics with @(

    // ── DataPoints ── with CriticalityCalculation matching formatter.ts thresholds

    // Hit Ratio: Good ≥ 90, Critical ≥ 70, Error < 70 (Maximize)
    UI.DataPoint #hitRatio: {
        Value      : hitRatio,
        Title      : 'Hit Ratio',
        TargetValue: 100,
        CriticalityCalculation: {
            $Type                   : 'UI.CriticalityCalculationType',
            ImprovementDirection    : #Maximize,
            ToleranceRangeLowValue  : 70,
            DeviationRangeLowValue  : 90
        }
    },

    // Error Rate: Good ≤ 1%, Critical ≤ 5%, Error > 5% (Minimize)
    UI.DataPoint #errorRate: {
        Value: errorRate,
        Title: 'Error Rate %',
        CriticalityCalculation: {
            $Type                    : 'UI.CriticalityCalculationType',
            ImprovementDirection     : #Minimize,
            ToleranceRangeHighValue  : 1,
            DeviationRangeHighValue  : 5
        }
    },

    // Cache Efficiency: Good > 5, Critical ≥ 3, Error < 3 (Maximize)
    UI.DataPoint #cacheEfficiencyKPI: {
        Value: cacheEfficiency,
        Title: 'Cache Efficiency',
        CriticalityCalculation: {
            $Type                   : 'UI.CriticalityCalculationType',
            ImprovementDirection    : #Maximize,
            ToleranceRangeLowValue  : 3,
            DeviationRangeLowValue  : 5
        }
    },

    // Throughput: plain KPI (no threshold defined in formatter)
    UI.DataPoint #throughputKPI: { Value: throughput, Title: 'Throughput (req/s)' },
    UI.DataPoint #hitsKPI:       { Value: hits,       Title: 'Hits' },
    UI.DataPoint #missesKPI:     { Value: misses,     Title: 'Misses' },

    // Latency: Good ≤ 10ms, Critical ≤ 50ms, Error > 50ms (Minimize)
    UI.DataPoint #avgHitLatencyDP: {
        Value: avgHitLatency,
        Title: 'Avg Hit Latency',
        CriticalityCalculation: {
            $Type                   : 'UI.CriticalityCalculationType',
            ImprovementDirection    : #Minimize,
            ToleranceRangeHighValue : 10,
            DeviationRangeHighValue : 50
        }
    },
    UI.DataPoint #avgMissLatencyDP: {
        Value: avgMissLatency,
        Title: 'Avg Miss Latency',
        CriticalityCalculation: {
            $Type                   : 'UI.CriticalityCalculationType',
            ImprovementDirection    : #Minimize,
            ToleranceRangeHighValue : 10,
            DeviationRangeHighValue : 50
        }
    },
    UI.DataPoint #avgRTLatencyDP: {
        Value: avgReadThroughLatency,
        Title: 'Avg RT Latency',
        CriticalityCalculation: {
            $Type                   : 'UI.CriticalityCalculationType',
            ImprovementDirection    : #Minimize,
            ToleranceRangeHighValue : 10,
            DeviationRangeHighValue : 50
        }
    },

    UI.DataPoint #nativeThroughputDP: { Value: nativeThroughput, Title: 'Native Throughput' },
    UI.DataPoint #nativeErrorRateDP: {
        Value: nativeErrorRate,
        Title: 'Native Error Rate %',
        CriticalityCalculation: {
            $Type                   : 'UI.CriticalityCalculationType',
            ImprovementDirection    : #Minimize,
            ToleranceRangeHighValue : 1,
            DeviationRangeHighValue : 5
        }
    },

    // ── Charts ──

    // Header: Donut for hit ratio
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

    // Header: Bullet for error rate
    UI.Chart #errorRateChart: {
        $Type            : 'UI.ChartDefinitionType',
        Title            : 'Error Rate %',
        ChartType        : #Bullet,
        Measures         : [errorRate],
        MeasureAttributes: [{
            $Type    : 'UI.ChartMeasureAttributeType',
            Measure  : errorRate,
            Role     : #Axis1,
            DataPoint: '@UI.DataPoint#errorRate'
        }]
    },

    // Body section charts: 3 Bullet charts for latency comparison (renders as horizontal bars)
    UI.Chart #avgHitLatencyChart: {
        $Type            : 'UI.ChartDefinitionType',
        Title            : 'Avg Hit Latency (ms)',
        ChartType        : #Bullet,
        Measures         : [avgHitLatency],
        MeasureAttributes: [{
            $Type    : 'UI.ChartMeasureAttributeType',
            Measure  : avgHitLatency,
            Role     : #Axis1,
            DataPoint: '@UI.DataPoint#avgHitLatencyDP'
        }]
    },
    UI.Chart #avgMissLatencyChart: {
        $Type            : 'UI.ChartDefinitionType',
        Title            : 'Avg Miss Latency (ms)',
        ChartType        : #Bullet,
        Measures         : [avgMissLatency],
        MeasureAttributes: [{
            $Type    : 'UI.ChartMeasureAttributeType',
            Measure  : avgMissLatency,
            Role     : #Axis1,
            DataPoint: '@UI.DataPoint#avgMissLatencyDP'
        }]
    },
    UI.Chart #avgRTLatencyChart: {
        $Type            : 'UI.ChartDefinitionType',
        Title            : 'Avg RT Latency (ms)',
        ChartType        : #Bullet,
        Measures         : [avgReadThroughLatency],
        MeasureAttributes: [{
            $Type    : 'UI.ChartMeasureAttributeType',
            Measure  : avgReadThroughLatency,
            Role     : #Axis1,
            DataPoint: '@UI.DataPoint#avgRTLatencyDP'
        }]
    },

    // Body section charts: Native performance
    UI.Chart #nativeThroughputChart: {
        $Type            : 'UI.ChartDefinitionType',
        Title            : 'Native Throughput (ops/s)',
        ChartType        : #Bullet,
        Measures         : [nativeThroughput],
        MeasureAttributes: [{
            $Type    : 'UI.ChartMeasureAttributeType',
            Measure  : nativeThroughput,
            Role     : #Axis1,
            DataPoint: '@UI.DataPoint#nativeThroughputDP'
        }]
    },
    UI.Chart #nativeErrorRateChart: {
        $Type            : 'UI.ChartDefinitionType',
        Title            : 'Native Error Rate %',
        ChartType        : #Bullet,
        Measures         : [nativeErrorRate],
        MeasureAttributes: [{
            $Type    : 'UI.ChartMeasureAttributeType',
            Measure  : nativeErrorRate,
            Role     : #Axis1,
            DataPoint: '@UI.DataPoint#nativeErrorRateDP'
        }]
    },

    // ── Header Facets (compact: 2 charts + 4 key KPI numbers) ──
    UI.HeaderFacets: [
        { $Type: 'UI.ReferenceFacet', ID: 'HitRatioChart',     Target: '@UI.Chart#hitRatioChart',        Label: 'Hit Ratio' },
        { $Type: 'UI.ReferenceFacet', ID: 'ErrorRateChart',    Target: '@UI.Chart#errorRateChart',       Label: 'Error Rate' },
        { $Type: 'UI.ReferenceFacet', ID: 'CacheEfficiency',   Target: '@UI.DataPoint#cacheEfficiencyKPI' },
        { $Type: 'UI.ReferenceFacet', ID: 'Throughput',        Target: '@UI.DataPoint#throughputKPI' },
        { $Type: 'UI.ReferenceFacet', ID: 'Hits',              Target: '@UI.DataPoint#hitsKPI' },
        { $Type: 'UI.ReferenceFacet', ID: 'Misses',            Target: '@UI.DataPoint#missesKPI' }
    ],

    // ── Body: fully custom sections replace annotation-driven FieldGroups ──
    UI.Facets: [],

    UI.FieldGroup #ReadThrough: {
        Label: 'Read-Through Details',
        Data: [
            { $Type: 'UI.DataField', Value: hits,                  Label: 'Hits' },
            { $Type: 'UI.DataField', Value: misses,                Label: 'Misses' },
            { $Type: 'UI.DataField', Value: errors,                Label: 'Errors' },
            { $Type: 'UI.DataField', Value: totalRequests,         Label: 'Total Requests' },
            { $Type: 'UI.DataField', Value: hitRatio,              Label: 'Hit Ratio %' },
            { $Type: 'UI.DataField', Value: throughput,            Label: 'Throughput (req/s)' },
            { $Type: 'UI.DataField', Value: errorRate,             Label: 'Error Rate %' },
            { $Type: 'UI.DataField', Value: cacheEfficiency,       Label: 'Cache Efficiency (x)' },
            { $Type: 'UI.DataField', Value: avgReadThroughLatency, Label: 'Avg RT Latency (ms)' },
            { $Type: 'UI.DataField', Value: avgHitLatency,         Label: 'Avg Hit Latency (ms)' },
            { $Type: 'UI.DataField', Value: minHitLatency,         Label: 'Min Hit Latency (ms)' },
            { $Type: 'UI.DataField', Value: maxHitLatency,         Label: 'Max Hit Latency (ms)' },
            { $Type: 'UI.DataField', Value: avgMissLatency,        Label: 'Avg Miss Latency (ms)' },
            { $Type: 'UI.DataField', Value: minMissLatency,        Label: 'Min Miss Latency (ms)' },
            { $Type: 'UI.DataField', Value: maxMissLatency,        Label: 'Max Miss Latency (ms)' }
        ]
    },

    UI.FieldGroup #NativeOps: {
        Label: 'Native Function Details',
        Data: [
            { $Type: 'UI.DataField', Value: nativeSets,           Label: 'Sets' },
            { $Type: 'UI.DataField', Value: nativeGets,           Label: 'Gets' },
            { $Type: 'UI.DataField', Value: nativeDeletes,        Label: 'Deletes' },
            { $Type: 'UI.DataField', Value: nativeClears,         Label: 'Clears' },
            { $Type: 'UI.DataField', Value: nativeDeleteByTags,   Label: 'Delete By Tags' },
            { $Type: 'UI.DataField', Value: nativeErrors,         Label: 'Native Errors' },
            { $Type: 'UI.DataField', Value: totalNativeOperations,Label: 'Total Native Ops' },
            { $Type: 'UI.DataField', Value: nativeThroughput,     Label: 'Native Throughput (ops/s)' },
            { $Type: 'UI.DataField', Value: nativeErrorRate,      Label: 'Native Error Rate %' }
        ]
    }
);
