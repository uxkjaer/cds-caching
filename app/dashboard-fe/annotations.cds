using {plugin.cds_caching.CachingApiService as S} from '../../index';

// List Report
annotate S.Caches with @(
    Common.SemanticKey: [name],
    UI.SelectionFields: [
        name,
        metricsEnabled,
        keyMetricsEnabled
    ],
    UI.LineItem       : [
        {
            $Type: 'UI.DataField',
            Value: name
        },
        {
            $Type: 'UI.DataField',
            Value: metricsEnabled
        },
        {
            $Type: 'UI.DataField',
            Value: keyMetricsEnabled
        }
    ]
);

// Object Page header
annotate S.Caches with @(
    UI.HeaderInfo                 : {
        $Type         : 'UI.HeaderInfoType',
        TypeName      : 'Cache',
        TypeNamePlural: 'Caches',
        Title         : {
            $Type: 'UI.DataField',
            Value: name
        }
    },
    UI.DataPoint #metricsStatus   : {
        Value      : metricsEnabled,
        Title      : 'Metrics',
        Criticality: metricsStatus
    },
    UI.DataPoint #keyMetricsStatus: {
        Value      : keyMetricsEnabled,
        Title      : 'Key Metrics',
        Criticality: keyMetricsStatus
    },
    UI.HeaderFacets               : [
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
    UI.Identification             : [
        {
            $Type : 'UI.DataFieldForAction',
            Action: 'plugin.cds_caching.CachingApiService.toggleMetrics',
            Label : 'Toggle Metrics',
            @UI.Emphasized,
        },

        {
            $Type : 'UI.DataFieldForAction',
            Action: 'plugin.cds_caching.CachingApiService.toggleKeyMetrics',
            Label : 'Toggle Key Metrics',
            @UI.Emphasized,
        },
        {
            $Type      : 'UI.DataFieldForAction',
            Action     : 'plugin.cds_caching.CachingApiService.clear',
            Label      : 'Clear Cache',
            Criticality: #Negative,
        },


    ],
    UI.Facets                     : []
);

// ─── METRICS TABLE ───────────────────────────────────────────────────────────

annotate S.Metrics with @(
    Common.SemanticKey: [
        ID,
        cache
    ],
    UI.HeaderInfo     : {
        TypeName      : 'Metric',
        TypeNamePlural: 'Metrics',
        Title         : {Value: timestamp}
    },
    UI.LineItem       : [
        {
            $Type : 'UI.DataFieldForAction',
            Action: 'plugin.cds_caching.CachingApiService.clearMetrics',
            Label : 'Clear Metrics',
            @UI.Emphasized,
        },
        {
            $Type: 'UI.DataField',
            Value: timestamp
        },
        {
            $Type: 'UI.DataField',
            Value: period
        },
        {
            $Type: 'UI.DataField',
            Value: hits
        },
        {
            $Type: 'UI.DataField',
            Value: misses
        },
        {
            $Type: 'UI.DataField',
            Value: errors
        },
        {
            $Type: 'UI.DataField',
            Value: totalRequests
        },
        {
            $Type: 'UI.DataField',
            Value: hitRatio
        },
        {
            $Type: 'UI.DataField',
            Value: throughput
        },
        {
            $Type: 'UI.DataField',
            Value: errorRate
        },
        {
            $Type: 'UI.DataField',
            Value: cacheEfficiency
        },
        {
            $Type: 'UI.DataField',
            Value: avgReadThroughLatency
        },
        {
            $Type: 'UI.DataField',
            Value: avgHitLatency
        },
        {
            $Type: 'UI.DataField',
            Value: minHitLatency
        },
        {
            $Type: 'UI.DataField',
            Value: maxHitLatency
        },
        {
            $Type: 'UI.DataField',
            Value: avgMissLatency
        },
        {
            $Type: 'UI.DataField',
            Value: minMissLatency
        },
        {
            $Type: 'UI.DataField',
            Value: maxMissLatency
        },
        {
            $Type: 'UI.DataField',
            Value: nativeSets
        },
        {
            $Type: 'UI.DataField',
            Value: nativeGets
        },
        {
            $Type: 'UI.DataField',
            Value: nativeDeletes
        },
        {
            $Type: 'UI.DataField',
            Value: nativeClears
        },
        {
            $Type: 'UI.DataField',
            Value: nativeDeleteByTags
        },
        {
            $Type: 'UI.DataField',
            Value: nativeErrors
        },
        {
            $Type: 'UI.DataField',
            Value: totalNativeOperations
        },
        {
            $Type: 'UI.DataField',
            Value: nativeThroughput
        },
        {
            $Type: 'UI.DataField',
            Value: nativeErrorRate
        }
    ]
);

// ─── KEY METRICS TABLE ────────────────────────────────────────────────────────

annotate S.KeyMetrics with @(
    UI.HeaderInfo: {
        TypeName      : 'Key Metric',
        TypeNamePlural: 'Key Metrics',
        Title         : {Value: keyName}
    },
    UI.LineItem  : [
        {
            $Type            : 'UI.DataFieldForAction',
            Action           : 'plugin.cds_caching.CachingApiService.clearKeyMetrics',
            Label            : 'Clear Key Metrics',
            ![@UI.Emphasized]: true
        },
        {
            $Type: 'UI.DataField',
            Value: keyName
        },
        {
            $Type: 'UI.DataField',
            Value: operation
        },
        {
            $Type: 'UI.DataField',
            Value: dataType
        },
        {
            $Type: 'UI.DataField',
            Value: operationType
        },
        {
            $Type: 'UI.DataField',
            Value: target
        },
        {
            $Type: 'UI.DataField',
            Value: hits
        },
        {
            $Type: 'UI.DataField',
            Value: misses
        },
        {
            $Type: 'UI.DataField',
            Value: totalRequests
        },
        {
            $Type: 'UI.DataField',
            Value: hitRatio
        },
        {
            $Type: 'UI.DataField',
            Value: cacheEfficiency
        },
        {
            $Type: 'UI.DataField',
            Value: avgHitLatency
        },
        {
            $Type: 'UI.DataField',
            Value: minHitLatency
        },
        {
            $Type: 'UI.DataField',
            Value: maxHitLatency
        },
        {
            $Type: 'UI.DataField',
            Value: avgMissLatency
        },
        {
            $Type: 'UI.DataField',
            Value: minMissLatency
        },
        {
            $Type: 'UI.DataField',
            Value: maxMissLatency
        },
        {
            $Type: 'UI.DataField',
            Value: nativeHits
        },
        {
            $Type: 'UI.DataField',
            Value: nativeMisses
        },
        {
            $Type: 'UI.DataField',
            Value: nativeSets
        },
        {
            $Type: 'UI.DataField',
            Value: nativeDeletes
        },
        {
            $Type: 'UI.DataField',
            Value: tenant
        },
        {
            $Type: 'UI.DataField',
            Value: user
        },
        {
            $Type: 'UI.DataField',
            Value: locale
        },
        {
            $Type: 'UI.DataField',
            Value: lastAccess
        },
        {
            $Type: 'UI.DataField',
            Value: timestamp
        }
    ]
);

// ─── METRICS SUB-OBJECT PAGE ─────────────────────────────────────────────────

annotate S.Metrics with @(

    // ── DataPoints ── with CriticalityCalculation matching formatter.ts thresholds

    // Hit Ratio: Good ≥ 90, Critical ≥ 70, Error < 70 (Maximize)
    UI.DataPoint #hitRatio          : {
        Value                 : hitRatio,
        Title                 : 'Hit Ratio',
        TargetValue           : 100,
        CriticalityCalculation: {
            $Type                 : 'UI.CriticalityCalculationType',
            ImprovementDirection  : #Maximize,
            ToleranceRangeLowValue: 70,
            DeviationRangeLowValue: 90
        }
    },

    // Error Rate: Good ≤ 1%, Critical ≤ 5%, Error > 5% (Minimize)
    UI.DataPoint #errorRate         : {
        Value                 : errorRate,
        Title                 : 'Error Rate %',
        CriticalityCalculation: {
            $Type                  : 'UI.CriticalityCalculationType',
            ImprovementDirection   : #Minimize,
            ToleranceRangeHighValue: 1,
            DeviationRangeHighValue: 5
        }
    },

    // Cache Efficiency: Good > 5, Critical ≥ 3, Error < 3 (Maximize)
    UI.DataPoint #cacheEfficiencyKPI: {
        Value                 : cacheEfficiency,
        Title                 : 'Cache Efficiency',
        CriticalityCalculation: {
            $Type                 : 'UI.CriticalityCalculationType',
            ImprovementDirection  : #Maximize,
            ToleranceRangeLowValue: 3,
            DeviationRangeLowValue: 5
        }
    },

    // Throughput: plain KPI (no threshold defined in formatter)
    UI.DataPoint #throughputKPI     : {
        Value: throughput,
        Title: 'Throughput (req/s)'
    },
    UI.DataPoint #hitsKPI           : {
        Value: hits,
        Title: 'Hits'
    },
    UI.DataPoint #missesKPI         : {
        Value: misses,
        Title: 'Misses'
    },

    // Latency: Good ≤ 10ms, Critical ≤ 50ms, Error > 50ms (Minimize)
    UI.DataPoint #avgHitLatencyDP   : {
        Value                 : avgHitLatency,
        Title                 : 'Avg Hit Latency',
        CriticalityCalculation: {
            $Type                  : 'UI.CriticalityCalculationType',
            ImprovementDirection   : #Minimize,
            ToleranceRangeHighValue: 10,
            DeviationRangeHighValue: 50
        }
    },
    UI.DataPoint #avgMissLatencyDP  : {
        Value                 : avgMissLatency,
        Title                 : 'Avg Miss Latency',
        CriticalityCalculation: {
            $Type                  : 'UI.CriticalityCalculationType',
            ImprovementDirection   : #Minimize,
            ToleranceRangeHighValue: 10,
            DeviationRangeHighValue: 50
        }
    },
    UI.DataPoint #avgRTLatencyDP    : {
        Value                 : avgReadThroughLatency,
        Title                 : 'Avg RT Latency',
        CriticalityCalculation: {
            $Type                  : 'UI.CriticalityCalculationType',
            ImprovementDirection   : #Minimize,
            ToleranceRangeHighValue: 10,
            DeviationRangeHighValue: 50
        }
    },

    UI.DataPoint #nativeThroughputDP: {
        Value: nativeThroughput,
        Title: 'Native Throughput'
    },
    UI.DataPoint #nativeErrorRateDP : {
        Value                 : nativeErrorRate,
        Title                 : 'Native Error Rate %',
        CriticalityCalculation: {
            $Type                  : 'UI.CriticalityCalculationType',
            ImprovementDirection   : #Minimize,
            ToleranceRangeHighValue: 1,
            DeviationRangeHighValue: 5
        }
    },

    // ── Charts ──

    // Header: Donut for hit ratio
    UI.Chart #hitRatioChart         : {
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
    UI.Chart #errorRateChart        : {
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
    UI.Chart #avgHitLatencyChart    : {
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
    UI.Chart #avgMissLatencyChart   : {
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
    UI.Chart #avgRTLatencyChart     : {
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
    UI.Chart #nativeThroughputChart : {
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
    UI.Chart #nativeErrorRateChart  : {
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
    UI.HeaderFacets                 : [
        {
            $Type : 'UI.ReferenceFacet',
            ID    : 'HitRatioChart',
            Target: '@UI.Chart#hitRatioChart'
        },
        {
            $Type : 'UI.ReferenceFacet',
            ID    : 'ErrorRateChart',
            Target: '@UI.Chart#errorRateChart'
        },
        {
            $Type : 'UI.ReferenceFacet',
            ID    : 'CacheEfficiency',
            Target: '@UI.DataPoint#cacheEfficiencyKPI'
        },
        {
            $Type : 'UI.ReferenceFacet',
            ID    : 'Throughput',
            Target: '@UI.DataPoint#throughputKPI'
        },
        {
            $Type : 'UI.ReferenceFacet',
            ID    : 'Hits',
            Target: '@UI.DataPoint#hitsKPI'
        },
        {
            $Type : 'UI.ReferenceFacet',
            ID    : 'Misses',
            Target: '@UI.DataPoint#missesKPI'
        }
    ],

    // ── Body: fully custom sections replace annotation-driven FieldGroups ──
    UI.Facets                       : [],

    UI.FieldGroup #ReadThrough      : {
        Label: 'Read-Through Details',
        Data : [
            {
                $Type: 'UI.DataField',
                Value: hits
            },
            {
                $Type: 'UI.DataField',
                Value: misses
            },
            {
                $Type: 'UI.DataField',
                Value: errors
            },
            {
                $Type: 'UI.DataField',
                Value: totalRequests
            },
            {
                $Type: 'UI.DataField',
                Value: hitRatio
            },
            {
                $Type: 'UI.DataField',
                Value: throughput
            },
            {
                $Type: 'UI.DataField',
                Value: errorRate
            },
            {
                $Type: 'UI.DataField',
                Value: cacheEfficiency
            },
            {
                $Type: 'UI.DataField',
                Value: avgReadThroughLatency
            },
            {
                $Type: 'UI.DataField',
                Value: avgHitLatency
            },
            {
                $Type: 'UI.DataField',
                Value: minHitLatency
            },
            {
                $Type: 'UI.DataField',
                Value: maxHitLatency
            },
            {
                $Type: 'UI.DataField',
                Value: avgMissLatency
            },
            {
                $Type: 'UI.DataField',
                Value: minMissLatency
            },
            {
                $Type: 'UI.DataField',
                Value: maxMissLatency
            }
        ]
    },

    UI.FieldGroup #NativeOps        : {
        Label: 'Native Function Details',
        Data : [
            {
                $Type: 'UI.DataField',
                Value: nativeSets
            },
            {
                $Type: 'UI.DataField',
                Value: nativeGets
            },
            {
                $Type: 'UI.DataField',
                Value: nativeDeletes
            },
            {
                $Type: 'UI.DataField',
                Value: nativeClears
            },
            {
                $Type: 'UI.DataField',
                Value: nativeDeleteByTags
            },
            {
                $Type: 'UI.DataField',
                Value: nativeErrors
            },
            {
                $Type: 'UI.DataField',
                Value: totalNativeOperations
            },
            {
                $Type: 'UI.DataField',
                Value: nativeThroughput
            },
            {
                $Type: 'UI.DataField',
                Value: nativeErrorRate
            }
        ]
    }
);
