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
