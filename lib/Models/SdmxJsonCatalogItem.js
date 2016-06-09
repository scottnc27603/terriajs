'use strict';

/*global require*/
var URI = require('urijs');

var clone = require('terriajs-cesium/Source/Core/clone');
var defaultValue = require('terriajs-cesium/Source/Core/defaultValue');
var defined = require('terriajs-cesium/Source/Core/defined');
var defineProperties = require('terriajs-cesium/Source/Core/defineProperties');
var DeveloperError = require('terriajs-cesium/Source/Core/DeveloperError');
var freezeObject = require('terriajs-cesium/Source/Core/freezeObject');
var deprecationWarning = require('terriajs-cesium/Source/Core/deprecationWarning');
var knockout = require('terriajs-cesium/Source/ThirdParty/knockout');
var loadJson = require('terriajs-cesium/Source/Core/loadJson');
var loadText = require('terriajs-cesium/Source/Core/loadText');
var objectToQuery = require('terriajs-cesium/Source/Core/objectToQuery');
var when = require('terriajs-cesium/Source/ThirdParty/when');

var AbsConcept = require('../Map/AbsConcept');
var arrayProduct = require('../Core/arrayProduct');
var arraysAreEqual = require('../Core/arraysAreEqual');
var CsvCatalogItem = require('./CsvCatalogItem');
var inherit = require('../Core/inherit');
var TerriaError = require('../Core/TerriaError');
var overrideProperty = require('../Core/overrideProperty');
var proxyCatalogItemUrl = require('./proxyCatalogItemUrl');
var RegionMapping = require('../Models/RegionMapping');
var TableColumn = require('../Map/TableColumn');
var TableStructure = require('../Map/TableStructure');
var VarSubType = require('../Map/VarSubType');
var VarType = require('../Map/VarType');

var allowedRegionCodes = ['AUS', 'STE', 'SA4', 'SA3', 'SA2', 'SA1', 'CED', 'LGA', 'POA', 'SED']; // These should be made a parameter.

/*
    The SDMX-JSON format.
    Descriptions of this format are available at:
    - https://data.oecd.org/api/sdmx-json-documentation/
    - https://github.com/sdmx-twg/sdmx-json/tree/master/data-message/docs
    - https://sdmx.org/

    Given an endpoint, such as  http://stats.oecd.org/sdmx-json/ (which incidentally hosts a handy query builder).

    Currently, only ever uses the first "dataSet" object provided. (This covers all situations of interest to us so far.)

    The dimension names and codes come from (in json format):
    http://stats.oecd.org/sdmx-json/dataflow/<dataset id> (eg. QNA).

    Then access:
      - result.structure.dimensions.observation[k] for {keyPosition, id, name, values[]} to get the name & id of dimension keyPosition and its array of allowed values (with {id, name}).
      - result.structure.dimensions.attributes.dataSet has some potentially interesting things such as units, unit multipliers, reference periods (eg. http://stats.oecd.org/sdmx-json/dataflow/QNA).
      - result.structure.dimensions.attributes.observation has some potentially interesting things such as time formats and status (eg. estimated value, forecast value).

    (Alternatively, in xml format):
    http://stats.oecd.org/restsdmx/sdmx.ashx/GetDataStructure/<dataset id> (eg. QNA).

    Data comes from:
    http://stats.oecd.org/sdmx-json/data/<dataset identifier>/<filter expression>/<agency name>[ ?<additional parameters>]
    
    Eg.
    http://stats.oecd.org/sdmx-json/data/QNA/AUS+AUT.GDP+B1_GE.CUR+VOBARSA.Q/all?startTime=2009-Q2&endTime=2011-Q4

    An example from the ABS:
    http://stat.abs.gov.au/sdmx-json/data/ABS_REGIONAL_LGA/CABEE_2.LGA2013.1+.A/all?startTime=2013&endTime=2013

    Then access:
      - result.structure.dimensions.series[i] for {keyPosition, id, name, values[]} to get the name & id of dimension keyPosition and its array of allowed values (with {id, name}).
      - result.structure.dimensions.observation[i] for {keyPosition, id, name, values[]} to get the name & id of dimension keyPosition and its array of allowed values (with {id, name}).
      - result.dataSets[0].series[key].observations[t][0] with key = "xx.yy.zz" where xx is the id of a value from dimension 0, etc, and t is the time index (eg. 0 for a single time).

 */

/**
 * A {@link CatalogItem} representing region-mapped data obtained from SDMX-JSON format.
 *
 * @alias SdmxJsonCatalogItem
 * @constructor
 * @extends CsvCatalogItem
 *
 * @param {Terria} terria The Terria instance.
 * @param {String} [url] The base URL from which to retrieve the data.
 */
var SdmxJsonCatalogItem = function(terria, url) {
    CsvCatalogItem.call(this, terria, url);

    /**
     * Gets or sets the ID of the dataset.
     * This property is observable.
     * @type {String}
     */
    this.datasetId = undefined;

    // An internal record of how many columns are region- or value-columns. These are always the first columns.
    // When the active columns change, we only keep the region and value columns, and regenerate a new total column at the end.
    this._numberOfNonTotalColumns = undefined;

    /**
     * Gets or sets whether this item can show percentages instead of raw values.  This property is observable.
     * @type {Boolean}
     * @default true
     */
    this.canDisplayPercent = true;

    /**
     * Gets or sets whether to show percentages or raw values.  This property is observable.
     * @type {Boolean}
     * @default true
     */
    this.displayPercent = true;

    /**
     * Gets or sets the SDMX region-type dimension id used with the region code to set the region type.
     * Usually defaults to 'REGIONTYPE'.
     * This property is observable.
     * @type {String}
     */
    this.regionTypeDimensionId = undefined;

    /**
     * Gets or sets the SDMX region dimension id. Defaults to 'REGION'.
     * This property is observable.
     * @type {String}
     */
    this.regionDimensionId = undefined;

    /**
     * Gets or sets the SDMX frequency dimension id, which is ignored. Defaults to 'FREQUENCY'.
     * This property is observable.
     * @type {String}
     */
    this.frequencyDimensionId = undefined;

    /**
     * Gets or sets the URL of a JSON file containing human-readable names of Australian Bureau of Statistics concept codes.
     * @type {String}
     */
    this.conceptNamesUrl = undefined;

    /**
     * Gets or sets the start of a URL of a csv file containing the total number of people in each region, eg.
     * SA4,Tot_P_M,Tot_P_F,Tot_P_P
     * 101,100000,23450,123450
     * 102,130000,100000,234560
     * The region code and '.csv' are appended to the end of this URL for the request, eg.
     * 'data/2011Census_TOT_' -> 'data/2011Census_TOT_SA4.csv' (and other region types).
     * @type {String}
     */
    this.regionPopulationsUrlPrefix = undefined;

    /**
     * Gets the list of initial concepts and codes on which to filter the data.  You can obtain a list of all available
     * concepts for a dataset by querying http://stat.abs.gov.au/itt/query.jsp?method=GetDatasetConcepts&datasetid=ABS_CENSUS2011_B25
     * (or equivalent) and a list of the possible values for a concept by querying
     * http://stat.abs.gov.au/itt/query.jsp?method=GetCodeListValue&datasetid=ABS_CENSUS2011_B25&concept=MEASURE&format=json.
     * @type {Array}
     * @editorformat table
     */
    this.filter = [];

    /**
     * Gets or sets the array of concept ids which should not be loaded.
     * Defaults to ['STATE', 'FREQUENCY', and the region concept (which defaults to 'REGION')].
     * @type {String[]}
     */
    this.conceptsNotToLoad = undefined;

    /**
     * Gets or sets the array of ids of concepts which are single-valued, in addition to the region type.
     * @type {String[]}
     */
    this.uniqueValuedConcepts = [];

    this._baseUrl = undefined;
    // These contain raw downloaded data used during the loading process.
    this._conceptIds = undefined;
    this._conceptNamesMap = undefined;
    this._conceptCodes = [];
    // These contain cached promises
    this._loadConceptsPromise = undefined;
    this._loadConceptIdsAndNameMapPromise = undefined;
    this._loadDataFilePromise = {}; // cached promises resolving to loaded data files, keyed by the 'and' parameter.

    // The array of AbsConcepts to display in the NowViewing panel.
    this._concepts = [];

    // Tracking _concepts makes this a circular object.
    // _concepts (via concepts) is both set and read in rebuildData.
    // A solution to this would be to make concepts a Promise, but that would require changing the UI side.
    knockout.track(this, ['datasetId', 'displayPercent', 'canDisplayPercent', '_concepts']);

    // overrideProperty(this, 'concepts', {
    //     get: function() {
    //         return this._concepts;
    //     }
    // });

    // knockout.getObservable(this, 'displayPercent').subscribe(rebuildData.bind(null, this), this);

};

inherit(CsvCatalogItem, SdmxJsonCatalogItem);

defineProperties(SdmxJsonCatalogItem.prototype, {
    /**
     * Gets the type of data member represented by this instance.
     * @memberOf SdmxJsonCatalogItem.prototype
     * @type {String}
     */
    type: {
        get: function() {
            return 'sdmx-json';
        }
    },

    /**
     * Gets a human-readable name for this type of data source, 'GPX'.
     * @memberOf SdmxJsonCatalogItem.prototype
     * @type {String}
     */
    typeName: {
        get: function() {
            return 'SDMX JSON';
        }
    },

    /**
     * Gets the set of names of the properties to be serialized for this object for a share link.
     * @memberOf ImageryLayerCatalogItem.prototype
     * @type {String[]}
     */
    propertiesForSharing: {
        get: function() {
            return SdmxJsonCatalogItem.defaultPropertiesForSharing;
        }
    },

    /**
     * Gets the set of functions used to serialize individual properties in {@link CatalogMember#serializeToJson}.
     * When a property name on the model matches the name of a property in the serializers object lieral,
     * the value will be called as a function and passed a reference to the model, a reference to the destination
     * JSON object literal, and the name of the property.
     * @memberOf SdmxJsonCatalogItem.prototype
     * @type {Object}
     */
    serializers: {
        get: function() {
            return SdmxJsonCatalogItem.defaultSerializers;
        }
    }
});

/**
 * Gets or sets the default set of properties that are serialized when serializing a {@link CatalogItem}-derived for a
 * share link.
 * @type {String[]}
 */
SdmxJsonCatalogItem.defaultPropertiesForSharing = clone(CsvCatalogItem.defaultPropertiesForSharing);
SdmxJsonCatalogItem.defaultPropertiesForSharing.push('filter');
SdmxJsonCatalogItem.defaultPropertiesForSharing.push('regionDimensionId');
SdmxJsonCatalogItem.defaultPropertiesForSharing.push('regionTypeDimensionId');
SdmxJsonCatalogItem.defaultPropertiesForSharing.push('displayPercent');
freezeObject(SdmxJsonCatalogItem.defaultPropertiesForSharing);

SdmxJsonCatalogItem.defaultSerializers = clone(CsvCatalogItem.defaultSerializers);

SdmxJsonCatalogItem.defaultSerializers.filter = function(item, json) {
    // Create the 'filter' that would start us off with the same active items as are currently shown.
    var nestedFilter = item._concepts.map(function(concept) {
        return concept.toFilter();
    });
    json.filter = nestedFilter.reduce(function(a, b) {
        return a.concat(b);
    }, []);
};

freezeObject(SdmxJsonCatalogItem.defaultSerializers);

//Just the items that would influence the load from the abs server or the file
SdmxJsonCatalogItem.prototype._getValuesThatInfluenceLoad = function() {
    return [this.url, this.datasetId];
};


SdmxJsonCatalogItem.prototype._load = function() {
    var that = this;
    // Set some defaults.
    this._baseUrl = cleanAndProxyUrl(this, this.url);
    // this.conceptNamesUrl = defaultValue(this.conceptNamesUrl, 'data/abs_names.json');
    // this.regionPopulationsUrlPrefix = defaultValue(this.regionPopulationsUrlPrefix, 'data/2011Census_TOT_');
    this.regionTypeDimensionId = defaultValue(this.regionTypeDimensionId, 'REGIONTYPE');
    this.regionDimensionId = defaultValue(this.regionDimensionId, 'REGION');
    this.frequencyDimensionId = defaultValue(this.frequencyDimensionId, 'FREQUENCY');
    // this.conceptsNotToLoad = ['STATE', 'FREQUENCY', this.regionConcept]; // Nicer to make this an updateable parameter.

    return loadAndBuildTable(that);
};

// // TODO: Sometimes these errors do not actually reach the end user, because they are thrown inside a promise.
// function throwLoadError(item, methodName) {
//     throw new TerriaError({
//         sender: item,
//         title: 'Item is not available',
//         message: '\
// An error occurred while invoking ' + methodName + ' on the ABS ITT server.  \
// <p>This error may indicate that the item you opened is temporarily unavailable or there is a \
// problem with your internet connection.  Try opening the group again, and if the problem persists, please report it by \
// sending an email to <a href="mailto:' + item.terria.supportEmail + '">' + item.terria.supportEmail + '</a>.</p>'
//     });
// }

// function throwDataColumnsError(item, columnNames) {
//     throw new TerriaError({
//         sender: item,
//         title: 'Unexpected data format',
//         message: '\
// An error occurred while invoking GetGenericData on the ABS ITT server: the data format has changed. \
// <p>Please report this error by \
// sending an email to <a href="mailto:' + item.terria.supportEmail + '">' + item.terria.supportEmail + '</a>.</p>'
//     });
// }

// function throwDataMergeError(item) {
//     throw new TerriaError({
//         sender: item,
//         title: 'Data cannot be merged',
//         message: '\
// An error occurred while invoking GetGenericData on the ABS ITT server: the individual data files cannot be merged. \
// <p>Please report this error by \
// sending an email to <a href="mailto:' + item.terria.supportEmail + '">' + item.terria.supportEmail + '</a>.</p>'
//     });
// }

// /**
//  * Returns a promise which, when resolved, indicates that item._conceptIds and item._conceptNamesMap are loaded.
//  * @private
//  * @param  {SdmxJsonCatalogItem} item This catalog item.
//  * @return {Promise} Promise which, when resolved, indicates that item._conceptIds and item._conceptNamesMap are loaded.
//  */
// function loadConceptIdsAndConceptNameMap(item) {
//     if (!defined(item._loadConceptIdsAndNameMapPromise)) {
//         var parameters = {
//             method: 'GetDatasetConcepts',
//             datasetid: item.datasetId,
//             format: 'json'
//         };
//         var datasetConceptsUrl = item._baseUrl + '?' + objectToQuery(parameters);
//         var loadDatasetConceptsPromise = loadJson(datasetConceptsUrl).then(function(json) {
//             item._conceptIds = json.concepts;
//             if (json.concepts.indexOf(item.regionConcept) === -1 || json.concepts.indexOf('REGIONTYPE') === -1) {
//                 throw new DeveloperError('datasetId ' + item.datasetId + ' concepts [' + json.concepts.join(', ') + '] do not include "' + item.regionConcept +'" and "REGIONTYPE".');
//             }
//         }).otherwise(throwLoadError.bind(null, item, 'GetDatasetConcepts'));
//         var loadConceptNamesPromise = loadJson(item.conceptNamesUrl).then(function(json) {
//             item._conceptNamesMap = json;
//         });

//         item._loadConceptIdsAndNameMapPromise = when.all([loadConceptNamesPromise, loadDatasetConceptsPromise]);
//         // item.concepts and item.conceptNameMap are now defined with the results.
//     }
//     return item._loadConceptIdsAndNameMapPromise;
// }

// eg. range(5) returns [0, 1, 2, 3, 4].
function range(length) {
    return Array.apply(null, Array(length)).map(function(x, i) { return i; });
}

function calculateDimensionNamesAndValues(structureSeries) {
    // Store the length of each dimension, in the correct keyPosition.
    var result = {
        values: [],
        names: []
    };
    for (var i = 0; i < structureSeries.length; i++) {
        result.values.push([]);
        result.names.push([]);
    }
    for (i = 0; i < structureSeries.length; i++) {
        var thisSeries = structureSeries[i];
        // eg. thisSeries.values may be [{id: "BD_2", name: "Births"}, {id: "BD_4", name: "Deaths"}].
        // We convert this to [0, 1] using range(length).
        result.values[thisSeries.keyPosition] = range(thisSeries.values.length);
        if (thisSeries.values.length > 1) {
            // Don't include the name of this dimension if it can only take a single value.
            result.names[thisSeries.keyPosition] = thisSeries.values.map(function(nameAndId) { return nameAndId.name; });
        } else {
            result.names[thisSeries.keyPosition] = [''];
        }
    }
    if (result.values.indexOf(undefined) >= 0) {
        // TODO: Raise an error properly.
        console.log('Missing dimension at key position ' + result.values.indexOf(undefined));
    }
    return result;
}

function calculateNonRegionDimensionCombinations(structureSeries, regionInfo) {
    // Restrict attention to the first region type, first region and first frequency only. Don't include their names.
    var dimensionInfo = calculateDimensionNamesAndValues(structureSeries);
    if (defined(regionInfo.regionDimensionIndex)) {
        dimensionInfo.values[regionInfo.regionDimensionIndex] = dimensionInfo.values[regionInfo.regionDimensionIndex].slice(0, 1);
        dimensionInfo.names[regionInfo.regionDimensionIndex] = [''];
    }
    if (defined(regionInfo.regionTypeDimensionIndex)) {
        dimensionInfo.values[regionInfo.regionTypeDimensionIndex] = dimensionInfo.values[regionInfo.regionTypeDimensionIndex].slice(0, 1);
        dimensionInfo.names[regionInfo.regionTypeDimensionIndex] = [''];
    }
    if (defined(regionInfo.frequencyDimensionIndex)) {
        dimensionInfo.values[regionInfo.frequencyDimensionIndex] = dimensionInfo.values[regionInfo.frequencyDimensionIndex].slice(0, 1);
        dimensionInfo.names[regionInfo.frequencyDimensionIndex] = [''];
    }
    // Convert the values into all the combinations we'll need to load into columns,
    // eg. [[0], [0], [0, 1, 2], [0, 1]] =>
    //     [[0, 0, 0, 0], [0, 0, 0, 1], [0, 0, 1, 0], [0, 0, 1, 1], [0, 0, 2, 0], [0, 0, 2, 1]].
    return {
        values: arrayProduct(dimensionInfo.values),
        names: arrayProduct(dimensionInfo.names)
    };
}

function calculateRegionDimensionInfo(item, structureSeries) {
    var result = {
        regionDimensionIndex: undefined,
        regionTypeDimensionIndex: undefined,
        regionCount: 0,
        regionTypeCount: 0
    };
    // Assume only one regiontype's regions are provided. (Is this a good assumption?)
    for (var i = 0; i < structureSeries.length; i++) {
        if (structureSeries[i].id === item.regionDimensionId) {
            result.regionDimensionIndex = structureSeries[i].keyPosition;
            result.regionCount = structureSeries[i].values.length;
        } else if (structureSeries[i].id === item.regionTypeDimensionId) {
            result.regionTypeDimensionIndex = structureSeries[i].keyPosition;
            result.regionTypeCount = structureSeries[i].values.length;
        }
    }
    return result;
}

function buildRegionColumn(structureSeries, regionInfo) {
    var regions = [];
    for (var i = 0; i < regionInfo.regionCount; i++) {
        regions.push(structureSeries[regionInfo.regionDimensionIndex].values[i].id);
    }
    var regionColumnName = 'lga_code_2013'; // TODO: for now
    var regionColumn = new TableColumn(regionColumnName, regions);
    return regionColumn;
}

// Create a column for each combination of (non-region) dimension values.
// The column has values for each region.
function buildValueColumns(item, structureSeries, series, regionInfo) {
    // This lets us loop through a single for loop, rather than a dynamically-determined number of nested for loops.
    var nonRegionDimensionCombinations = calculateNonRegionDimensionCombinations(structureSeries, regionInfo);
    var columns = [];
    for (var combinationIndex = 0; combinationIndex < nonRegionDimensionCombinations.values.length; combinationIndex++) {
        var dimensionIndices = nonRegionDimensionCombinations.values[combinationIndex];
        // The name is just the joined names of all the columns involved, 
        var dimensionName = nonRegionDimensionCombinations.names[combinationIndex].filter(function(name) {return !!name; }).join(' ');
        var values = [];
        for (var regionIndex = 0; regionIndex < regionInfo.regionCount; regionIndex++) {
            dimensionIndices[regionInfo.regionDimensionIndex] = regionIndex;
            var key = dimensionIndices.join(':');
            values.push(defined(series[key]) ? series[key].observations[0][0] : null);
        }
        var column = new TableColumn(dimensionName, values);
        columns.push(column);
    }
    return columns;
}

// Create columns for the total (and possibly total percentage) values.
// If <=1 active column, returns [].
function buildTotalColumns(item, columns) {
    // If more than one column is active, add the values together.
    var activeColumns = columns.filter(function(column) { return column.isActive; });
    if (activeColumns.length > 1) {
        return new TableColumn('Total selected', TableColumn.sumValues(activeColumns));
    }
    return [];
}

// Called when the active column changes.
function changedActiveItems(item) {
    var columns = item._tableStructure.columns.slice(0, item._numberOfNonTotalColumns);
    columns.push(buildTotalColumns(item, columns));
    item._tableStructure.columns = columns;
}

// Among other things, rebuildData is triggered when the active items are changed.
// This leads to double-triggering on regions: when the old item is deactivated, and when the new one is activated.
// Since loads are cached, it isn't a problem. It would be nice to find a way around it though.
function loadAndBuildTable(item) {
    item._tableStructure = new TableStructure(item.name, {displayDuration: item._tableStyle.displayDuration});
    item._regionMapping = new RegionMapping(item, item._tableStructure, item._tableStyle);
    // This should apply to the concepts, not the table.
    // knockout.getObservable(item._tableStructure, 'activeItems').subscribe(changedActiveItems.bind(null, item), item);

    if (!defined(item._regionMapping)) {
        // This can happen when you open a shared URL with displayRegionPercent defined, since that has a ko subscription above.
        return when();
    }
    item._regionMapping.isLoading = true;

    return loadJson(item.url).then(function(json) {
        var structureSeries = json.structure.dimensions.series;
        var series = json.dataSets[0].series;

        var regionInfo = calculateRegionDimensionInfo(item, structureSeries);
        if (regionInfo.regionDimensionIndex < 0) {
            // TODO: Raise an error, or handle case when there are no regions.
            return;
        }

        var regionColumn = buildRegionColumn(structureSeries, regionInfo);
        var valueColumns = buildValueColumns(item, structureSeries, series, regionInfo);
        var totalColumns = buildTotalColumns(item, valueColumns); // The region column can't be active, so ok not to pass it.
        item._numberOfNonTotalColumns = 1 + valueColumns.length;
        var columns = [regionColumn].concat(valueColumns, totalColumns);

        columns[columns.length - 1].isActive = true;

        item._tableStructure.columns = columns;

        if (item._tableStructure.columns.length === 0) {
            // Nothing to show, so the attempt to redraw will fail; need to explicitly hide the existing regions.
            item._regionMapping.hideImageryLayer();
            item.terria.currentViewer.notifyRepaintRequired();
        }
        return item._regionMapping.loadRegionDetails();

    }).then(function(regionDetails) {
        // Can get here with undefined region column name, hence no regionDetails.
        if (regionDetails) {
            RegionMapping.setRegionColumnType(regionDetails);
            // Force a recalc of the imagery.
            // Required because we load the region details _after_ setting the active column.
            item._regionMapping.isLoading = false;
        }
        return when();
    });

    // - result.structure.dimensions.series[i] for {keyPosition, id, name, values[]} to get the name & id of dimension keyPosition and its array of allowed values (with {id, name}).
    // - result.dataSets[0].series[key].observations[t][0] with key = "xx.yy.zz" where xx is the id of a value from dimension 0, etc, and t is the time index (eg. 0 for a single time).


    // OLD
 
    // return loadConceptIdsAndConceptNameMap(item).then(function() {
    //     return loadConcepts(item);
    // }).then(function() {
    //     return loadDataFiles(item);
    // }).then(function(tableStructuresAndCombinations) {
    //     item._tableStructure.columns = buildTableColumns(item, tableStructuresAndCombinations);
    //     if (item._tableStructure.columns.length === 0) {
    //         // Nothing to show, so the attempt to redraw will fail; need to explicitly hide the existing regions.
    //         item._regionMapping.hideImageryLayer();
    //         item.terria.currentViewer.notifyRepaintRequired();
    //     }
    //     return item._regionMapping.loadRegionDetails();
    // }).then(function(regionDetails) {
    //     // Can get here with undefined region column name, hence no regionDetails.
    //     if (regionDetails) {
    //         RegionMapping.setRegionColumnType(regionDetails);
    //         // Force a recalc of the imagery.
    //         // Required because we load the region details _after_ setting the active column.
    //         item._regionMapping.isLoading = false;
    //     }
    //     return when();
    // });
}

// /**
//  * Loads concept codes.
//  * As they are loaded, each is processed into a tree of AbsCodes under an AbsConcept.
//  * Returns a promise which, when resolved, indicates that item._concepts is complete.
//  * The promise is cached, since the promise won't ever change for a given datasetId.
//  * @private
//  * @param  {SdmxJsonCatalogItem} item This catalog item.
//  * @return {Promise} Promise.
//  */
// function loadConcepts(item) {
//     if (!defined(item._loadConceptsPromise)) {
//         var absConcepts = [];
//         var promises = item._conceptIds.filter(function(conceptId) {
//             return (item.conceptsNotToLoad.indexOf(conceptId) === -1);
//         }).map(function(conceptId) {
//             var parameters = {
//                 method: 'GetCodeListValue',
//                 datasetid: item.datasetId,
//                 concept: conceptId,
//                 format: 'json'
//             };
//             var conceptCodesUrl = item._baseUrl + '?' + objectToQuery(parameters);

//             return loadJson(conceptCodesUrl).then(function(json) {
//                 // If this is a region type, only include valid region codes (eg. AUS, SA4, but not GCCSA).
//                 // Valid region codes must have a total population data file, eg. data/2011Census_TOT_SA4.csv
//                 var codes = json.codes;
//                 if (conceptId === item.regionTypeConcept) {
//                     codes = codes.filter(function(absCode) { return allowedRegionCodes.indexOf(absCode.code) >= 0; });
//                 }
//                 // We have loaded the file, process it into an AbsConcept.
//                 var concept = new AbsConcept({
//                     id: conceptId,
//                     codes: codes,
//                     filter: item.filter,
//                     allowMultiple: !(conceptId === item.regionTypeConcept || item.uniqueValuedConcepts.indexOf(conceptId) >= 0),
//                     activeItemsChangedCallback: function() {
//                         // Close any picked features, as the description of any associated with this catalog item may change.
//                         item.terria.pickedFeatures = undefined;
//                         rebuildData(item);
//                     }
//                 });
//                 // Give the concept its human-readable name.
//                 concept.name = getHumanReadableConceptName(item._conceptNamesMap, concept);
//                 absConcepts.push(concept);
//             }).otherwise(throwLoadError.bind(null, item, 'GetCodeListValue'));
//         });

//         item._loadConceptsPromise = when.all(promises).then(function() {
//             // All the AbsConcept objects have been created, we just need to order them correctly and save them.
//             // Put the region type concept first.
//             var makeFirst = item.regionTypeConcept;
//             absConcepts.sort(function(a, b) {
//                 return (a.id === makeFirst) ? -1 : ((b.id === makeFirst) ? 1 : (a.name > b.name ? 1 : -1));
//             });
//             item._concepts = absConcepts;
//         });
//     }
//     return item._loadConceptsPromise;
// }

// /**
//  * Given a concept object with name and possibly items properties, return its human-readable version.
//  * @private
//  * @param  {Object} conceptNameMap An object whose keys are the concept.names, eg. "ANCP".
//  *         Values may be Strings (eg. "Ancestry"), or
//  *         a 'code map' (eg. "MEASURE" : {"Persons": "Sex", "85 years and over": "Age", "*": "Measure"}.
//  * @param  {AbsConcept} concept An object with a name property and, if a codemap is to be used, an items array of objects with a name property.
//  *         In that case, it finds the first of those names to appear as a key in the code map. The value of this property is returned. (Phew!)
//  * @return {String} Human-readable concept name.
//  */
// function getHumanReadableConceptName(conceptNameMap, concept) {
//     if (!defined(conceptNameMap[concept.name])) {
//         return concept.name; // Default to the name given in the file.
//     }
//     if (typeof conceptNameMap[concept.name] === 'string') {
//         return conceptNameMap[concept.name];
//     } else {
//         var codeMap = conceptNameMap[concept.name];
//         for (var j = 0; j < concept.items.length; j++) {
//             if (defined(codeMap[concept.items[j].name])) {
//                 return codeMap[concept.items[j].name];
//             }
//         }
//         // Use the wildcard, if defined, or else fall back to the name in the file.
//         return codeMap['*'] || concept.name;
//     }
// }

// // Returns the active regiontype code, eg. SA4, or undefined if none _or more than one_ active.
// function getActiveRegionTypeCode(item) {
//     // We always put the region first, and at most one is active.
//     var activeRegions = item._concepts[0].activeItems;
//     if (activeRegions.length === 1) {
//         return activeRegions[0].code;
//     }
// }


// function loadTableStructure(item, csvString) {
//     var tableStructure = new TableStructure('ABS_ITT');
//     tableStructure.loadFromCsv(csvString);
//     return tableStructure;
// }


// /**
//  * Loads all the datafiles for this catalog item, given the active concepts.
//  * @private
//  * @param  {SdmxJsonCatalogItem} item The SdmxJsonCatalogItem instance.
//  * @return {Promise} A Promise which resolves to an object of TableStructures for each loaded dataset,
//  * with the total populations as the final one; and the active combinations.
//  */
// function loadDataFiles(item) {
//     // An array of arrays, indexed by activeItemsPerConcept[conceptIndex][codeIndex].
//     var activeCodesPerConcept = item._concepts.map(function(concept) {
//         return concept.activeItems;
//     });

//     // If any one of the concepts has no active selection, there will be no files to load.
//     for (var i = activeCodesPerConcept.length - 1; i >= 0; i--) {
//         if (activeCodesPerConcept[i].length === 0) {
//             return when();
//         }
//     }

//     // If there is no valid region selected (including when there are two!), stop.
//     if (!defined(getActiveRegionTypeCode(item))) {
//         return when();
//     }

//     // Find every possible combination, taking a single code from each concept.
//     var activeCombinations = SdmxJsonCatalogItem._generateAllCombinations(activeCodesPerConcept);

//     // Construct the 'and' part of the requests by combining concept.id and the AbsCode.name,
//     // eg. and=REGIONTYPE.SA4,AGE.A04,MEASURE.3
//     var andParameters = activeCombinations.map(function(codes) {
//         return codes.map(function(code, index) {
//             return code.concept.id + '.' + code.code;
//         }).join(',');
//     });

//     var loadDataPromises = andParameters.map(function(andParameter) {
//         // Build a promise which resolves once this datafile has loaded (unless we have cached this promise already).
//         // Note in the original there was different cacheing stuff... but not sure if it was being used?
//         if (!defined(item._loadDataFilePromise[andParameter])) {
//             var parameters = {
//                 method: 'GetGenericData',
//                 datasetid: item.datasetId,
//                 and: andParameter,
//                 or: item.regionConcept,
//                 format: 'csv'
//             };
//             var url = item._baseUrl + '?' + objectToQuery(parameters);
//             item._loadDataFilePromise[andParameter] = loadText(url).then(loadTableStructure.bind(undefined, item));
//         }
//         return item._loadDataFilePromise[andParameter];
//     });

//     var totalPopulationsUrl = item.regionPopulationsUrlPrefix + getActiveRegionTypeCode(item) + '.csv';
//     loadDataPromises.push(loadText(totalPopulationsUrl).then(loadTableStructure.bind(undefined, item)));

//     return when.all(loadDataPromises).then(function(tableStructures) {
//         return {
//             tableStructures: tableStructures,
//             activeCombinations: activeCombinations
//         };
//     });
// }

// // Given the loaded data files in their TableStructures, create a suitably named array of columns, from their "Value" columns.
// function buildValueColumns(item, tableStructures, activeCombinations) {
//     // The tableStructures are from the raw data files, one per activeCombinations.
//     return tableStructures.map(function(tableStructure, index) {
//         var columnNames = tableStructure.getColumnNames();
//         // Check that the data is not blank, and that the column names are ["Time", "Value", "REGION" (or equivalent), "Description"]
//         if (columnNames.length === 0) {
//             throwLoadError(item, 'GetGenericData');
//         } else if (!arraysAreEqual(columnNames, ['Time', 'Value', item.regionConcept, 'Description'])) {
//             throwDataColumnsError(item, columnNames);
//         }
//         var absCodes = activeCombinations[index];
//         // Pull out and clone the 'Value' column, and rename it to match all the codes except the region type, eg. Persons 0-4 years.
//         var valueColumn = tableStructure.columns[columnNames.indexOf('Value')];
//         var valueName = absCodes
//             .filter(function(absCode) {
//                 return absCode.concept.id !== item.regionTypeConcept;
//             })
//             .map(function(absCode) {
//                 return absCode.name.trim();
//             })
//             .reverse() // It would be nice to specify a preferred order of codes here; reverse because "Persons 0-4 years" sounds better than "0-4 years Persons".
//             .join(' ');
//         valueColumn = new TableColumn(valueName, valueColumn.values, valueColumn.options);
//         return valueColumn;
//     });
// }


// /**
//  * This builds the columns for the table data source.
//  * @private
//  * @param  {SdmxJsonCatalogItem} item This catalog item instance.
//  * @param  {Object} tableStructuresAndCombinations The output from loadDataFiles.
//  * @return {TableColumn[]} The columns.
//  */
// function buildTableColumns(item, tableStructuresAndCombinations) {
//     if (!defined(tableStructuresAndCombinations)) {
//         // Invalid selection, eg. missing a selected code for a concept.
//         return [];  // Remove current columns.
//     }
//     var tableStructures = tableStructuresAndCombinations.tableStructures;
//     var activeCombinations = tableStructuresAndCombinations.activeCombinations;
//     if (tableStructures.length <= 1) {
//         throwLoadError(item, 'GetGenericData');
//     }
//     // The last element of the tableStructures is the total population.
//     var totalPopulationTableStructure = tableStructures.pop();

//     var firstTableStructure = tableStructures[0];
//     var timeColumn = firstTableStructure.columns[0];
//     var regionColumn = firstTableStructure.columns[2];
//     var isTimeVarying = defined(timeColumn) && (timeColumn.minimumValue !== timeColumn.maximumValue);
//     // If there is time variation, for now, let's turn off the displayPercent option.
//     // (since we don't have time-varying population data yet)
//     if (isTimeVarying) {
//         item.canDisplayPercent = false;
//         item.displayPercent = false;
//     } else {
//         item.canDisplayPercent = true; // Assume we can unless proven otherwise.
//     }

//     var newColumns = buildValueColumns(item, tableStructures, activeCombinations);
//     var totalSelectedColumn = new TableColumn('Total selected', TableColumn.sumValues(newColumns), {
//         active: !item.displayPercent
//     });
//     // If there's more than one combination selected, add the total selected.
//     if (newColumns.length > 1) {
//         newColumns.push(totalSelectedColumn);
//     } else if (!item.displayPercent) {
//         // If there's only one value column, activate it.
//         newColumns[0].isActive = true;
//     }

//     // Only add the total population columns if there is no time variation
//     // (for now, since we don't have time-varying population data)
//     if (!isTimeVarying) {
//         var totalPopColumns = totalPopulationTableStructure.columns;
//         // Check that the regions correspond in the two tables.  Assume total popn table has regions as first column.
//         if (!arraysAreEqual(totalPopColumns[0].values.slice(0, regionColumn.values.length), regionColumn.values)) {
//             console.error('Region ordering is different between total population data file and ABS data set.');
//             // Activate the last value column, turn off percentDisplay and don't add the total population columns.
//             item.canDisplayPercent = false;
//             item.displayPercent = false;
//             newColumns[newColumns.length - 1].isActive = true;
//         } else {
//             var totalPopColumn = new TableColumn('Total population', totalPopColumns[totalPopColumns.length - 1].values.slice(0, regionColumn.values.length));
//             newColumns.push(totalPopColumn);
//             var percentValues = TableColumn.divideValues(totalSelectedColumn, totalPopColumn, 0).map(function(fraction) {
//                 return (fraction < 0.01) ? (Math.round(fraction * 1000) / 10) : (Math.round(fraction * 10000) / 100);
//             });
//             newColumns.push(new TableColumn('Total selected percent', percentValues, {active: item.displayPercent}));
//         }
//     }
//     // Rename the region column to conform to csv-geo-au.
//     // Since only one regiontype code can be selected, all combinations have the same regiontype.
//     // (Don't use getActiveRegionTypeCode() because the UI might no longer reflect which regiontype was loaded.)
//     // Just use the first time from the time column; we assume these are all the same (eg. 2011).
//     var loadedRegionCode = activeCombinations[0].filter(function(absCode) {
//         return absCode.concept.id === item.regionTypeConcept;
//     })[0];
//     var regionColumnName = regionTypeToCsvGeo(loadedRegionCode.code, timeColumn);

//     // Make sure the Time and REGION columns match across columns (apart from the totalPopulationTableStructure).
//     // Don't worry about Description - it is a description of the region, which we get from the region mapping anyway.
//     if (!isTimeVarying) {
//         timeColumn.options.type = VarType.SCALAR;  // Otherwise we can get a spurious clock.
//     }
//     timeColumn = new TableColumn('Year', timeColumn.values, timeColumn.options);
//     regionColumn = new TableColumn(regionColumnName, regionColumn.values, regionColumn.options);
//     tableStructures.slice(1).forEach(function(tableStructure) {
//         if (!arraysAreEqual(tableStructure.columns[0], timeColumn) ||
//             !arraysAreEqual(tableStructure.columns[2], regionColumn)) {
//             throwDataMergeError(item);
//         }
//     });

//     // Put these columns to the front of the value columns.
//     newColumns.unshift(timeColumn);
//     newColumns.unshift(regionColumn);

//     return newColumns;
// }

// function regionTypeToCsvGeo(regionType, timeColumn) {
//     // aus is left as aus, but all others (sa1, sa2, sa3, sa4, ste etc)
//     // are converted to sa4_code_2011 (if timeColumn is YEAR and all values are the same),
//     // or sa4_code if not. (Replacing sa4 with the given regionType.)
//     var lowerCaseRegionType = regionType.toLowerCase();
//     if (lowerCaseRegionType === 'aus') {
//         return lowerCaseRegionType;
//     } else {
//         if (timeColumn.subtype === VarSubType.YEAR && timeColumn.minimumValue === timeColumn.maximumValue) {
//             return lowerCaseRegionType + '_code_' + timeColumn.minimumValue;
//         } else {
//             return lowerCaseRegionType + '_code';
//         }
//     }
// }

// cleanAndProxyUrl appears in a few catalog items - we should split it into its own Core file.

function cleanUrl(url) {
    // Strip off the search portion of the URL
    var uri = new URI(url);
    uri.search('');
    return uri.toString();
}

function cleanAndProxyUrl(catalogItem, url) {
    return proxyCatalogItemUrl(catalogItem, cleanUrl(url));
}


module.exports = SdmxJsonCatalogItem;
