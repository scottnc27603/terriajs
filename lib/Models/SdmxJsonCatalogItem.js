'use strict';

/*global require*/
var URI = require('urijs');

var clone = require('terriajs-cesium/Source/Core/clone');
var defaultValue = require('terriajs-cesium/Source/Core/defaultValue');
var defined = require('terriajs-cesium/Source/Core/defined');
var defineProperties = require('terriajs-cesium/Source/Core/defineProperties');
// var DeveloperError = require('terriajs-cesium/Source/Core/DeveloperError');
var freezeObject = require('terriajs-cesium/Source/Core/freezeObject');
// var deprecationWarning = require('terriajs-cesium/Source/Core/deprecationWarning');
var knockout = require('terriajs-cesium/Source/ThirdParty/knockout');
var loadJson = require('terriajs-cesium/Source/Core/loadJson');
// var loadText = require('terriajs-cesium/Source/Core/loadText');
// var objectToQuery = require('terriajs-cesium/Source/Core/objectToQuery');
var when = require('terriajs-cesium/Source/ThirdParty/when');

var arrayProduct = require('../Core/arrayProduct');
// var arraysAreEqual = require('../Core/arraysAreEqual');
var CsvCatalogItem = require('./CsvCatalogItem');
var DisplayVariablesConcept = require('../Map/DisplayVariablesConcept');
var inherit = require('../Core/inherit');
// var TerriaError = require('../Core/TerriaError');
var overrideProperty = require('../Core/overrideProperty');
var proxyCatalogItemUrl = require('./proxyCatalogItemUrl');
var RegionMapping = require('../Models/RegionMapping');
var TableColumn = require('../Map/TableColumn');
var TableStructure = require('../Map/TableStructure');
var VariableConcept = require('../Map/VariableConcept');
// var VarSubType = require('../Map/VarSubType');
// var VarType = require('../Map/VarType');

// var allowedRegionCodes = ['AUS', 'STE', 'SA4', 'SA3', 'SA2', 'SA1', 'CED', 'LGA', 'POA', 'SED']; // These should be made a parameter.

/*
    The SDMX-JSON format.
    Descriptions of this format are available at:
    - https://data.oecd.org/api/sdmx-json-documentation/
    - https://github.com/sdmx-twg/sdmx-json/tree/master/data-message/docs
    - https://sdmx.org/
    - http://stats.oecd.org/sdmx-json/ (hosts a handy query builder)

    The URL can be of two types, eg:
    1. http://stat.abs.gov.au/sdmx-json/data/ABS_REGIONAL_LGA/CABEE_2.LGA2013.1+.A/all?startTime=2013&endTime=2013
    2. http://stat.abs.gov.au/sdmx-json/data/ABS_REGIONAL_LGA

    For #2, the dimension names and codes come from (in json format):
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
      - result.structure.dimensions.observation[i] for {role, id, name, values[]} to get the name & id of the observations and its array of allowed values (with {id, name}).
      - result.dataSets[0].series[key].observations[t][0] with key = "xx.yy.zz" where xx is the id of a value from dimension 0, etc, and t is the time index (eg. 0 for a single time).

    Currently, we only parse the first "dataSet" object provided. (This covers all situations of interest to us so far.)

    Time seems to be handled specially, at least by the OECD.
    Eg.
      http://stats.oecd.org/sdmx-json/dataflow/QNA shows there are 5 dimensions (result.structure.dimensions.observation): LOCATION, SUBJECT, MEASURE, FREQUENCY, TIME_PERIOD.
      But http://stats.oecd.org/sdmx-json/data/QNA/.B1_GE.VOBARSA.Q/all only returns 4 dimensions (result.structure.dimensions.series): TIME_PERIOD is gone.
      Instead, it has become an observation: result.structure.dimensions.observation[0] has property "values" with lots of {id, name} fields, eg. {id: "1960-Q1", name: "Q1-1960"}.
      And result.dataSets[0].series[key].observations[t] has lots of values for different t, not necessarily including t = 0. (eg. key = "21:0:0:0" starts at t = 140).
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

    // The options that should be passed to TableColumn when creating a new column.
    this._columnOptions = undefined;

    // Allows conversion between the dimensions and the table columns.
    this._dimensionInfo = undefined;
    this._combinations = undefined;

    // This is set to the dataflow URL for this data, if relevant.
    this._dataflowUrl = undefined;

    // The array of Concepts to display in the NowViewing panel.
    this._concepts = [];

    /**
     * Gets or sets the 'data' SDMX URL component, eg. 'data' in http://stats.oecd.org/sdmx-json/data/QNA.
     * Defaults to 'data'.
     * This property is observable.
     * @type {String}
     */
    this.dataUrlComponent = undefined;

    /**
     * Gets or sets the 'dataflow' SDMX URL component, eg. 'dataflow' in http://stats.oecd.org/sdmx-json/dataflow/QNA.
     * Defaults to 'dataflow'.
     * This property is observable.
     * @type {String}
     */
    this.dataflowUrlComponent = undefined;

    /**
     * Gets or sets the SDMX region-type dimension id used with the region code to set the region type.
     * Usually defaults to 'REGIONTYPE'.
     * This property is observable.
     * @type {String}
     */
    this.regionTypeDimensionId = undefined;

    /**
     * Gets or sets the SDMX region dimension id, which is not displayed as a user-choosable dimension. Defaults to 'REGION'.
     * This property is observable.
     * @type {String}
     */
    this.regionDimensionId = undefined;

    /**
     * Gets or sets the SDMX frequency dimension id. Defaults to 'FREQUENCY'. TODO: Not used currently.
     * This property is observable.
     * @type {String}
     */
    this.frequencyDimensionId = undefined;  // TODO: not sure if frequency needs to be 'special' like this.

    /**
     * Gets or sets the SDMX time period dimension id, which is not displayed as a user-choosable dimension. Defaults to 'TIME_PERIOD'.
     * This property is observable.
     * @type {String}
     */
    this.timePeriodDimensionId = undefined;

    /**
     * Gets or sets the regiontype directly, which is an alternative to including a regiontype in the data.
     * Eg. "cnt3" would tell us that we should use cnt3 as the table column name.
     * By default this is undefined.
     * This property is observable.
     * @type {String}
     */
    this.regionType = undefined;

    // Tracking _concepts makes this a circular object.
    // _concepts (via concepts) is both set and read in rebuildData.
    // A solution to this would be to make concepts a Promise, but that would require changing the UI side.
    knockout.track(this, ['_concepts']);

    overrideProperty(this, 'concepts', {
        get: function() {
            return this._concepts;
        }
    });

    knockout.defineProperty(this, 'activeConcepts', {
        get: function() {
            if (defined(this._concepts) && this._concepts.length > 0) {
                return this._concepts.map(function(parent) {
                    return parent.items.filter(function(concept) { return concept.isActive; });
                });
            }
        }
    });

    knockout.getObservable(this, 'activeConcepts').subscribe(changedActiveItems.bind(null, this), this);

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
SdmxJsonCatalogItem.defaultPropertiesForSharing = clone(CsvCatalogItem.defaultPropertiesForSharing); // TODO: do we need to add dataUrlComponent etc to this list?
SdmxJsonCatalogItem.defaultPropertiesForSharing.push('regionDimensionId');
SdmxJsonCatalogItem.defaultPropertiesForSharing.push('regionTypeDimensionId');
SdmxJsonCatalogItem.defaultPropertiesForSharing.push('frequencyDimensionId');
SdmxJsonCatalogItem.defaultPropertiesForSharing.push('timePeriodDimensionId');
freezeObject(SdmxJsonCatalogItem.defaultPropertiesForSharing);

SdmxJsonCatalogItem.defaultSerializers = clone(CsvCatalogItem.defaultSerializers);
freezeObject(SdmxJsonCatalogItem.defaultSerializers);

// Just the items that would influence the load from the abs server or the file
SdmxJsonCatalogItem.prototype._getValuesThatInfluenceLoad = function() {
    return [this.url];
};

// The URL can have two different forms, which require different handling.
// 1. http://stat.abs.gov.au/sdmx-json/data/ABS_REGIONAL_LGA/CABEE_2.LGA2013.1+.A/all?startTime=2013&endTime=2013
//    Read data from this URL directly and construct the table and concepts from it.
// 2. http://stat.abs.gov.au/sdmx-json/data/ABS_REGIONAL_LGA
//    Do not attempt to hit this URL directly.
//    Instead get the concepts from .../dataflow/ABS_REGIONAL_LGA, and then, whenever the active concepts are changed,
//    construct a specific URL like in #1 from those concepts, load the data from it, and construct a table.
//    If no 'dataflow' URL is recognizable, revert to #1 behaviour.
// If the URL fits neither form, assume it is a datafile to be handled like #1.
// This function returns undefined for #1, and the dataflow URL for #2.
function getDataflowUrl(item) {
    var dataUrlComponent = '/' + item.dataUrlComponent + '/';
    var dataUrlIndex = (item.url.lastIndexOf(dataUrlComponent));
    // If the URL contains /data/, look for how many / terms come after it.
    if (dataUrlIndex >= 0) {
        var suffix = item.url.slice(dataUrlIndex + dataUrlComponent.length);
        // eg. suffix would be ABS_REGIONAL_LGA/CABEE_2.LGA2013.1+.A/all...
        // If it contains a /, and anything after the /, then treat it as #1.
        if (suffix.indexOf('/') >= 0 && suffix.indexOf('/') < suffix.length - 1) {
            return;
        } else {
            // return the same URL but with /data/ replaced with /dataflow/.
            var dataflowUrlComponent = '/' + item.dataflowUrlComponent + '/';
            return item.url.replace(dataUrlComponent, dataflowUrlComponent);
        }
    }
}

SdmxJsonCatalogItem.prototype._load = function() {
    // Set some defaults.
    this.regionTypeDimensionId = defaultValue(this.regionTypeDimensionId, 'REGIONTYPE');
    this.regionDimensionId = defaultValue(this.regionDimensionId, 'REGION');
    this.frequencyDimensionId = defaultValue(this.frequencyDimensionId, 'FREQUENCY');
    this.timePeriodDimensionId = defaultValue(this.timePeriodDimensionId, 'TIME_PERIOD');
    this.dataUrlComponent = defaultValue(this.dataUrlComponent, 'data');
    this.dataflowUrlComponent = defaultValue(this.dataflowUrlComponent, 'dataflow');

    var tableStyle = this._tableStyle;
    this._columnOptions = {
        displayDuration: tableStyle.displayDuration,
        displayVariableTypes: TableStructure.defaultDisplayVariableTypes,
        replaceWithNullValues: tableStyle.replaceWithNullValues,
        replaceWithZeroValues: tableStyle.replaceWithZeroValues
    };

    // We pass column options to TableStructure too, but they only do anything if TableStructure itself (eg. via fromJson) adds the columns,
    // which is not the case here.  We will need to pass them to each call to new TableColumn as well.
    this._tableStructure = new TableStructure(this.name, this._columnOptions);

    this._dataflowUrl = getDataflowUrl(this);
    if (this._dataflowUrl) {
        return loadDataflow(this);
    } else {
        return loadAndBuildTable(this);
    }
};

// eg. range(5) returns [0, 1, 2, 3, 4].
function range(length) {
    return Array.apply(null, Array(length)).map(function(x, i) { return i; });
}

/**
 * Returns an object with properties:
 *   values: An array of length structureSeries.length ( = number of dimensions).
 *           Each element is range(dimension length), ie. [0, 1, 2, 3, ..., n - 1].
 *   names:  An array of length structureSeries.length ( = number of dimensions).
 *           Each element is an array of the names of each entry in the dimension, eg. ['Births', 'Deaths'].
 *           If there is only one name, it is blanked out so as not to confuse the UX.
 *   ids:    As for names, but ids. A single id is not made blank.
 *   dimensionNames: An array of length structureSeries.length ( = number of dimensions).
 *           Each element is the name of that dimension.
 *   dimensionIds: As for dimensionNames, but ids.
 *   All dimensions are ordered in terms of their keyPositions
 *   (which could theoretically differ from their index in the series array).
 * If specialDimensions is provided, removes the region dimension.
 * @private
 * @param  {Array} structureSeries The structure's series property, json.structure.dimensions.series.
 * @param  {Object} specialDimensions The output of calculateSpecialDimensions.
 * @return {Object} The values and names of the dimensions.
 */
function calculateShownDimensionNamesAndValues(structureSeries, specialDimensions) {
    // Store the length of each dimension, in the correct keyPosition.
    var result = {
        values: [],
        names: [],
        ids: [],
        dimensionNames: [],
        dimensionIds: []
    };
    for (var i = 0; i < structureSeries.length; i++) {
        result.values.push([]);
        result.names.push([]);
        result.dimensionNames.push(undefined);
        result.dimensionIds.push(undefined);
    }
    for (i = 0; i < structureSeries.length; i++) {
        var thisSeries = structureSeries[i];
        // eg. thisSeries.values may be [{id: "BD_2", name: "Births"}, {id: "BD_4", name: "Deaths"}].
        // We convert this to [0, 1] using range(length).
        result.values[thisSeries.keyPosition] = range(thisSeries.values.length);
        result.dimensionNames[thisSeries.keyPosition] = thisSeries.name;
        result.dimensionIds[thisSeries.keyPosition] = thisSeries.id;
        result.ids[thisSeries.keyPosition] = thisSeries.values.map(function(nameAndId) { return nameAndId.id; });
        if (thisSeries.values.length > 1) {
            result.names[thisSeries.keyPosition] = thisSeries.values.map(function(nameAndId) { return nameAndId.name; });
        } else {
            // Don't include the name of this dimension if it can only take a single value.
            result.names[thisSeries.keyPosition] = [''];
        }
    }
    if (result.values.indexOf(undefined) >= 0) {
        // TODO: Raise an error properly.
        console.log('Missing dimension at key position ' + result.values.indexOf(undefined));
    }
    // Now hide some special dimensions.
    function removeIndex(index) {
        if (defined(index)) {
            result.values[index] = result.values[index].slice(0, 1);
            result.names[index] = [''];
        }
    }
    if (defined(specialDimensions)) {
        removeIndex(specialDimensions.regionDimensionIndex);
        removeIndex(specialDimensions.timePeriodDimensionIndex);
        // removeIndex(specialDimensions.regionTypeDimensionIndex);
        // removeIndex(specialDimensions.frequencyDimensionIndex);
    }
    return result;
}

// Returns a function that can be used as a filter on item._dimensionInfo to exclude the time_period dimension.
// Eg. item._dimensionInfo.ids.filter(getNonTimeDimensionFilter(item))
function getNonTimeDimensionFilter(item) {
    return function(_, dimensionIndex) {
        return dimensionIndex !== item._specialDimensions.timePeriodDimensionIndex;
    };
}

/**
 * Calculates all the combinations of values that should appear as columns in our table.
 * Returns an object with properties:
 *   values: An array, each element of which is an array of indices into each dimension.
 *           Eg. If the dimensions have lengths 1, 1, 3 and 2 respectively, the values would be
 *               [[0, 0, 0, 0], [0, 0, 0, 1], [0, 0, 1, 0], [0, 0, 1, 1], [0, 0, 2, 0], [0, 0, 2, 1]].
 *   names: An array, each element of which is an array of the names of each relevant dimension value. The names of dimensions with only one value do not contribute.
 * @private
 * @param  {SdmxJsonCatalogItem} item The catalog item. item._dimensionInfo must be set.
 * @return {Object} The values and names of the dimensions to be shown.
 */
function calculateShownDimensionCombinations(item) {
    // Note we need to suppress the time dimension from the dimension list, if any; it appears as an observation instead.    
    // Convert the values into all the combinations we'll need to load into columns,
    // eg. [[0], [0], [0, 1, 2], [0, 1]] =>
    //     [[0, 0, 0, 0], [0, 0, 0, 1], [0, 0, 1, 0], [0, 0, 1, 1], [0, 0, 2, 0], [0, 0, 2, 1]].
    return {
        values: arrayProduct(item._dimensionInfo.values.filter(getNonTimeDimensionFilter(item))),
        names: arrayProduct(item._dimensionInfo.names.filter(getNonTimeDimensionFilter(item)))
    };
}

function calculateSpecialDimensions(item, structureSeries) {
    var result = {
        regionDimensionIndex: undefined,
        regionTypeDimensionIndex: undefined,
        regionTypeCount: 0,
        timePeriodDimensionIndex: undefined
    };
    for (var i = 0; i < structureSeries.length; i++) {
        if (structureSeries[i].id === item.regionDimensionId) {
            // Assume only one regiontype's regions are provided. (Is this a good assumption?)
            result.regionDimensionIndex = structureSeries[i].keyPosition;
        } else if (structureSeries[i].id === item.regionTypeDimensionId) {
            result.regionTypeDimensionIndex = structureSeries[i].keyPosition;
            result.regionTypeCount = structureSeries[i].values.length;
        } else if (structureSeries[i].id === item.timePeriodDimensionId) {
            result.timePeriodDimensionIndex = structureSeries[i].keyPosition;
        }
    }
    return result;
}

function getRegionColumnName(regionTypeId) {
    // Convert using the principles of csv-geo-au.
    // Assume the raw data is just missing the word "code", eg. LGA or LGA_2013 should be lga_code or lga_code_2013.
    // So, if there's a _, replace the last one with _code_; else append _code.
    var lowerCaseId = regionTypeId.toLowerCase();
    var underscoreIndex = lowerCaseId.lastIndexOf('_');
    if (underscoreIndex >= 0) {
        return lowerCaseId.slice(0, underscoreIndex) + '_code' + lowerCaseId.slice(underscoreIndex);
    } else {
        return lowerCaseId + '_code';
    }
}

function getRegionTypeId(item, structureSeries) {
    var specialDimensions = item._specialDimensions;
    if (defined(specialDimensions.regionTypeDimensionIndex)) {
        // If there is a REGIONTYPE dimension, return its id.
        return structureSeries[specialDimensions.regionTypeDimensionIndex].values[0].id;
    } else if (defined(specialDimensions.regionDimensionIndex)) {
        // Else, if there is a REGION dimension and item.regionType has been defined, return item.regionType.
        var regionDimId = structureSeries[specialDimensions.regionDimensionIndex].id;
        if (defined(item.regionType) && defined(item.regionType)) {
            return item.regionType;
        }
        // Else, return the REGION dimension id, if present.
        return regionDimId;
    }
}

function buildRegionColumn(item, structureSeries) {
    var specialDimensions = item._specialDimensions;
    if (!defined(specialDimensions.regionDimensionIndex)) {
        // No region dimension (with the actual region values in it).
        return;
    }
    var regions = [];
    var regionCount = structureSeries[specialDimensions.regionDimensionIndex].values.length;
    for (var i = 0; i < regionCount; i++) {
        regions.push(structureSeries[specialDimensions.regionDimensionIndex].values[i].id);
    }
    // TODO: for now, only implements the first region type.
    var regionTypeId = getRegionTypeId(item, structureSeries);
    var regionColumnName = getRegionColumnName(regionTypeId);
    var regionColumn = new TableColumn(regionColumnName, regions, item._columnOptions);
    return regionColumn;
}

// Create a column for each combination of (non-region) dimension values.
// The column has values for each region.
function buildValueColumns(shownDimensionCombinations, structureSeries, series, specialDimensions, columnOptions) {
    var columns = [];
    var uniqueValue = (shownDimensionCombinations.values.length <= 1);
    for (var combinationIndex = 0; combinationIndex < shownDimensionCombinations.values.length; combinationIndex++) {
        var dimensionIndices = shownDimensionCombinations.values[combinationIndex];
        // The name is just the joined names of all the columns involved, or 'value' if no columns still have names.
        var dimensionName = shownDimensionCombinations.names[combinationIndex].filter(function(name) {return !!name; }).join(' ');
        if (uniqueValue) {
            dimensionName = 'Value';
        }
        var values = [];
        var regionCount = structureSeries[specialDimensions.regionDimensionIndex].values.length;
        for (var regionIndex = 0; regionIndex < regionCount; regionIndex++) {
            dimensionIndices[specialDimensions.regionDimensionIndex] = regionIndex;
            var key = dimensionIndices.join(':');
            values.push(defined(series[key]) ? series[key].observations[0][0] : null);
        }
        var column = new TableColumn(dimensionName, values, columnOptions);
        if (uniqueValue) {
            column.isActive = true;
        }
        columns.push(column);
    }
    return columns;
}

// Map the active concepts into arrays of arrays.
// Eg. Return [[0, 2], [1]] if the first & third values of the first concept are selected, and the second of the second.
function calculateActiveConceptValues(concepts) {
    return concepts.map(function(parent) {
        return parent.items.map(function(concept, i) {
            return i;
        }).filter(function(i) {
            return parent.items[i].isActive;
        });
    });
}

// Create columns for the total (and possibly total percentage) values.
// If <=1 active column, returns [].
function buildTotalColumns(item) {
    // Build a total column equal to the sum of all the active concepts.
    // Start by mapping the active concepts into arrays of arrays,
    // eg. [[0, 2], [1]] if the first & third values of the first concept are selected, and the second of the second.
    var activeConceptValues = calculateActiveConceptValues(item._concepts);
    if (activeConceptValues.length === 0) {
        return [];
    }
    // Find all the combinations of active concepts, eg. [[0, 2], [1, 3]] => [[0, 1], [0, 3], [2, 1], [2, 3]].
    var activeCombinations = arrayProduct(activeConceptValues);
    // Look up which columns these correspond to, using item._combinations.
    // Note we need to convert the arrays to strings for indexOf to work.
    var stringifiedCombinations = item._combinations.map(function(combination) { return combination.join(','); });
    var indicesIntoCombinations = activeCombinations.map(function(activeCombination) {
        var stringifiedActiveCombination = activeCombination.join(',');
        return stringifiedCombinations.indexOf(stringifiedActiveCombination);
    });
    // Slice off the first column, which is the region column, and only keep the value columns (ignoring total columns which come at the end).
    var valueColumns = item._tableStructure.columns.slice(1, item._combinations.length + 1);
    var includedColumns = valueColumns.filter(function(column, i) { return indicesIntoCombinations.indexOf(i) >= 0; });
    var totalColumn = new TableColumn('Total selected', TableColumn.sumValues(includedColumns), item._columnOptions);
    totalColumn.isActive = true;
    return [totalColumn];
}

// Sets the tableStructure's columns to the new columns, redraws the map, and closes the feature info panel.
function updateColumns(item, newColumns) {
    item._tableStructure.columns = newColumns;
    if (item._tableStructure.columns.length === 0) {
        // Nothing to show, so the attempt to redraw will fail; need to explicitly hide the existing regions.
        item._regionMapping.hideImageryLayer();
        item.terria.currentViewer.notifyRepaintRequired();
    }
    // Close any picked features, as the description of any associated with this catalog item may change.
    item.terria.pickedFeatures = undefined;
}

// Returns the dimension request string, eg. "BD_2+BD_4.LGA_2013..A." appropriate for the active concept values.
// One trick is that the time dimension can appear in the dataflow, but should not be included in the data (or this request string).
function calculateDimensionRequestString(item) {
    var activeConceptValues = calculateActiveConceptValues(item._concepts);
    var nextConceptIndex = 0;
    var nonTimeDimensionIds = item._dimensionInfo.ids.filter(getNonTimeDimensionFilter(item));
    var activeDimensionValues = nonTimeDimensionIds.map(function(theseIds, dimensionIndex) {
        if (dimensionIndex === item._specialDimensions.regionDimensionIndex) {
            return ['']; // A missing id loads all ids.
        }
        if (theseIds.length === 1) {
            return theseIds;
        } else {
            return activeConceptValues[nextConceptIndex++].map(function(activeIndex) {
                return theseIds[activeIndex];
            });
        }
    });
    return activeDimensionValues.map(function(values) {
        return values.join('+');
    }).join('.');
}

// Called when the active column changes.
function changedActiveItems(item) {
    console.log('changedActiveItems');
    if (!defined(item._dataflowUrl)) {
        // All the data is already here, just update the total columns.
        var columns = item._tableStructure.columns.slice(0, item._combinations.length + 1);
        if (columns.length > 0) {
            // TODO: If all the values for a single dimension are deselected, handle it specially.
            columns = columns.concat(buildTotalColumns(item));
            updateColumns(item, columns);
        }
    } else {
        // Download the data and build the appropriate table.
        // Eg. by appending /a+b+c.d.e+f.g/all to the url.
        // These need to be in the order of the original dimensions, not the concepts.
        var dimensionRequestString = calculateDimensionRequestString(item);
        var url = item.url;
        if (url[url.length - 1] !== '/') {
            url += '/';
        }
        url += dimensionRequestString + '/all';
        // TODO: FOR TESTING ONLY
        console.log('Would download', url);
        url = item.url;
        return loadAndBuildTable(item, url);
    }
}

// Build out the concepts displayed in the NowViewing panel,
// and store a mapping (item._combinations) to convert from concepts to table columns.
// The meaning of item._combinations is:
//   If the activeConceptValues array matches an array in item._combinations, then its index gives the column # in the table,
//   not including any region or time columns.
function setConceptsAndCombinations(item, shownDimensionCombinations) {
    // Only store the combinations as they relate the concepts.
    // Ie. Drop the trivial (single-valued) dimensions from shownDimensionCombinations.values.
    // Eg. [[0, 0, 0, 0], [0, 0, 0, 1], [0, 0, 1, 0], [0, 0, 1, 1], [0, 0, 2, 0], [0, 0, 2, 1]]
    // should become [[0, 0], [0, 1], [1, 0], [1, 1], [2, 0], [2, 1]], as the first two indices are single-valued.
    function nonTrivialFilter(_, i) {
        return item._dimensionInfo.values[i].length > 1;
    }

    item._combinations = shownDimensionCombinations.values.map(function(combination) {
        return combination.filter(nonTrivialFilter);
    });
    var nonTrivialDimensionNames = item._dimensionInfo.dimensionNames.filter(nonTrivialFilter);
    var nonTrivialNames = item._dimensionInfo.names.filter(nonTrivialFilter);
    item._concepts = nonTrivialDimensionNames.map(function(dimName, i) {
        // For now assume all variables can have multiple active, so pass true.
        var concept = new DisplayVariablesConcept(dimName, true);
        concept.items = nonTrivialNames[i].map(function(valueName) {
            return new VariableConcept(valueName, {parent: concept, active: true});
        });
        return concept;
    });
}

// This is called when the URL gives a datasetId, but no specifics.
// We start by loading in the structure (without any data) from the dataflow URL.
function loadDataflow(item) {
    var dataflowUrl = cleanAndProxyUrl(item, item._dataflowUrl);
    return loadJson(dataflowUrl).then(function(json) {
        // Then access:
        //   - result.structure.dimensions.observation[k] for {keyPosition, id, name, values[]} to get the name & id of dimension keyPosition and its array of allowed values (with {id, name}).
        //   - result.structure.dimensions.attributes.dataSet has some potentially interesting things such as units, unit multipliers, reference periods (eg. http://stats.oecd.org/sdmx-json/dataflow/QNA).
        //   - result.structure.dimensions.attributes.observation has some potentially interesting things such as time formats and status (eg. estimated value, forecast value).
        var structureSeries = json.structure.dimensions.observation;

        console.log('Loaded from dataflow', item._dataflowUrl, structureSeries);
        item._specialDimensions = calculateSpecialDimensions(item, structureSeries);
        if (!defined(item._specialDimensions.regionDimensionIndex)) {
            // TODO: Raise an error, or handle case when there are no regions.
            console.log('No regions defined.');
            return;
        }
        // console.log('special dimensions', item._specialDimensions);
        item._dimensionInfo = calculateShownDimensionNamesAndValues(structureSeries, item._specialDimensions);
        var shownDimensionCombinations = calculateShownDimensionCombinations(item);
        setConceptsAndCombinations(item, shownDimensionCombinations);
        // console.log('concepts', item._concepts);
        // The rest of the magic occurs because the concepts are made active.
        return when();
    });
}

// This is called with item.url when the URL is for a specific data file, ie. dataflow is not used.
// It is also called with a calculated url when dataflow is used.
function loadAndBuildTable(item, url) {
    if (!defined(url)) {
        url = item.url;
    }
    item._regionMapping = new RegionMapping(item, item._tableStructure, item._tableStyle);
    item._regionMapping.isLoading = true;
    return loadJson(cleanAndProxyUrl(item, url)).then(function(json) {
        var structureSeries = json.structure.dimensions.series;
        var series = json.dataSets[0].series;

        if (!defined(item._specialDimensions)) {
            item._specialDimensions = calculateSpecialDimensions(item, structureSeries);
        }
        if (!defined(item._specialDimensions.regionDimensionIndex)) {
            // TODO: Raise an error, or handle case when there are no regions.
            console.log('No regions defined.');
            return;
        }
        if (!defined(item._dimensionInfo)) {
            item._dimensionInfo = calculateShownDimensionNamesAndValues(structureSeries, item._specialDimensions);
        }
        // This lets us loop through a single for loop, rather than a dynamically-determined number of nested for loops.
        var shownDimensionCombinations = calculateShownDimensionCombinations(item);
        if (!defined(item._combinations)) {
            setConceptsAndCombinations(item, shownDimensionCombinations);
        }

        var regionColumn = buildRegionColumn(item, structureSeries);
        var valueColumns = buildValueColumns(shownDimensionCombinations, structureSeries, series, item._specialDimensions, item._columnOptions);
        item._tableStructure.columns = [regionColumn].concat(valueColumns);
        // Set the columns and the concepts before building the total column, because it uses them both.
        var totalColumns = buildTotalColumns(item, valueColumns); // The region column can't be active, so ok not to pass it.
        var columns = item._tableStructure.columns.concat(totalColumns);
        updateColumns(item, columns);

        return item._regionMapping.loadRegionDetails();
    }).then(function(regionDetails) {
        // Can get here with undefined region column name, hence no regionDetails.
        if (regionDetails) {
            RegionMapping.setRegionColumnType(regionDetails);
            // Force a recalc of the imagery.
            // Required because we load the region details _after_ setting the active column.
            item._regionMapping.isLoading = false;
            item._regionMapping.enable();  // TODO: Why do we need this? Only needed for the dataflow case.
        }
        return when();
    });
}

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
