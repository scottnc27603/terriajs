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

    // The options that should be passed to TableColumn when creating a new column.
    this._columnOptions = undefined;

    // Allows conversion between the dimensions and the table columns.
    this._dimensionInfo = undefined;
    this._combinations = undefined;

    // The array of Concepts to display in the NowViewing panel.
    this._concepts = [];


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

    // Tracking _concepts makes this a circular object.
    // _concepts (via concepts) is both set and read in rebuildData.
    // A solution to this would be to make concepts a Promise, but that would require changing the UI side.
    knockout.track(this, ['datasetId', '_concepts']);

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
SdmxJsonCatalogItem.defaultPropertiesForSharing = clone(CsvCatalogItem.defaultPropertiesForSharing);
SdmxJsonCatalogItem.defaultPropertiesForSharing.push('regionDimensionId');
SdmxJsonCatalogItem.defaultPropertiesForSharing.push('regionTypeDimensionId');
SdmxJsonCatalogItem.defaultPropertiesForSharing.push('frequencyDimensionId');
freezeObject(SdmxJsonCatalogItem.defaultPropertiesForSharing);

SdmxJsonCatalogItem.defaultSerializers = clone(CsvCatalogItem.defaultSerializers);
freezeObject(SdmxJsonCatalogItem.defaultSerializers);

//Just the items that would influence the load from the abs server or the file
SdmxJsonCatalogItem.prototype._getValuesThatInfluenceLoad = function() {
    return [this.url];
};


SdmxJsonCatalogItem.prototype._load = function() {
    // Set some defaults.
    this.regionTypeDimensionId = defaultValue(this.regionTypeDimensionId, 'REGIONTYPE');
    this.regionDimensionId = defaultValue(this.regionDimensionId, 'REGION');
    this.frequencyDimensionId = defaultValue(this.frequencyDimensionId, 'FREQUENCY');

    var tableStyle = this._tableStyle;
    this._columnOptions = {
        displayDuration: tableStyle.displayDuration,
        displayVariableTypes: TableStructure.defaultDisplayVariableTypes,
        replaceWithNullValues: tableStyle.replaceWithNullValues,
        replaceWithZeroValues: tableStyle.replaceWithZeroValues
    };

    // if (this.url.indexOf(''))
    return loadAndBuildTable(this);
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
 *   dimensionNames: An array of length structureSeries.length ( = number of dimensions).
 *           Each element is the name of that dimension.
 *   All dimensions are ordered in terms of their keyPositions
 *   (which could theoretically differ from their index in the series array).
 * @private
 * @param  {Array} structureSeries The structure's series property, json.structure.dimensions.series.
 * @return {Object} The values and names of the dimensions.
 */
function calculateDimensionNamesAndValues(structureSeries) {
    // Store the length of each dimension, in the correct keyPosition.
    var result = {
        values: [],
        names: [],
        dimensionNames: []
    };
    for (var i = 0; i < structureSeries.length; i++) {
        result.values.push([]);
        result.names.push([]);
        result.dimensionNames.push(undefined);
    }
    for (i = 0; i < structureSeries.length; i++) {
        var thisSeries = structureSeries[i];
        // eg. thisSeries.values may be [{id: "BD_2", name: "Births"}, {id: "BD_4", name: "Deaths"}].
        // We convert this to [0, 1] using range(length).
        result.values[thisSeries.keyPosition] = range(thisSeries.values.length);
        if (thisSeries.values.length > 1) {
            result.dimensionNames[thisSeries.keyPosition] = thisSeries.name;
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
    return result;
}

/**
 * Calculates all the combinations of values that should appear as columns in our table.
 * Ignores region and region-type dimensions.
 * Returns an object with properties:
 *   values: An array, each element of which is an array of indices into each dimension.
 *           Eg. If the dimensions have lengths 1, 1, 3 and 2 respectively, the values would be
 *               [[0, 0, 0, 0], [0, 0, 0, 1], [0, 0, 1, 0], [0, 0, 1, 1], [0, 0, 2, 0], [0, 0, 2, 1]].
 *   names: An array, each element of which is an array of the names of each relevant dimension value. The names of dimensions with only one value do not contribute.
 * @private
 * @param  {SdmxJsonCatalogItem} item The catalog item. item._dimensionInfo must be set.
 * @param  {Array} structureSeries The structure's series property, json.structure.dimensions.series.
 * @param  {Object} regionInfo The output of calculateRegionDimensionInfo.
 * @return {Object} The values and names of the dimensions.
 */
function calculateNonRegionDimensionCombinations(item, structureSeries, regionInfo) {
    // Restrict attention to the first region type, first region only. Don't include their names.
    var dimensionInfo = item._dimensionInfo;
    if (defined(regionInfo.regionDimensionIndex)) {
        dimensionInfo.values[regionInfo.regionDimensionIndex] = dimensionInfo.values[regionInfo.regionDimensionIndex].slice(0, 1);
        dimensionInfo.names[regionInfo.regionDimensionIndex] = [''];
    }
    if (defined(regionInfo.regionTypeDimensionIndex)) {
        dimensionInfo.values[regionInfo.regionTypeDimensionIndex] = dimensionInfo.values[regionInfo.regionTypeDimensionIndex].slice(0, 1);
        dimensionInfo.names[regionInfo.regionTypeDimensionIndex] = [''];
    }
    // if (defined(regionInfo.frequencyDimensionIndex)) {
    //     dimensionInfo.values[regionInfo.frequencyDimensionIndex] = dimensionInfo.values[regionInfo.frequencyDimensionIndex].slice(0, 1);
    //     dimensionInfo.names[regionInfo.frequencyDimensionIndex] = [''];
    // }
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

function getRegionColumnName(regionTypeId) {
    // Convert using the principles of csv-geo-au.
    // Assume the raw data is just missing the word "code", eg. LGA or LGA_2013 should be lga_code or lga_code_2013.
    // So, if there's a _, replace the last one with _code_; else append _code.
    var underscoreIndex = regionTypeId.lastIndexOf('_');
    if (underscoreIndex >= 0) {
        return regionTypeId.slice(0, underscoreIndex) + "_code" + regionTypeId.slice(underscoreIndex);
    } else {
        return regionTypeId + '_code';
    }
}

function buildRegionColumn(structureSeries, regionInfo, columnOptions) {
    if (!defined(regionInfo.regionTypeDimensionIndex) || !defined(regionInfo.regionDimensionIndex)) {
        // Either no regiontype dimension (eg. LGA) or no region dimension (with the actual region values in it).
        return;
    }
    var regions = [];
    for (var i = 0; i < regionInfo.regionCount; i++) {
        regions.push(structureSeries[regionInfo.regionDimensionIndex].values[i].id);
    }
    // TODO: for now, only implements the first region type.
    var regionTypeId = structureSeries[regionInfo.regionTypeDimensionIndex].values[0].id.toLowerCase();
    var regionColumnName = getRegionColumnName(regionTypeId);
    var regionColumn = new TableColumn(regionColumnName, regions, columnOptions);
    return regionColumn;
}

// Create a column for each combination of (non-region) dimension values.
// The column has values for each region.
function buildValueColumns(nonRegionDimensionCombinations, structureSeries, series, regionInfo, columnOptions) {
    var columns = [];
    var uniqueValue = (nonRegionDimensionCombinations.values.length <= 1);
    for (var combinationIndex = 0; combinationIndex < nonRegionDimensionCombinations.values.length; combinationIndex++) {
        var dimensionIndices = nonRegionDimensionCombinations.values[combinationIndex];
        // The name is just the joined names of all the columns involved, or 'value' if no columns still have names.
        var dimensionName = nonRegionDimensionCombinations.names[combinationIndex].filter(function(name) {return !!name; }).join(' ');
        if (uniqueValue) {
            dimensionName = 'Value';
        }
        var values = [];
        for (var regionIndex = 0; regionIndex < regionInfo.regionCount; regionIndex++) {
            dimensionIndices[regionInfo.regionDimensionIndex] = regionIndex;
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

// Create columns for the total (and possibly total percentage) values.
// If <=1 active column, returns [].
function buildTotalColumns(item) {
    // Build a total column equal to the sum of all the active concepts.
    // Start by mapping the active concepts into arrays of arrays,
    // eg. [[0, 2], [1]] if the first & third values of the first concept are selected, and the second of the second.
    var activeConceptValues = item._concepts.map(function(parent) {
        return parent.items.map(function(concept, i) {
            return i;
        }).filter(function(i) {
            return parent.items[i].isActive;
        });
    });

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

// Called when the active column changes.
function changedActiveItems(item) {
    var columns = item._tableStructure.columns.slice(0, item._combinations.length + 1);
    if (columns.length > 0) {
        columns = columns.concat(buildTotalColumns(item));
        updateColumns(item, columns);
    }
}

// Build out the concepts displayed in the NowViewing panel.
function setConceptsAndCombinations(item, valueColumns, nonRegionDimensionCombinations) {
    // Only store the combinations as they relate the concepts.
    // Ie. Drop the trivial (single-valued) dimensions from nonRegionDimensionCombinations.values.
    // Eg. [[0, 0, 0, 0], [0, 0, 0, 1], [0, 0, 1, 0], [0, 0, 1, 1], [0, 0, 2, 0], [0, 0, 2, 1]]
    // should become [[0, 0], [0, 1], [1, 0], [1, 1], [2, 0], [2, 1]], as the first two indices are single-valued.
    function nonTrivialFilter(_, i) {
        return item._dimensionInfo.values[i].length > 1;
    }

    item._combinations = nonRegionDimensionCombinations.values.map(function(combination) {
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

function loadAndBuildTable(item) {
    // We pass column options to TableStructure too, but they only do anything if TableStructure itself (eg. via fromJson) adds the columns,
    // which is not the case here.  We will need to pass them to each call to new TableColumn as well.
    item._tableStructure = new TableStructure(item.name, item._columnOptions);
    item._regionMapping = new RegionMapping(item, item._tableStructure, item._tableStyle);

    if (!defined(item._regionMapping)) {
        // This can happen when you open a shared URL with displayRegionPercent defined, since that has a ko subscription above.
        return when();
    }
    item._regionMapping.isLoading = true;

    var url = cleanAndProxyUrl(item, item.url);
    return loadJson(url).then(function(json) {
        var structureSeries = json.structure.dimensions.series;
        var series = json.dataSets[0].series;

        var regionInfo = calculateRegionDimensionInfo(item, structureSeries);
        if (!defined(regionInfo.regionDimensionIndex) || !defined(regionInfo.regionTypeDimensionIndex)) {
            // TODO: Raise an error, or handle case when there are no regions.
            console.log('No region or region type defined.');
            return;
        }

        item._dimensionInfo = calculateDimensionNamesAndValues(structureSeries);
        // This lets us loop through a single for loop, rather than a dynamically-determined number of nested for loops.
        var nonRegionDimensionCombinations = calculateNonRegionDimensionCombinations(item, structureSeries, regionInfo);

        var regionColumn = buildRegionColumn(structureSeries, regionInfo, item._columnOptions);
        var valueColumns = buildValueColumns(nonRegionDimensionCombinations, structureSeries, series, regionInfo, item._columnOptions);
        setConceptsAndCombinations(item, valueColumns, nonRegionDimensionCombinations);
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
