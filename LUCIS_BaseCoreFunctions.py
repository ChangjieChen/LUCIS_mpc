import os
import re
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
import fiona
import rasterio
from rasterio import features, Affine
from rasterstats import zonal_stats
import importlib.util as implib
from scipy.stats import gamma
from scipy import spatial
from osgeo import gdal
from photutils.utils import ShepardIDWInterpolator as idw


def ShapeToGeoPandas(inputshapefile=None, selectquery=None, usecols='All'):
    def records(inputshapefile=None, selectquery=None, usecols='All'):
        with fiona.open(inputshapefile) as source:
            for feature in source:
                if type(selectquery) is str:
                    df = pd.read_csv(selectquery, dtype=str)
                    if df.columns[0] == 'DORUC':
                        selectquery = ['DORUC', list(df['DORUC'])]
                    else:
                        pass
                else:
                    pass
                if selectquery is None or feature['properties'][selectquery[0]] in selectquery[1]:
                    if usecols == 'All':
                        yield feature
                    else:
                        f = {k: feature[k] for k in ['id', 'geom']}
                        f['properties'] = {k: feature['properties'][k] for k in usecols}
                        yield f
                else:
                    pass
    return gpd.GeoDataFrame.from_features(records(inputshapefile, selectquery, usecols))


def GetGeodfByAttribute(inputshapefile, fieldname, csvdict, usecols):
    output = pd.DataFrame()
    for csv in csvdict.keys():
        df = pd.read_csv(csvdict[csv], dtype=str)
        df['name'] = csv
        output = output.append(df, ignore_index=True)
    query = [fieldname, list(output[fieldname])]
    allgeodf = ShapeToGeoPandas(inputshapefile, query, usecols)
    return {(k, v) for k, v in zip(csvdict.keys(),
                                   [allgeodf[allgeodf[fieldname] in list(output[output['name'] == i][fieldname])] for i
                                    in csvdict.keys()])}


def SelectionQuery(inputgeodf, querycsv, inlist=True):
    query = pd.read_csv(querycsv, dtype=str)
    if len(query.columns) == 1 or query.columns[0] == 'DORUC':
        if inlist is True:
            return inputgeodf[inputgeodf[query.columns[0]].isin(query[query.columns[0]])]
        else:
            return inputgeodf[-inputgeodf[query.columns[0]].isin(query[query.columns[0]])]
    elif len(query.columns) == 3:
        if query.columns[1] == 'AND':
            return inputgeodf[(inputgeodf[query.columns[0]].isin(query[query.columns[0]])) &
            (inputgeodf[query.columns[2]].isin(query[query.columns[2]]))]
        else:
            return inputgeodf[(inputgeodf[query.columns[0]].isin(query[query.columns[0]])) |
            (inputgeodf[query.columns[2]].isin(query[query.columns[2]]))]
    elif len(query.columns) == 5:
        if query.columns[1] == 'AND':
            return inputgeodf[(inputgeodf[query.columns[0]].isin(query[query.columns[0]])) &
            (inputgeodf[query.columns[2]].isin(query[query.columns[2]])) &
            (inputgeodf[query.columns[4]].isin(query[query.columns[4]]))]
        else:
            return inputgeodf[(inputgeodf[query.columns[0]].isin(query[query.columns[0]])) |
            (inputgeodf[query.columns[2]].isin(query[query.columns[2]])) |
            (inputgeodf[query.columns[4]].isin(query[query.columns[4]]))]


def CSVtoDict(csvfile):
    df = pd.read_csv(csvfile)
    return dict(zip(df[df.columns[0]], df[df.columns[1]]))


def PostgresTableToDict(sqldef, conn):
    """Convert a postgresql table to a python dictionary. The table must contain two columns column 1 contains the keys, and column 2 contains values

    :param str sqldef: sql definition of the selected table
    :param str conn: database connection string
    :return: the converted dictionary from the table
    :rtype: dict
    """
    df = pd.read_sql(sqldef, con=conn)
    return dict(zip(df.iloc[:, 0], df.iloc[:, 1]))


def WeightedSum(inputgeodf, suitfields, weightdf, newsuitfield, dropweightsuits=False):
    """Calculate a new field based on weights given to existing fields

    :param GeoDataFrame inputgeodf: input GeoDataFrame
    :param list suitfields: a list of field names that are used in the calculation
    :param DataFrame weightdf: Pandas DataFrame contains the weights information for each field
    :param str newsuitfield: name of the new field
    :param bool dropweightsuits: whether to drop the suitability fields
    :return: A GeoDataFrame contains the weighted sum result
    :rtype: GeoDataFrame
    """
    inputgeodf[newsuitfield] = 0
    for field in suitfields:
        inputgeodf[newsuitfield] = weightdf['weight'].loc[field.split('_')[0]] * inputgeodf[field] + inputgeodf[newsuitfield]
    if dropweightsuits == 1:
        inputgeodf.drop(suitfields, axis=1, inplace=True)
    return inputgeodf


def Envelope(inputgeodf):
    bnd = inputgeodf.total_bounds
    return Polygon([[bnd[0], bnd[1]], [bnd[2], bnd[1]], [bnd[2], bnd[3]], [bnd[0], bnd[3]]])


def WithinDistance(distance, inputgeodf, inputuniqueid, sourcegeodf, sourcename):
    """Using GeoPandas spatial join to test whether one GeoDataFrame is within a certain distance of another GeoDataFrame

    :param int distance: distance in source unit
    :param inputgeodf: input GeoDataFrame, features need to be checked
    :param inputuniqueid: uniqueid field
    :param sourcegeodf: source GeoDataFrame, features within specified distance to input GeoDataFrame
    :param sourcename: name of source. used for the column name of the returned GeoDataFrame
    :return: input GeoDataFrame with a new field indicating whether source features are presented within the specified distance
    :rtype: GeoDataFrame
    """
    inputgeodf.loc[:, sourcename] = 0
    inputwithin = gpd.sjoin(
        inputgeodf, sourcegeodf.assign(geom=sourcegeodf.buffer(distance)),
        how='inner', op='intersects'
    )[inputuniqueid]
    inputgeodf.loc[inputgeodf[inputuniqueid].isin(inputwithin), sourcename] = 1
    return inputgeodf


def Erase(inputgeodf, erasegeodf):
    """ Erase erasegeodf from inputgeodf

    :param GeoDataFrame inputgeodf: polygon GeoDataFrame parts remained in result
    :param GeoDataFrame erasegeodf: polygon GeoDataFrame parts erased from result
    :return: areas in inputgeodf but not in erasegeodf
    :rtype: GeoDataFrame
    """
    return gpd.overlay(inputgeodf, gpd.GeoDataFrame({'geometry': erasegeodf.unary_union}), how='difference')


def Identity(inputgeodf, identitygeodf, fieldlist):
    """ Using spatial join functionality of GeoPandas to examine whether features in two GeoDataFrames intersects with each other

    :param GeoDataFrame inputgeodf: input GeoDataFrame
    :param GeoDataFrame identitygeodf: identity GeoDataFrame
    :param list fieldlist: a list of column names from identity GeoDataFrame that are joined to the result
    :return: GeoDataFrame contains all attributes in input GeoDataFrame and the desired information from identity GeoDataFrame
    :rtype: GeoDataFrame
    """
    fieldlist.append(identitygeodf.geometry.name)
    return gpd.sjoin(inputgeodf, identitygeodf[fieldlist], how='left', op='intersects')


def ReclassifyByValue(inputgeodf, inputfield, newfield, rcldict, novalue=1):
    """Reclassify values by categories provided in the dictionary.

    :param GeoDataFrame inputgeodf: The input GeoDataFrame
    :param str inputfield: column name which contains the original values
    :param str newfield: new column name
    :param dict rcldict: the dictionary contains the old classes and new classes
    :param int novalue: value that nodata will be given to
    :return: the reclassified GeoDataFrame
    :rtype: GeoDataFrame
    """
    inputgeodf[newfield] = novalue
    for key in rcldict.keys():
        inputgeodf.loc[inputgeodf[inputfield].str.upper() == key.upper(), newfield] = rcldict[key]
    return inputgeodf


def ZonalStats_Raster(zonegeodf, raster, stats, reclasscsv=None):
    if reclasscsv is not None:
        reclassraster = ReclassifyRasterArray(raster, reclasscsv)
        zonaloutput = zonal_stats(vectors=zonegeodf.geometry, raster=reclassraster[0], affine=reclassraster[1], nodata=reclassraster[2],
                                  stats=stats, all_touched=True)
    else:
        src = rasterio.open(raster)
        arr = src.read(1)
        zonaloutput = zonal_stats(zonegeodf, arr, affine=src.meta['transform'], nodata=src.nodata, stats=stats)
    return zonegeodf.join(pd.DataFrame(zonaloutput))


def ZonalStatsRasterArray(zonegeodf, rasterarr, transaffine, stats, nodatavalue=0):
    """use rasterstats zonal_stats to return summarized statistics for zonegeodf

    :param GeoDataFrame zonegeodf: input zonal GeoDataFrame, units in which statistics of raster values are calculated
    :param ndarray rasterarr: numpy array of the relevant raster
    :param affine object transaffine: an affine object used to transform numpy array to raster
    :param list stats: list of statistics
    :return: GeoDataFrame contains the statistics for each feature in the zonegeodf
    :rtype: GeoDataFrame
    """
    zonaloutput = zonal_stats(vectors=zonegeodf.geometry, raster=rasterarr, nodata=nodatavalue, affine=transaffine, stats=stats, all_touched=True)
    indexname = 'index' if zonegeodf.index.name is None else zonegeodf.index.name
    zonegeodf.reset_index(inplace=True)
    output = zonegeodf.join(pd.DataFrame(zonaloutput))
    output.set_index(indexname, inplace=True)
    return output


def ReclassifyRasterArray(inputarray, reclassdf):
    """Reclassify raster array by pre-defined dataframe with related pairs of original and transformed values

    :param ndarray inputarray: input numpy array
    :param DataFrame reclassdf: DataFrame contains the transformation information
    :return: a transformed numpy array based on the reclassdf
    :rtype: ndarray
    """
    arrshape = inputarray.shape
    arrdf = pd.DataFrame(inputarray.reshape(-1), columns=['class'])
    reclassdf['class'] = reclassdf['class'].astype(int)
    arrsuitjoin = arrdf.join(reclassdf.set_index('class'), on='class', how='left')
    return np.array(arrsuitjoin['suit_score']).reshape(arrshape)


def Closest(input_centroid, nearfeature_centroids, method):
    centroid = np.asarray(input_centroid)
    deltas = nearfeature_centroids - centroid
    if method.lower() == 'euclidean':
        dist = np.sqrt(np.einsum('ij,ij->i', deltas, deltas))
        return np.min(dist)
    elif method.lower() == 'manhattan':
        dist = np.sqrt(np.einsum('i,i->i', deltas[:, 0], deltas[:, 0])) + np.sqrt(np.einsum('i,i->i', deltas[:, 1], deltas[:, 1]))
        return np.min(dist)
    else:
        raise ValueError('a valid method is either "Euclidean", or "Manhattan".')


def ToPointDistance(inputgeodf, pointgeodf, method):
    """
    Description:
    Find the nearest neighbor of each feature in the inputgeodf from features in pointgeodf. Return the distances.
    Parameters:
        inputgeodf: input geopandas dataframe. Centroids of inputgeodf are used to calculate distances
        pointgeodf: point geopandas dataframe. Centroids of polygons will be used if polygon data is provided
        method: 'manhattan' or 'euclidean'
    Return:
    a numpy array of distances of each feature to its nearest neighbor
    """
    dist_method = 1 if method.lower() == 'manhattan' else 2
    input_centroid_array = np.column_stack((inputgeodf.centroid.x.values, inputgeodf.centroid.y.values))
    point_array = np.column_stack((pointgeodf.centroid.x.values, pointgeodf.centroid.y.values))
    point_tree = spatial.cKDTree(point_array)
    return point_tree.query(input_centroid_array, p=dist_method)[0]


def ToLineDistance(inputgeodf, linegeodf, cellsize, method, shapetoarray=None):
    """
    Description:
    Find the nearest neighbor of each feature in the inputgeodf from features in linegeodf. Return the distances.
    linegeodf is burned into numpy array to calculate distance to line features.
    Parameters:
        inputgeodf: input geopandas dataframe. Centroids of inputgeodf are used to calculate distances
        linegeodf: line geopandas dataframe. Will be burned into numpy array
        cellsize: The cell size used to burn linegeodf to numpy array
        method: 'manhattan' or 'euclidean'
        shapetoarray: the numpy array to use, if a desired numpy array has already been created
    Return:
    a numpy array of distances of each feature to its nearest neighbor
    """
    if shapetoarray is None:
        line_data = ShapeToArray(linegeodf, cellsize)
    else:
        line_data = shapetoarray
    line_tree = spatial.cKDTree(np.argwhere(line_data[0] > 0))
    input_rowcol = np.column_stack((np.round((line_data[2][3] - inputgeodf.centroid.y)/cellsize).values,
                                    np.round((inputgeodf.centroid.x - line_data[2][0])/cellsize).values))
    dist_method = 1 if method.lower() == 'manhattan' else 2
    return line_tree.query(input_rowcol, p=dist_method)[0]*cellsize


def ToPolygonDistance(inputgeodf, polygongeodf, cellsize, method, shapetoarray=None):
    """
    Description:
    Find the nearest neighbor of each feature in the inputgeodf from features in polygongeodf. Return the distances.
    Geometries of polygongeodf are first transformed to their outerings/boundaries, then burned into numpy array to calculate distance to polygon features.
    Parameters:
        inputgeodf: input geopandas dataframe. Centroids of inputgeodf are used to calculate distances
        polygongeodf: polygon geopandas dataframe. Will be burned into numpy array
        cellsize: The cell size used to burn polygongeodf to numpy array
        method: 'manhattan' or 'euclidean'
        shapetoarray: the numpy array to use, if a desired numpy array has already been created
    Return:
    a numpy array of distances of each feature to its nearest neighbor
    """
    if shapetoarray is None:
        polygongeodf.geometry = polygongeodf.boundary
        polygon_data = ShapeToArray(polygongeodf, cellsize)
    else:
        polygon_data = shapetoarray
    polygon_tree = spatial.cKDTree(np.argwhere(polygon_data[0] > 0))
    input_rowcol = np.column_stack((np.round((polygon_data[2][3] - inputgeodf.centroid.y)/cellsize).values,
                                    np.round((inputgeodf.centroid.x - polygon_data[2][0])/cellsize).values))
    dist_method = 1 if method.lower() == 'manhattan' else 2
    return polygon_tree.query(input_rowcol, p=dist_method)[0]*cellsize


def ToRasterDistance(inputgeodf, rastername, rasterconn, value, method):
    """
    Calculate nearest neighbor distance to cells of a specific value in PostGIS raster

    :param GeoDataFrame inputgeodf: vector geopandas dataframe from which distance will be calculated
    :param string rastername: name of the raster, raster to which distance will be calculated
    :param string rasterconn: connection string to PostGIS database
    :param int value: the value of interest
    :param string method: a string to specify either manhattan distance or euclidean distance
    :return: return a numpy array of each feature in inputgeodf to its nearest raster cell neighbor
    :rtype: ndarray
    """
    raster_data = PostgresRasterToArray(rastername, rasterconn)
    raster_tree = spatial.cKDTree(np.argwhere(raster_data[0] == value))
    input_rowcol = np.column_stack((np.round((raster_data[1][5] - inputgeodf.centroid.y)/raster_data[1][0]).values,
                                    np.round((inputgeodf.centroid.x - raster_data[1][2])/raster_data[1][0]).values))
    dist_method = 1 if method.lower() == 'manhattan' else 2
    return raster_tree.query(input_rowcol, p=dist_method)[0]*raster_data[1][0]


def SummaryPointToPointDistance(inputgeodf, neargeodf, stats, inplace=False, distfieldname='dist'):
    availstats = {'mean': np.mean, 'sum': np.sum, 'max': np.max, 'min': np.min, 'std': np.std, 'median': np.median}
    output = {}
    series = ToPointDistance(inputgeodf, neargeodf, 'euclidean')
    for statistic in stats:
        output[statistic] = availstats[statistic](series)
    if inplace is True:
        inputgeodf[distfieldname] = series
    return output


def SummaryPointToLineDistance(input_geodf, line_geodf, stats, cellsize, method, inplace=False, distfieldname='dist'):
    # Calculate Statistics for distance between input features and the line features
    # statistics returns in a dictionary
    availstats = {'mean': np.mean, 'sum': np.sum, 'max': np.max, 'min': np.min, 'std': np.std, 'median': np.median}
    output = {}
    series = ToLineDistance(input_geodf, line_geodf, cellsize, method)
    for statistic in stats:
        output[statistic] = availstats[statistic](series)
    if inplace is True:
        input_geodf[distfieldname] = series
    return output


def SummaryDistance(inputgeodf, neargeodf, stats, inplace=False, distfieldname='dist', cellsize=25, method='euclidean'):
    availstats = {'mean': np.mean, 'sum': np.sum, 'max': np.max, 'min': np.min, 'std': np.std, 'median': np.median}
    output = {}
    if neargeodf.geometry.type[0] == 'LineString':
        series = ToLineDistance(inputgeodf, neargeodf, cellsize, method)
    else:
        series = ToPointDistance(inputgeodf, neargeodf, method)
    for statistic in stats:
        output[statistic] = availstats[statistic](series)
    if inplace is True:
        inputgeodf[distfieldname] = series
    return output


def LinearRescale(inputgeodf, oldfield, newfield, oldmin, oldmax, newmin, newmax):
    oldinterval = abs(oldmax - oldmin)
    newinterval = abs(newmax - newmin)
    if oldmax > oldmin:      # suitability increase as being further away from the object
        inputgeodf[newfield] = (inputgeodf[oldfield] - oldmin) * newinterval / oldinterval + newmin
        inputgeodf.loc[inputgeodf[oldfield] > oldmax, newfield] = newmax
        inputgeodf.loc[inputgeodf[oldfield] < oldmin, newfield] = newmin
    else:                    # suitability increase as being closer to the object
        inputgeodf[newfield] = newmax - (inputgeodf[oldfield] - oldmax) * newinterval / oldinterval
        inputgeodf.loc[inputgeodf[oldfield] < oldmax, newfield] = newmax
        inputgeodf.loc[inputgeodf[oldfield] > oldmin, newfield] = newmin
    return inputgeodf


def PointToPointLinearRescale(origin_geodf, destination_geodf, suit_for, suit_type, outputgeodf=None):
    distfield = suit_for + '_dist'
    suitfield = suit_for + '_distsuit'
    if outputgeodf is None:
        zstats = SummaryPointToPointDistance(origin_geodf, destination_geodf, ['min', 'max', 'mean'], inplace=True, distfieldname=distfield)
        outputgeodf = origin_geodf
    else:
        zstats = SummaryPointToPointDistance(origin_geodf, destination_geodf, ['min', 'max', 'mean'], distfieldname=distfield)
        outputgeodf[distfield] = ToPointDistance(outputgeodf, destination_geodf, 'euclidean')
    if suit_type.upper() == 'NEAR':
        LinearRescale(outputgeodf, distfield, suitfield, zstats['max'], zstats['mean'], 1, 9)
    else:
        LinearRescale(outputgeodf, distfield, suitfield, zstats['min'], zstats['mean'], 1, 9)
    return outputgeodf


def PointToLineLinearRescale(origin_geodf, destination_geodf, suit_for, suit_type, cellsize, method, outputgeodf=None):
    distfield = suit_for + '_dist'
    suitfield = suit_for + '_distsuit'
    if outputgeodf is None:
        zstats = SummaryPointToLineDistance(origin_geodf, destination_geodf, ['min', 'max', 'mean'], cellsize, method, inplace=True, distfieldname=distfield)
        outputgeodf = origin_geodf
    else:
        zstats = SummaryPointToLineDistance(origin_geodf, destination_geodf, ['min', 'max', 'mean'], cellsize, method, distfieldname=distfield)
        outputgeodf[distfield] = ToLineDistance(outputgeodf, destination_geodf, cellsize, method)
    if suit_type.upper() == 'NEAR':
        LinearRescale(outputgeodf, distfield, suitfield, zstats['max'], zstats['mean'], 1, 9)
    else:
        LinearRescale(outputgeodf, distfield, suitfield, zstats['min'], zstats['mean'], 1, 9)
    return outputgeodf


def ReclassifyByPointDistance(input_geodf, target_geodf, suit_for, classify_csv):
    suitfield = suit_for + '_suit'
    input_geodf[suitfield] = 0
    distfield = suit_for + '_dist'
    classify = pd.read_csv(classify_csv)
    input_geodf[distfield] = ToPointDistance(input_geodf, target_geodf, 'euclidean')
    if pd.isna(classify['lower'].iloc[0]) or pd.isna(classify['upper'].iloc[-1]):
        input_geodf.loc[input_geodf[distfield] < classify['upper'].iloc[0], [suitfield]] = classify['suit_score'].iloc[0]
        for level in range(1, len(classify['suit_score'] - 1)):
            input_geodf.loc[(input_geodf[distfield] >= classify['lower'].iloc[level])
                            & (input_geodf[distfield] < classify['upper'].iloc[level]), [suitfield]] = classify['suit_score'].iloc[level]
        input_geodf.loc[input_geodf[distfield] >= classify['lower'].iloc[-1], [suitfield]] = classify['suit_score'].iloc[-1]
    input_geodf.drop([distfield], axis=1, inplace=True)
    return input_geodf


def RescaleByInterval(inputgdf, rescaledf, rescalefield, fillnaval=1):
    """
    Rescale original data by specified intervals with new values

    :param GeoDataFrame inputgdf: input GeoDataFrame with data
    :param DataFrame rescaledf: DataFrame with the intervals and new values
    :param string rescalefield: the field needs to be rescaled
    :param int fillnaval: value used to fill the pandas series if null value exists
    :return: rescaled values
    :rtype: pandas Series
    """
    close = 'left' if rescaledf.iloc[0, 0] == 0 else 'right'
    rescale_interval = pd.IntervalIndex.from_arrays(rescaledf.iloc[:, 0], rescaledf.iloc[:, 1], closed=close)
    rescale_series = pd.cut(inputgdf[rescalefield], rescale_interval).cat.\
        rename_categories(np.asarray(rescaledf.iloc[:, 2]))
    if rescale_series.isna().values.any():
        rescale_series.fillna(fillnaval, inplace=True)
    else:
        pass
    return rescale_series.astype(int)


def ShapeToArray(inputgeodf, cellsize, valfield=None, fillval=0):
    """Transform vector data into numpy arrays. Locations that vector data presents are valued to 1, otherwise 0.

    :param GeoDataFrame inputgeodf: input GeoDataFrame
    :param int cellsize: cell size used to transform the vector data
    :param string valfield: the field contains values for each cell in the output array
    :param int fillval: fill value for all areas not covered by the input geometries
    :return: numpy array, affine object, and extent of the original vector data
    """
    extent = inputgeodf.total_bounds
    outshape = (int(round((extent[3] - extent[1]) / cellsize)), int(round((extent[2] - extent[0]) / cellsize)))
    trans = Affine(cellsize, 0, extent[0], 0, -cellsize, extent[3])
    if valfield is None:
        arr = features.rasterize(inputgeodf[inputgeodf.geometry.name], out_shape=outshape, fill=fillval, transform=trans)
    else:
        arr = features.rasterize(tuple(zip(inputgeodf[inputgeodf.geometry.name], inputgeodf[valfield])), out_shape=outshape, fill=fillval, transform=trans)
    return arr, trans, extent


def PostgresRasterToArray(rastername, rasterconn, nodata=None):
    """raster data in PostgreSQL database to numpy array

    :param str rastername: the name of the raster
    :param str rasterconn: connection string to postgis database
    :param float nodata: value used for nodata cells
    :return: a numpy array converted from the raster, the affine object, and nodata value
    """
    rasterconn = rasterconn + " table=\'" + rastername + "\'"
    raster = gdal.Open(rasterconn)
    rasterband = raster.GetRasterBand(1)
    rastertrans = raster.GetGeoTransform()
    rasterarr = rasterband.ReadAsArray()
    if nodata is not None:
        rasterarr[np.where(rasterarr == rasterband.GetNoDataValue())] = nodata
    return rasterarr, Affine(rastertrans[1], rastertrans[2], rastertrans[0], rastertrans[4], rastertrans[5], rastertrans[3]), nodata


def ZonalShapeDensity(shapegdf, zonegdf, cellsize, shapetoarray=None):
    # works only under following situations, future upgrades to accommodate other units
    # density unit: square mile
    # input unit: meters

    """
    :param GeoDataFrame shapegdf: input shape features can be either line or polygon features
    :param GeoDataFrame zonegdf: zone GeoDataFrame units to collect statistics
    :param int cellsize: the cell size used to convert vector to raster
    :param obj shapetoarray: the numpy array to use, if a desired numpy array has already been created
    :return: a pandas series that contains the density value for each zone feature
    :rtype: Series
    """
    if shapetoarray is None:
        indata = ShapeToArray(shapegdf, cellsize)
    else:
        indata = shapetoarray
    zonegdf = zonegdf.join(pd.DataFrame(zonal_stats(zonegdf.centroid.buffer(907.9751), indata[0],
                                                    affine=indata[1], stats=['sum'], nodata=0),
                                        index=zonegdf.index))
    return zonegdf['sum'] * 1.4142 * cellsize / 1609.344


def ZonalPointDensity(pointgeodf, zonegeodf):
    """density unit: counts/square mile; input unit: meters

    :param GeoDataFrame pointgeodf: point GeoDataFrame
    :param GeoDataFrame zonegeodf: zone GeoDataFrame units for counting point features
    :return: a pandas series containing density for each feature in zonegeodf
    :rtype: Series
    """
    zonegeodf.geometry = zonegeodf.centroid.buffer(907.9751)
    jointdf = gpd.sjoin(zonegeodf, pointgeodf, how='inner', op='intersects')
    countseries = jointdf.groupby(zonegeodf.index.values)['index_right'].count()
    return pd.concat([zonegeodf, countseries], axis=1)['index_right'].fillna(0)


def RescaleByGammaDistribution(inputgeodf, inputfield, newfield, newscale=9):
    """Transform original scale to 1 to 9 by using gamma distribution. Maximum Likelihood Estimation
    is used first to estimate the parameters for the distribution, then the estimated cdf transform
    the original field to 1 to 9.

    :param GeoDataFrame inputgeodf: input GeoDataFrame contains the field needs to be transformed
    :param str inputfield: column name
    :param str newfield: new column name contains the transformed values
    :param int newscale: new scale
    :return: a GeoDataFrame that contains the transformed values and other columns/* Please do not use this import in production.
    :rtype: GeoDataFrame
    """
    df = inputgeodf[~np.isnan(inputgeodf[inputfield])]
    a, loc, b = gamma.fit(df[inputfield], floc=0)
    inputgeodf[newfield] = 1
    inputgeodf.loc[~np.isnan(inputgeodf[inputfield]), newfield] = gamma.cdf(df[inputfield], a, loc=0, scale=b) * (newscale - 2) + 2
    return inputgeodf


def IDW(inputgeodf, valuegeodf, valueclm):
    """ Use Shepard's method for inverse distance weighting calculation, utilize photutils package in Astropy

    :param GeoDataFrame inputgeodf: GeoDataFrame that contains the location information for interpolation
    :param GeoDataFrame valuegeodf: GeoDataFrame that contains the value of the points used in IDW
    :param str valueclm: name of the column in valuegeodf that contains the values
    :return: numpy array of the values caluclated by IDW
    :rtype: ndarray
    """
    position = np.column_stack((valuegeodf.centroid.x, valuegeodf.centroid.y))

    idwfunction = idw(position, valuegeodf[valueclm])
    return idwfunction(np.column_stack((inputgeodf.centroid.x, inputgeodf.centroid.y)))


def SelectByLocation(inputgeodf, selectgeodf, how='inner', op='intersects', buffer=0):
    """Select input features based on spatial relationship with selection features

    :param GeoDataFrame inputgeodf: input GeoDataFrame
    :param GeoDataFrame selectgeodf: Selection GeoDataFrame
    :param str op: (optional) operational relationship. "intersects", "contains", or "within"
    :param int buffer: (optional) buffering distance
    :return: Part of input GeoDataFrame that satisfies the spatial relationship
    :rtype: GeoDataFrame
    """
    if buffer == 0:
        output_geodf = inputgeodf.loc[inputgeodf.index.to_series().isin(gpd.sjoin(inputgeodf, selectgeodf, how=how, op=op).index.values), :]
    else:
        selectgeodf[selectgeodf.geometry.name] = selectgeodf.buffer(buffer)
        output_geodf = inputgeodf.loc[inputgeodf.index.to_series().isin(gpd.sjoin(inputgeodf, selectgeodf, how=how, op=op).index.values), :]
    del output_geodf.index.name
    return output_geodf.copy()


def CallRunScript(path, script):
    mod_name = script.split('_')[0].lower() + script.split('_')[-1].split('.')[0]
    spec = implib.spec_from_file_location(mod_name, path + '\\' + script)
    mod = implib.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return [mod, mod_name]


def Organization(path):
    scripts = os.listdir(path)
    organization = {}
    a=b=c=1
    for goal in list(filter(lambda _: not bool(re.search(r'Obj\d\d', _)), scripts)):
        organization['Goal' + str(a)] = [path + goal, {}]
        obj_regex = r'Obj' + str(a) + r'\d'
        for obj in list(filter(lambda _: (not bool(re.search(r'SubObj\d\d\d', _))) and (bool(re.search(obj_regex, _))), scripts)):
            organization['Goal' + str(a)][1]['Obj' + str(a) + str(b)] = [path + obj, {}]
            subobj_regex = r'SubObj' + str(a) + str(b) + r'\d'
            for subobj in list(filter(lambda _: bool(re.search(subobj_regex, _)), scripts)):
                organization['Goal' + str(a)][1]['Obj' + str(a) + str(b)][1]['SubObj' + str(a) + str(b) + str(c)] = subobj
                c += 1
            b += 1
        a += 1
    return organization

