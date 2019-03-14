from LUCIS_BaseCoreFunctions import *
from settings import *


def PhysicalNoiseQuality(inputgdfsql, noisebarriersql, lucode):
    noise_barrier_dict = PostgresTableToDict(noisebarriersql, connect_class())

    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    airport_gdf = gpd.GeoDataFrame.from_postgis(sql=airport_noise_data, con=connect_spatial())
    industry_gdf = gpd.GeoDataFrame.from_postgis(sql="".join(ind_parcel_data.split(parcel_uniqueid+', ')),
                                                 con=connect_spatial())
    racetrack_gdf = gpd.GeoDataFrame.from_postgis(sql=racetrack_parcel_data, con=connect_spatial())
    cntbnd_gdf = gpd.GeoDataFrame.from_postgis(sql=cntbnd_data, con=connect_spatial())
    majrds_gdf = SelectByLocation(gpd.GeoDataFrame.from_postgis(sql=majrds_wointerstate_data,
                                                                con=connect_spatial()),
                                  cntbnd_gdf)  # excluding interstate highways
    majhwys_gdf = SelectByLocation(gpd.GeoDataFrame.from_postgis(sql=majhwys_onlyinterstate_data,
                                                                 con=connect_spatial()),
                                   cntbnd_gdf)  # interstate highways only
    rail_gdf = SelectByLocation(gpd.GeoDataFrame.from_postgis(sql=rail_noise_data, con=connect_spatial()), cntbnd_gdf)
    railxing_gdf = SelectByLocation(gpd.GeoDataFrame.from_postgis(sql=railxing_noise_data,
                                                                  con=connect_spatial()), cntbnd_gdf)

    for key in noise_barrier_dict.keys():
        WithinDistance(noise_barrier_dict[key], input_gdf, parcel_uniqueid, locals()[key + '_gdf'], key + '_noise')

    input_gdf['noise_score'] = input_gdf['airport_noise'] + input_gdf['industry_noise'] + \
        input_gdf['racetrack_noise'] + input_gdf['majrds_noise'] + input_gdf['majhwys_noise'] + \
        input_gdf['rail_noise'] * input_gdf['railxing_noise']

    return pd.Series((-4/3) * input_gdf['noise_score'].values + 9,
                     index=input_gdf[parcel_uniqueid], name=lucode+'_noisequality')


def PhysicalSoilQuality(inputgdfsql, soilcorrwgtsql, soildrainsuitsql, soilcorrsuitsql, lucode):
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    soil_gdf = gpd.GeoDataFrame.from_postgis(sql=soil_data, con=connect_spatial())

    input_gdf['geom'] = input_gdf.centroid
    soilcorr_wdf = pd.read_sql(soilcorrwgtsql, connect_class(), index_col='class')
    soilcorr_sdf = pd.read_sql(soilcorrsuitsql, connect_class())
    soildrain_sdf = pd.read_sql(soildrainsuitsql, connect_class())
    ReclassifyByValue(soil_gdf, 'drainagecl', 'drain_suit',
                      dict(zip(soildrain_sdf.iloc[:, 0], soildrain_sdf.iloc[:, 1])), novalue=1)
    ReclassifyByValue(soil_gdf, 'corcon', 'corcon_suit',
                      dict(zip(soilcorr_sdf.iloc[:, 0], soilcorr_sdf.iloc[:, 1])), novalue=1)
    ReclassifyByValue(soil_gdf, 'corsteel', 'corstl_suit',
                      dict(zip(soilcorr_sdf.iloc[:, 0], soilcorr_sdf.iloc[:, 1])), novalue=1)

    WeightedSum(soil_gdf, ['corcon_suit', 'corstl_suit'], soilcorr_wdf, 'corr_suit')
    soil_gdf['soilquality_suit'] = soil_gdf[['drain_suit', 'corr_suit']].mean(axis=1)

    return pd.Series(Identity(input_gdf, soil_gdf, ['soilquality_suit'])['soilquality_suit'].values,
                     index=input_gdf[parcel_uniqueid], name=lucode+'_soilquality')


def PhysicalFlooding(inputgdfsql, floodsoilsuitsql, floodgfchabsuitsql, floodwgtsql, lucode):
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    soil_gdf = gpd.GeoDataFrame.from_postgis(sql=floodsoil_data, con=connect_spatial())

    input_gdf[input_gdf.geometry.name] = input_gdf.centroid
    floodsoil_sdf = pd.read_sql(floodsoilsuitsql, connect_class())
    ReclassifyByValue(soil_gdf, 'flodfreqdc', 'floodsoil_suit',
                      dict(zip(floodsoil_sdf.iloc[:, 0], floodsoil_sdf.iloc[:, 1])), novalue=1)
    input_gdf['floodsoil_suit'] = Identity(input_gdf, soil_gdf, ['floodsoil_suit'])['floodsoil_suit']

    arr_data = PostgresRasterToArray(gfchabraster, rasterconn)
    floodgfchab_sdf = pd.read_sql(floodgfchabsuitsql, connect_class())
    input_gdf['floodgfchab_suit'] = ZonalStatsRasterArray(input_gdf, ReclassifyRasterArray(arr_data[0],
                                                                                           floodgfchab_sdf),
                                                          arr_data[1], ['mean'])['mean']
    flood_wdf = pd.read_sql(floodwgtsql, connect_class(), index_col='class')
    WeightedSum(input_gdf, ['floodsoil_suit', 'floodgfchab_suit'], flood_wdf, 'flooding_suit')
    return pd.Series(input_gdf['flooding_suit'].values, index=input_gdf[parcel_uniqueid], name=lucode+'_flooding')


def PhysicalAirQuality(inputgdfsql, exstgdfsql, lucode):
    exst_gdf = gpd.GeoDataFrame.from_postgis(sql=exstgdfsql, con=connect_spatial())
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    cntbnd_gdf = gpd.GeoDataFrame.from_postgis(sql=cntbnd_data, con=connect_spatial())

    sewtrt_gdf = SelectByLocation(gpd.GeoDataFrame.from_postgis(sql=sewagetrt_data, con=connect_spatial()), cntbnd_gdf)
    intag_gdf = gpd.GeoDataFrame.from_postgis(sql=intensag_parcel_data, con=connect_spatial())
    intindutil_gdf = gpd.GeoDataFrame.from_postgis(sql=intensindutil_parcel_data, con=connect_spatial())

    PointToPointLinearRescale(exst_gdf, sewtrt_gdf, 'sewtrt', 'FAR', input_gdf)
    PointToPointLinearRescale(exst_gdf, intag_gdf, 'intag', 'FAR', input_gdf)
    PointToPointLinearRescale(exst_gdf, intindutil_gdf, 'intutil', 'FAR', input_gdf)

    return pd.Series(input_gdf[['sewtrt_distsuit', 'intag_distsuit', 'intutil_distsuit']].min(axis=1).values,
                     index=input_gdf[parcel_uniqueid], name=lucode+'_airquality')


def PhysicalHazard(inputgdfsql, hazardsuperfundsuitsql, hazardstatesitessuitsql, hazardwgtsql, lucode):
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    superfund_gdf = gpd.GeoDataFrame.from_postgis(sql=superfund_data, con=connect_spatial())
    statesites_gdf = gpd.GeoDataFrame.from_postgis(sql=statehazsite_data, con=connect_spatial())

    input_gdf['superfunddist'] = ToPointDistance(input_gdf, superfund_gdf, 'euclidean')
    input_gdf['statesitesdist'] = ToPointDistance(input_gdf, statesites_gdf, 'euclidean')
    input_gdf['superfund_suit'] = RescaleByInterval(input_gdf,
                                                    pd.read_sql(hazardsuperfundsuitsql, con=connect_class()),
                                                    'superfunddist')
    input_gdf['statesites_suit'] = RescaleByInterval(input_gdf,
                                                     pd.read_sql(hazardstatesitessuitsql, con=connect_class()),
                                                     'statesitesdist')
    hazard_wdf = pd.read_sql(hazardwgtsql, con=connect_class(), index_col='class')
    WeightedSum(input_gdf, ['superfund_suit', 'statesites_suit'], hazard_wdf, 'hazard_suit')
    return pd.Series(input_gdf['hazard_suit'].values, index=input_gdf[parcel_uniqueid], name=lucode+'_hazard')


def ProximityServiceAmenities(inputgdfsql, exstgdfsql, schoolsyswgtsql, servicewgtsql, lucode):
    exst_gdf = gpd.GeoDataFrame.from_postgis(sql=exstgdfsql, con=connect_spatial())
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())

    fireresque_gdf = gpd.GeoDataFrame.from_postgis(sql=fireresque_data, con=connect_spatial())
    police_gdf = gpd.GeoDataFrame.from_postgis(sql=police_data, con=connect_spatial())
    hospital_gdf = gpd.GeoDataFrame.from_postgis(sql=hospital_data, con=connect_spatial())
    PointToPointLinearRescale(exst_gdf, fireresque_gdf, 'fireresque', 'NEAR', input_gdf)
    PointToPointLinearRescale(exst_gdf, police_gdf, 'police', 'NEAR', input_gdf)
    PointToPointLinearRescale(exst_gdf, hospital_gdf, 'hospital', 'NEAR', input_gdf)

    service_wdf = pd.read_sql(servicewgtsql, connect_class(), index_col='class')
    if 'school' in service_wdf.index.values:
        highschool_gdf = gpd.GeoDataFrame.from_postgis(sql=highschool_data, con=connect_spatial())
        middleschool_gdf = gpd.GeoDataFrame.from_postgis(sql=middleschool_data, con=connect_spatial())
        primaryschool_gdf = gpd.GeoDataFrame.from_postgis(sql=primaryschool_data, con=connect_spatial())
        PointToPointLinearRescale(exst_gdf, highschool_gdf, 'highschool', 'NEAR', input_gdf)
        PointToPointLinearRescale(exst_gdf, middleschool_gdf, 'middleschool', 'NEAR', input_gdf)
        PointToPointLinearRescale(exst_gdf, primaryschool_gdf, 'primaryschool', 'NEAR', input_gdf)
        school_wdf = pd.read_sql(schoolsyswgtsql, connect_class(), index_col='class')
        WeightedSum(input_gdf, ['highschool_distsuit', 'middleschool_distsuit', 'primaryschool_distsuit'],
                    school_wdf, 'school_distsuit')
        WeightedSum(input_gdf, ['school_distsuit', 'fireresque_distsuit', 'police_distsuit', 'hospital_distsuit'],
                    service_wdf, 'service_suit')
        return pd.Series(input_gdf['service_suit'].values, index=input_gdf[parcel_uniqueid], name=lucode+'_service')
    else:
        WeightedSum(input_gdf, ['fireresque_distsuit', 'police_distsuit', 'hospital_distsuit'],
                    service_wdf, 'service_suit')
        return pd.Series(input_gdf['service_suit'].values, index=input_gdf[parcel_uniqueid], name=lucode+'_service')


def ProximityTransitRoads(inputgdfsql, exstgdfsql, cellsize, transitroadswgtsql, lucode):
    exst_gdf = gpd.GeoDataFrame.from_postgis(sql=exstgdfsql, con=connect_spatial())
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    # combined_gdf = pd.concat([exst_gdf, input_gdf])
    majrds_gdf = gpd.GeoDataFrame.from_postgis(sql=majrds_data, con=connect_spatial())
    cntbnd_gdf = gpd.GeoDataFrame.from_postgis(sql=cntbnd_data, con=connect_spatial())
    selected_majrds_gdf = SelectByLocation(majrds_gdf, cntbnd_gdf)
    majrds_arr = ShapeToArray(selected_majrds_gdf, cellsize)
    input_gdf['majrdsdist'] = ToLineDistance(input_gdf, selected_majrds_gdf, cellsize, 'manhattan', majrds_arr)
    exst_series = ToLineDistance(exst_gdf, selected_majrds_gdf, cellsize, 'manhattan', majrds_arr)
    # combined_gdf['majrdsdist'] = ToLineDistance(combined_gdf, selected_majrds_gdf, cellsize, 'manhattan', majrds_arr)
    # print(combined_gdf[combined_gdf.index.duplicated()])
    # input_gdf['majrdsdist'] = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(input_gdf[parcel_uniqueid]),
    #                                            'majrdsdist']
    # exst_series = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(exst_gdf[parcel_uniqueid]), 'majrdsdist']
    dist_mean = np.mean(exst_series)
    dist_max = np.max(exst_series)
    LinearRescale(input_gdf, 'majrdsdist', 'majrdsdist_suit', dist_max, dist_mean, 1, 9)
    input_gdf['majrdsden'] = ZonalShapeDensity(selected_majrds_gdf, input_gdf, cellsize, majrds_arr)
    RescaleByGammaDistribution(input_gdf, 'majrdsden', 'majrdsden_suit')
    roads_wdf = pd.read_sql(transitroadswgtsql, connect_class(), index_col='class')
    WeightedSum(input_gdf, ['majrdsdist_suit', 'majrdsden_suit'], roads_wdf, 'transitroads_suit')
    return pd.Series(input_gdf['transitroads_suit'].values,
                     index=input_gdf[parcel_uniqueid], name=lucode+'_transitroads')


def ProximityConservationParksWater(inputgdfsql, exstgdfsql, cellsize, owsuitsql, consparkowwgtsql, lucode):
    exst_gdf = gpd.GeoDataFrame.from_postgis(sql=exstgdfsql, con=connect_spatial())
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    combined_gdf = pd.concat([exst_gdf, input_gdf])
    cntbnd_gdf = gpd.GeoDataFrame.from_postgis(sql=cntbnd_data, con=connect_spatial())

    flma_gdf = SelectByLocation(gpd.GeoDataFrame.from_postgis(sql=flma_data, con=connect_spatial()), cntbnd_gdf)
    combined_gdf['flmadist'] = ToPolygonDistance(combined_gdf, flma_gdf, cellsize, 'euclidean')
    input_gdf['flmadist'] = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(input_gdf[parcel_uniqueid]), 'flmadist']
    exst_dist = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(exst_gdf[parcel_uniqueid]), 'flmadist']
    dist_mean = np.mean(exst_dist)
    dist_max = np.max(exst_dist)
    input_gdf = LinearRescale(input_gdf, 'flmadist', 'flma_distsuit', dist_max, dist_mean, 1, 9)

    parks_gdf = gpd.GeoDataFrame.from_postgis(sql=parks_data, con=connect_spatial())
    combined_gdf['parksdist'] = ToPointDistance(combined_gdf, parks_gdf, 'euclidean')
    input_gdf['parksdist'] = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(input_gdf[parcel_uniqueid]),
                                              'parksdist']
    exst_dist = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(exst_gdf[parcel_uniqueid]), 'parksdist']
    dist_mean = np.mean(exst_dist)
    dist_max = np.max(exst_dist)
    input_gdf = LinearRescale(input_gdf, 'parksdist', 'parks_distsuit', dist_max, dist_mean, 1, 9)

    nhd24_gdf = SelectByLocation(gpd.GeoDataFrame.from_postgis(sql=nhdwaterbody_data, con=connect_spatial()), cntbnd_gdf)
    input_gdf['nhd24dist'] = ToPolygonDistance(input_gdf, nhd24_gdf, cellsize, 'euclidean')
    input_gdf['gfchabdist'] = ToRasterDistance(input_gdf, gfchabraster, rasterconn, gfchab_ow_value, 'euclidean')
    input_gdf['openwaterdist'] = np.where(input_gdf['nhd24dist'] < input_gdf['gfchabdist'],
                                          input_gdf['nhd24dist'], input_gdf['gfchabdist'])
    ow_sdf = pd.read_sql(owsuitsql, connect_class())
    input_gdf['openwater_distsuit'] = RescaleByInterval(input_gdf, ow_sdf, 'openwaterdist')

    consparkow_wdf = pd.read_sql(consparkowwgtsql, connect_class(), index_col='class')
    WeightedSum(input_gdf, ['flma_distsuit', 'parks_distsuit', 'openwater_distsuit'], consparkow_wdf, 'consparkow_suit')
    return pd.Series(input_gdf['consparkow_suit'].values, index=input_gdf[parcel_uniqueid], name=lucode+'_consparkow')


def ProximityShopping(inputgdfsql, exstgdfsql, lucode):
    exst_gdf = gpd.GeoDataFrame.from_postgis(sql=exstgdfsql, con=connect_spatial())
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    combined_gdf = pd.concat([exst_gdf, input_gdf])

    shop_gdf = gpd.GeoDataFrame.from_postgis(sql=ret_parcel_data, con=connect_spatial())
    combined_gdf['shopdist'] = ToPointDistance(combined_gdf, shop_gdf, 'manhattan')
    input_gdf['shopdist'] = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(input_gdf[parcel_uniqueid]), 'shopdist']
    exst_dist = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(exst_gdf[parcel_uniqueid]), 'shopdist']
    dist_mean = np.mean(exst_dist)
    dist_max = np.max(exst_dist)
    LinearRescale(input_gdf, 'shopdist', 'shop_suit', dist_max, dist_mean, 1, 9)
    return pd.Series(input_gdf['shop_suit'].values, index=input_gdf[parcel_uniqueid], name=lucode+'_shopping')


def ProximityExistingLandUse(inputgdfsql, exstgdfsql, exstcutyear, lucode):
    exstgeomix = exstgdfsql.find('geom')
    exstyrgdfsql = exstgdfsql[:exstgeomix] + yearbltfield + ', ' + exstgdfsql[exstgeomix:]
    exst_gdf = gpd.GeoDataFrame.from_postgis(sql=exstyrgdfsql, con=connect_spatial())
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    bfore_gdf = exst_gdf[exst_gdf[yearbltfield] < exstcutyear]
    after_gdf = exst_gdf[exst_gdf[yearbltfield] >= exstcutyear]

    aftertobfore_dist = ToPointDistance(after_gdf, bfore_gdf, 'euclidean')
    dist_mean = np.mean(aftertobfore_dist)
    dist_max = np.max(aftertobfore_dist)
    input_gdf['exstludist'] = ToPointDistance(input_gdf, exst_gdf, 'euclidean')
    LinearRescale(input_gdf, 'exstludist', 'exstlu_suit', dist_max, dist_mean, 1, 9)
    return pd.Series(input_gdf['exstlu_suit'].values, index=input_gdf[parcel_uniqueid], name=lucode+'_exstlu')


def ProximityInfrastructure(inputgdfsql, exstgdfsql, proxiinfraswgtsql, lucode):
    exst_gdf = gpd.GeoDataFrame.from_postgis(sql=exstgdfsql, con=connect_spatial())
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    combined_gdf = pd.concat([exst_gdf, input_gdf])

    wtreat_gdf = gpd.GeoDataFrame.from_postgis(sql=watertreat_data, con=connect_spatial())
    combined_gdf['wtreatdist'] = ToPointDistance(combined_gdf, wtreat_gdf, 'euclidean')
    input_gdf['wtreatdist'] = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(input_gdf[parcel_uniqueid]),
                                               'wtreatdist']
    exst_dist = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(exst_gdf[parcel_uniqueid]), 'wtreatdist']
    dist_mean = np.mean(exst_dist)
    dist_max = np.max(exst_dist)
    LinearRescale(input_gdf, 'wtreatdist', 'wtreatdist_suit', dist_max, dist_mean, 1, 9)

    sewtrt_gdf = gpd.GeoDataFrame.from_postgis(sql=sewagetrt_data, con=connect_spatial())
    combined_gdf['sewtrtdist'] = ToPointDistance(combined_gdf, sewtrt_gdf, 'euclidean')
    input_gdf['sewtrtdist'] = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(input_gdf[parcel_uniqueid]),
                                               'sewtrtdist']
    exst_dist = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(exst_gdf[parcel_uniqueid]), 'sewtrtdist']
    dist_mean = np.mean(exst_dist)
    dist_max = np.max(exst_dist)
    LinearRescale(input_gdf, 'sewtrtdist', 'sewtrtdist_suit', dist_max, dist_mean, 1, 9)
    infras_wdf = pd.read_sql(proxiinfraswgtsql, connect_class(), index_col='class')
    WeightedSum(input_gdf, ['wtreatdist_suit', 'sewtrtdist_suit'], infras_wdf, 'infrastructure_suit')
    return pd.Series(input_gdf['infrastructure_suit'].values,
                     index=input_gdf[parcel_uniqueid], name=lucode+'_infrastructure')


def ProximityEntertainment(inputgdfsql, exstgdfsql, cellsize, entertainwgtsql, lucode):
    exst_gdf = gpd.GeoDataFrame.from_postgis(sql=exstgdfsql, con=connect_spatial())
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    combined_gdf = pd.concat([exst_gdf, input_gdf])

    entertain_gdf = gpd.GeoDataFrame.from_postgis(sql=ent_parcel_data, con=connect_spatial())
    combined_gdf['entertaindist'] = ToPointDistance(combined_gdf, entertain_gdf, 'manhattan')
    input_gdf['entertaindist'] = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(input_gdf[parcel_uniqueid]),
                                                  'entertaindist']
    exst_dist = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(exst_gdf[parcel_uniqueid]), 'entertaindist']
    dist_mean = np.mean(exst_dist)
    dist_max = np.max(exst_dist)
    LinearRescale(input_gdf, 'entertaindist', 'entertaindist_suit', dist_max, dist_mean, 1, 9)
    input_gdf['entertainden'] = ZonalShapeDensity(entertain_gdf, input_gdf, cellsize)
    input_gdf = RescaleByGammaDistribution(input_gdf, 'entertainden', 'entertainden_suit')
    entertain_wdf = pd.read_sql(entertainwgtsql, connect_class(), index_col='class')
    WeightedSum(input_gdf, ['entertaindist_suit', 'entertainden_suit'], entertain_wdf, 'entertain_suit')
    return pd.Series(input_gdf['entertain_suit'].values, index=input_gdf[parcel_uniqueid], name=lucode+'_entertain')


def ProximityInverse(inputgdfsql, exstgdfsql, antigeodfsql, antiname, lucode):
    exst_gdf = gpd.GeoDataFrame.from_postgis(sql=exstgdfsql, con=connect_spatial())
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    combined_gdf = pd.concat([exst_gdf, input_gdf])

    anti_gdf = gpd.GeoDataFrame.from_postgis(sql=antigeodfsql, con=connect_spatial())
    combined_gdf['antidist'] = ToPointDistance(combined_gdf, anti_gdf, 'euclidean')
    input_gdf['antidist'] = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(input_gdf[parcel_uniqueid]), 'antidist']
    exst_dist = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(exst_gdf[parcel_uniqueid]), 'antidist']
    dist_mean = np.mean(exst_dist)
    dist_min = np.min(exst_dist)
    LinearRescale(input_gdf, 'antidist', 'anti_suit', dist_min, dist_mean, 1, 9)
    return pd.Series(input_gdf['anti_suit'].values, index=input_gdf[parcel_uniqueid], name=lucode+'_anti'+antiname)


def ProximityLandValue(inputgdfsql, exstgdfsql, lucode):
    inputgeomix = inputgdfsql.find('geom')
    inputlvgdfsql = inputgdfsql[:inputgeomix] + landvaluefield + ', ' + inputgdfsql[inputgeomix:]
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputlvgdfsql, con=connect_spatial())
    exstgeomix = exstgdfsql.find('geom')
    exstlvgdfsql = exstgdfsql[:exstgeomix] + landvaluefield + ', ' + exstgdfsql[exstgeomix:]
    exst_gdf = gpd.GeoDataFrame.from_postgis(sql=exstlvgdfsql, con=connect_spatial())
    input_gdf['dolperacre'] = input_gdf['jv'] / (input_gdf.area / sqmtoacre)
    exst_gdf['dolperacre'] = exst_gdf['jv'] / (exst_gdf.area / sqmtoacre)
    if np.any(input_gdf['dolperacre'] == 0) | np.any(exst_gdf['dolperacre'] == 0):
        landvalue_gdf = gpd.GeoDataFrame.from_postgis(
            sql="SELECT {}, geom FROM {} WHERE {} > 0".format(landvaluefield, parcel, landvaluefield),
            con=connect_spatial())
        landvalue_gdf['dolperacre'] = landvalue_gdf['jv'] / (landvalue_gdf.area / sqmtoacre)
        position = np.column_stack((landvalue_gdf.centroid.x, landvalue_gdf.centroid.y))
        idwfunction = idw(position, landvalue_gdf['dolperacre'])
        if np.any(input_gdf['dolperacre'] == 0):
            nolvinput_gdf = input_gdf.loc[input_gdf[landvaluefield] == 0]
            input_gdf['dolperacre'].iloc[input_gdf[input_gdf[landvaluefield] == 0].index.values] = idwfunction(
                np.column_stack((nolvinput_gdf.centroid.x, nolvinput_gdf.centroid.y)))
        if np.any(exst_gdf['dolperacre'] == 0):
            nolvexst_gdf = exst_gdf.loc[exst_gdf[landvaluefield] == 0]
            exst_gdf['dolperacre'].iloc[exst_gdf[exst_gdf[landvaluefield] == 0].index.values] = idwfunction(
                np.column_stack((nolvexst_gdf.centroid.x, nolvexst_gdf.centroid.y)))
    landvalue_mean = np.mean(exst_gdf['dolperacre'])
    landvalue_max = np.max(exst_gdf['dolperacre'])
    LinearRescale(input_gdf, 'dolperacre', 'landvalue_suit', landvalue_max, landvalue_mean, 1, 9)
    return pd.Series(input_gdf['landvalue_suit'].values, index=input_gdf[parcel_uniqueid], name=lucode+'_landvalue')


def PresentLandUse(inputgdfsql, gfchabsuitsql, yrsuitsql, actusewgtsql, lucode):
    inputgeomix = inputgdfsql.find('geom')
    inputyrgdfsql = inputgdfsql[:inputgeomix] + yearbltfield + ', ' + lucodefield + ', ' + inputgdfsql[inputgeomix:]
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputyrgdfsql, con=connect_spatial())
    gfchab_data = PostgresRasterToArray(gfchabraster, rasterconn)
    input_gdf['gfchab_suit'] = ZonalStatsRasterArray(input_gdf,
                                                     ReclassifyRasterArray(
                                                         gfchab_data[0],
                                                         pd.read_sql(gfchabsuitsql, connect_class())),
                                                     gfchab_data[1], ['mean'])['mean']
    input_gdf['yrblt_suit'] = RescaleByInterval(input_gdf, pd.read_sql(yrsuitsql, connect_class()), yearbltfield)
    input_gdf.loc[input_gdf[yearbltfield] == 0, 'yrblt_suit'] = 1
    "SELECT doruc FROM "
    input_gdf['landuse_suit'] = 1
    if lucode in ['mf', 'sf']:
        luseries = pd.read_sql("SELECT {} FROM def_parcel "
                               "WHERE classification = '{}' OR "
                               "classification = 'vacant residential'".format(lucodefield, lucisAllLuCode[lucode]),
                               connect_class())[lucodefield]
    elif lucode in ['com', 'ret', 'ser', 'ent']:
        luseries = pd.read_sql("SELECT {} FROM def_parcel "
                               "WHERE classification = '{}' OR "
                               "classification = 'vacant commercial'".format(lucodefield, lucisAllLuCode[lucode]),
                               connect_class())[lucodefield]
    elif lucode == 'ind':
        luseries = pd.read_sql("SELECT {} FROM def_parcel "
                               "WHERE classification = '{}' OR "
                               "classification = 'vacant industrial'".format(lucodefield, lucisAllLuCode[lucode]),
                               connect_class())[lucodefield]
    else:
        luseries = pd.read_sql("SELECT {} FROM def_parcel "
                               "WHERE classification = '{}' OR "
                               "classification = 'vacant institutional'".format(lucodefield, lucisAllLuCode[lucode]),
                               connect_class())[lucodefield]
    input_gdf.loc[input_gdf[lucodefield].isin(luseries), 'landuse_suit'] = 9
    actuse_wdf = pd.read_sql(actusewgtsql, connect_class(), index_col='class')
    WeightedSum(input_gdf, ['gfchab_suit', 'yrblt_suit', 'landuse_suit'], actuse_wdf, 'actualuse_suit')
    return pd.Series(input_gdf['actualuse_suit'].values, index=input_gdf[parcel_uniqueid], name=lucode+'_presentuse')


def LandUseGrowth(inputgdfsql, exstgdfsql, growthrangedict, lucode):
    exstgeomix = exstgdfsql.find('geom')
    exstyeargdfsql = exstgdfsql[:exstgeomix] + yearbltfield + ', ' + exstgdfsql[exstgeomix:]
    exst_gdf = gpd.GeoDataFrame.from_postgis(sql=exstyeargdfsql, con=connect_spatial())
    exst_gdf['area'] = exst_gdf.area / sqmtoacre
    exst_gdf[exst_gdf.geometry.name] = exst_gdf.centroid
    plss_gdf = gpd.GeoDataFrame.from_postgis(sql=plss_data, con=connect_spatial())
    plssexst_gdf = gpd.sjoin(plss_gdf, exst_gdf, how='inner', op='intersects')

    for growth in growthrangedict.keys():
        growthdata = plssexst_gdf.loc[(plssexst_gdf[yearbltfield] > growthrangedict[growth][0]) &
                                      (plssexst_gdf[yearbltfield] <= growthrangedict[growth][1])]
        plss_gdf = plss_gdf.join(growthdata.groupby(growthdata.index)['area'].sum()).rename(columns={'area': growth})
    plss_gdf.fillna(0, inplace=True)

    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    input_gdf['lugrowth'] = 0
    for growth in growthrangedict.keys():
        input_gdf['lugrowth'] = input_gdf['lugrowth'] + IDW(input_gdf, plss_gdf, growth) * growthrangedict[growth][2]
    input_gdf.loc[input_gdf['lugrowth'] == 0, 'lugrowth'] = np.min(input_gdf.loc[input_gdf['lugrowth'] != 0,
                                                                                 'lugrowth'])
    RescaleByGammaDistribution(input_gdf, 'lugrowth', 'lugrowth_suit')
    return pd.Series(input_gdf['lugrowth_suit'].values, index=input_gdf[parcel_uniqueid], name=lucode+'_lugrowth')


def ProximityRetail(inputgdfsql, exstgdfsql, cellsize, retailwgtsql, lucode):
    exst_gdf = gpd.GeoDataFrame.from_postgis(sql=exstgdfsql, con=connect_spatial())
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    combined_gdf = pd.concat([exst_gdf, input_gdf])

    retail_gdf = gpd.GeoDataFrame.from_postgis(sql=ret_parcel_data, con=connect_spatial())
    combined_gdf['retaildist'] = ToPointDistance(combined_gdf, retail_gdf, 'manhattan')
    input_gdf['retaildist'] = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(input_gdf[parcel_uniqueid]),
                                               'retaildist']
    exst_dist = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(exst_gdf[parcel_uniqueid]), 'retaildist']
    dist_mean = np.mean(exst_dist)
    dist_max = np.max(exst_dist)
    LinearRescale(input_gdf, 'retaildist', 'retaildist_suit', dist_max, dist_mean, 1, 9)
    input_gdf['retailden'] = ZonalShapeDensity(retail_gdf, input_gdf, cellsize)
    RescaleByGammaDistribution(input_gdf, 'retailden', 'retailden_suit')
    retail_wdf = pd.read_sql(retailwgtsql, connect_class(), index_col='class')
    WeightedSum(input_gdf, ['retaildist_suit', 'retailden_suit'], retail_wdf, 'retail_suit')
    return pd.Series(input_gdf['retail_suit'].values, index=input_gdf[parcel_uniqueid], name=lucode+'_retail')


def ProximityResidential(inputgdfsql, exstgdfsql, lucode):
    exst_gdf = gpd.GeoDataFrame.from_postgis(sql=exstgdfsql, con=connect_spatial())
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    combined_gdf = pd.concat([exst_gdf, input_gdf])

    res_gdf = gpd.GeoDataFrame.from_postgis(sql=res_parcel_data, con=connect_spatial())
    combined_gdf['resdist'] = ToPointDistance(combined_gdf, res_gdf, 'manhattan')
    input_gdf['resdist'] = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(input_gdf[parcel_uniqueid]), 'resdist']
    exst_dist = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(exst_gdf[parcel_uniqueid]), 'resdist']
    dist_mean = np.mean(exst_dist)
    dist_max = np.max(exst_dist)
    LinearRescale(input_gdf, 'resdist', 'res_suit', dist_max, dist_mean, 1, 9)
    return pd.Series(input_gdf['res_suit'].values, index=input_gdf[parcel_uniqueid], name=lucode+'_residential')


def ProximityInstitution(inputgdfsql, exstgdfsql, cellsize, institutionwgtsql, lucode):
    exst_gdf = gpd.GeoDataFrame.from_postgis(sql=exstgdfsql, con=connect_spatial())
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    combined_gdf = pd.concat([exst_gdf, input_gdf])

    institution_gdf = gpd.GeoDataFrame.from_postgis(sql=ins_parcel_data, con=connect_spatial())
    combined_gdf['institutiondist'] = ToPointDistance(combined_gdf, institution_gdf, 'manhattan')
    input_gdf['institutiondist'] = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(input_gdf[parcel_uniqueid]),
                                                    'institutiondist']
    exst_dist = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(exst_gdf[parcel_uniqueid]), 'institutiondist']
    dist_mean = np.mean(exst_dist)
    dist_max = np.max(exst_dist)
    LinearRescale(input_gdf, 'institutiondist', 'institutiondist_suit', dist_max, dist_mean, 1, 9)
    input_gdf['institutionden'] = ZonalShapeDensity(institution_gdf, input_gdf, cellsize)
    RescaleByGammaDistribution(input_gdf, 'institutionden', 'institutionden_suit')
    institution_wdf = pd.read_sql(institutionwgtsql, connect_class(), index_col='class')
    WeightedSum(input_gdf, ['institutiondist_suit', 'institutionden_suit'], institution_wdf, 'institution_suit')
    return pd.Series(input_gdf['institution_suit'].values, index=input_gdf[parcel_uniqueid], name=lucode+'_institution')


def ProximityCommercial(inputgdfsql, exstgdfsql, cellsize, commercialwgtsql, lucode):
    exst_gdf = gpd.GeoDataFrame.from_postgis(sql=exstgdfsql, con=connect_spatial())
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    combined_gdf = pd.concat([exst_gdf, input_gdf])

    commercial_gdf = gpd.GeoDataFrame.from_postgis(sql=com_parcel_data, con=connect_spatial())
    combined_gdf['commercialdist'] = ToPointDistance(combined_gdf, commercial_gdf, 'manhattan')
    input_gdf['commercialdist'] = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(input_gdf[parcel_uniqueid]),
                                                   'commercialdist']
    exst_dist = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(exst_gdf[parcel_uniqueid]), 'commercialdist']
    dist_mean = np.mean(exst_dist)
    dist_max = np.max(exst_dist)
    LinearRescale(input_gdf, 'commercialdist', 'commercialdist_suit', dist_max, dist_mean, 1, 9)
    input_gdf['commercialden'] = ZonalShapeDensity(commercial_gdf, input_gdf, cellsize)
    RescaleByGammaDistribution(input_gdf, 'commercialden', 'commercialden_suit')
    commercial_wdf = pd.read_sql(commercialwgtsql, connect_class(), index_col='class')
    WeightedSum(input_gdf, ['commercialdist_suit', 'commercialden_suit'], commercial_wdf, 'commercial_suit')
    return pd.Series(input_gdf['commercial_suit'].values, index=input_gdf[parcel_uniqueid], name=lucode+'_commercial')


def ProximityDensity(inputgdfsql, exstgdfsql, cellsize, lucode):
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())

    exst_gdf = gpd.GeoDataFrame.from_postgis(sql=exstgdfsql, con=connect_spatial())
    input_gdf['exstden'] = ZonalShapeDensity(exst_gdf, input_gdf, cellsize)
    input_gdf = RescaleByGammaDistribution(input_gdf, 'exstden', 'exstden_suit')
    return pd.Series(input_gdf['exstden_suit'].values, index=input_gdf[parcel_uniqueid], name=lucode+'_density')


def ProximityInterstate(inputgdfsql, exstgdfsql, cellsize, lucode):
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    exst_gdf = gpd.GeoDataFrame.from_postgis(sql=exstgdfsql, con=connect_spatial())
    combined_gdf = pd.concat([exst_gdf, input_gdf])

    cntbnd_gdf = gpd.GeoDataFrame.from_postgis(sql=cntbnd_data, con=connect_spatial())
    interstate_gdf = SelectByLocation(gpd.GeoDataFrame.from_postgis(sql=majhwys_onlyinterstate_data,
                                                                    con=connect_spatial()), cntbnd_gdf)
    interstate_arr = ShapeToArray(interstate_gdf, cellsize)
    combined_gdf['interstatedist'] = ToLineDistance(combined_gdf, interstate_gdf, cellsize, 'manhattan', interstate_arr)
    input_gdf['interstatedist'] = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(input_gdf[parcel_uniqueid]),
                                                   'interstatedist']
    exst_series = combined_gdf.loc[combined_gdf[parcel_uniqueid].isin(exst_gdf[parcel_uniqueid]), 'interstatedist']
    dist_mean = np.mean(exst_series)
    dist_max = np.max(exst_series)
    LinearRescale(input_gdf, 'interstatedist', 'interstatedist_suit', dist_max, dist_mean, 1, 9)
    return pd.Series(input_gdf['interstatedist_suit'].values,
                     index=input_gdf[parcel_uniqueid], name=lucode+'_interstate')


def UrbanFinal(paraoutput):
    urbout_dict = dict((item.name, item) for item in paraoutput if item.name.split('_')[0] in lucisUrbLuCode.keys())
    urbfinal_series = pd.Series(0, index=next(iter(urbout_dict.values())).index)
    urblucds = pd.read_sql("SELECT doruc, classification FROM def_parcel "
                           "WHERE classification IN ('{}')".format("', '".join(lucisUrbLuCode.values())),
                           connect_class())
    lucdsacres = pd.read_sql("SELECT doruc, SUM(acres) FROM vector.parcel2015_orange "
                             "WHERE doruc IN ('{}') GROUP BY doruc".format("', '".join(urblucds['doruc'].values)),
                             connect_spatial())
    acres_series = urblucds.merge(lucdsacres, on='doruc').groupby('classification')['sum'].sum()
    lucdsjv = pd.read_sql("SELECT doruc, SUM(jv) FROM vector.parcel2015_orange "
                          "WHERE doruc IN ('{}') GROUP BY doruc".format("', '".join(urblucds['doruc'].values)),
                          connect_spatial())
    jv_series = urblucds.merge(lucdsjv, on='doruc').groupby('classification')['sum'].sum()
    luwgt_series = pd.read_sql("SELECT sublevel_name, weight FROM wgt_lucis "
                               "WHERE land_use_type = 'urb'", connect_class(), index_col='sublevel_name')['weight']
    urbwgt_series = (jv_series / np.sum(jv_series) + acres_series / np.sum(acres_series) + luwgt_series) / 3
    lutypes = lucisUrbLuCode.keys()
    for lutype in lutypes:
        goalname = lutype + '_goal'
        goal_series = pd.Series(0, index=next(iter(urbout_dict.values())).index, name=goalname)
        objnames = pd.read_sql("SELECT DISTINCT level_name FROM wgt_lucis "
                               "WHERE land_use_type = '{}' AND level = 'objective'".format(lutype),
                               connect_class())['level_name'].values
        for objname in objnames:
            subobj_wdf = pd.read_sql("SELECT sublevel_name, weight FROM wgt_lucis "
                                     "WHERE land_use_type = '{}' AND level = 'objective' "
                                     "AND level_name = '{}'".format(lutype, objname),
                                     connect_class(), index_col='sublevel_name')
            objname = lutype + '_' + objname
            obj_series = pd.Series(0, index=next(iter(urbout_dict.values())).index, name=objname)
            for subobjname in subobj_wdf.index.values:
                obj_series = obj_series + urbout_dict[lutype + '_' + subobjname] * subobj_wdf.loc[subobjname, 'weight']
            urbout_dict[objname] = obj_series
        obj_wdf = pd.read_sql("SELECT sublevel_name, weight FROM wgt_lucis "
                              "WHERE land_use_type = '{}' AND level = 'goal'".format(lutype),
                              connect_class(), index_col='sublevel_name')
        for objname in obj_wdf.index.values:
            goal_series = goal_series + urbout_dict[lutype + '_' + objname] * obj_wdf.loc[objname, 'weight']
        minval = np.min(goal_series)
        maxval = np.max(goal_series)
        goal_series = (goal_series - minval) * 8 / (maxval - minval) + 1
        urbfinal_series = urbfinal_series + goal_series * urbwgt_series[lucisUrbLuCode[lutype]]
    # urbmin = np.min(urbfinal_series)
    # urbmax = np.max(urbfinal_series)
    # urbfinal_series = (urbfinal_series - urbmin) * 8 / (urbmax - urbmin) + 1
    return pd.Series(urbfinal_series, name='urb_final')


def ClipModelRescale(inputgdfsql, modelname):
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    clip_ddf = pd.read_sql("SELECT raster_name, no_data_value FROM def_clipmodel "
                           "WHERE model_name = '{}'".format(modelname), connect_class())
    clipmodel_data = PostgresRasterToArray(clip_ddf['raster_name'].values[0],
                                           rasterconn, clip_ddf['no_data_value'].values[0])
    modelname = modelname.replace(' ', '')
    input_gdf[modelname+'_v'] = ZonalStatsRasterArray(input_gdf, clipmodel_data[0], clipmodel_data[1], ['mean'])['mean']
    minvalue = np.min(input_gdf[modelname+'_v'])
    maxvalue = np.max(input_gdf[modelname+'_v'])
    LinearRescale(input_gdf, modelname + '_v', modelname + '_s', maxvalue, minvalue, 1, 9)
    return pd.Series(input_gdf[modelname+'_s'].values, index=input_gdf[parcel_uniqueid], name='con_' + modelname)


def ConversationConnectivity(inputgdfsql):
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    mgbank_gdf = gpd.GeoDataFrame.from_postgis(sql=mgbank_data, con=connect_spatial())
    input_gdf['mgbank'] = 1
    input_gdf.loc[input_gdf[parcel_uniqueid].isin(gpd.sjoin(input_gdf, mgbank_gdf)[parcel_uniqueid]), 'mgbank'] = 9
    greenway_ddf = pd.read_sql("SELECT raster_name, no_data_value FROM def_clipmodel WHERE model_name = 'greenway'",
                               connect_class())
    greenway_data = PostgresRasterToArray(greenway_ddf['raster_name'].values[0],
                                          rasterconn, greenway_ddf['no_data_value'].values[0])
    input_gdf['greenway'+'_v'] = ZonalStatsRasterArray(input_gdf, greenway_data[0], greenway_data[1], ['mean'])['mean']
    minvalue = np.min(input_gdf['greenway'+'_v'])
    maxvalue = np.max(input_gdf['greenway'+'_v'])
    LinearRescale(input_gdf, 'greenway'+'_v', 'greenway'+'_s', maxvalue, minvalue, 1, 9)
    return pd.Series(input_gdf[['greenway'+'_s', 'mgbank']].max(axis=1).values,
                     index=input_gdf[parcel_uniqueid], name='con_connectivity')


def HabitatReclassify(inputgdfsql, gfchabsuit):
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    cntbnd_gdf = gpd.GeoDataFrame.from_postgis(sql=cntbnd_data, con=connect_spatial())
    flma_gdf = SelectByLocation(gpd.GeoDataFrame.from_postgis(sql=flma_data, con=connect_spatial()), cntbnd_gdf)
    WithinDistance(250, input_gdf, parcel_uniqueid, flma_gdf, 'flma_suit')
    input_gdf.loc[input_gdf['flma_suit'] == 1, 'flma'] = 9
    input_gdf.loc[input_gdf['flma_suit'] == 0, 'flma'] = 1

    arr_data = PostgresRasterToArray(gfchabraster, rasterconn)
    gfchab_sdf = pd.read_sql(gfchabsuit, connect_class())
    input_gdf['gfchab_suit'] = ZonalStatsRasterArray(input_gdf,
                                                     ReclassifyRasterArray(arr_data[0], gfchab_sdf),
                                                     arr_data[1], ['mean'])['mean']
    return pd.Series(input_gdf[['gfchab_suit', 'flma_suit']].max(axis=1).values,
                     index=input_gdf[parcel_uniqueid], name='con_gfchabflma')


def ConservationFinal(paraoutput):
    conout_dict = dict((item.name, item) for item in paraoutput if item.name.split('_')[0] == 'con')
    conwaterquality_series = np.maximum(conout_dict['con_surfacewater'],
                                        conout_dict['con_subsurfacewater']).rename("con_waterquality")
    confldplnwtlnd_series = np.maximum(conout_dict['con_floodplain'],
                                       conout_dict['con_wetland']).rename("con_fldplnwtlnd")
    conecoprcss_series = np.maximum(confldplnwtlnd_series,
                                    conout_dict['con_gfchabflma']).rename("con_ecoprcss")
    conwgt_series = pd.read_sql("SELECT sublevel_name, weight FROM wgt_lucis "
                                "WHERE land_use_type = 'con'", connect_class(), index_col='sublevel_name')['weight']
    confinal_series = np.maximum(
        conout_dict['con_connectivity'],
        conout_dict['con_biodiversity'] * conwgt_series['biodiversity'] +
        conwaterquality_series * conwgt_series['waterquality'] +
        conecoprcss_series * conwgt_series['ecologicalprocess']
    )
    # conmin = np.min(confinal_series)
    # conmax = np.max(confinal_series)
    # confinal_series = (confinal_series - conmin) * 8 / (conmax - conmin) + 1
    return pd.Series(confinal_series, name='con_final')


def SoilProduction(inputgdfsql):
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    rcrp_gdf = gpd.GeoDataFrame.from_postgis(sql=rowcrops_data, con=connect_spatial())
    joint_geodf = gpd.sjoin(input_gdf, rcrp_gdf)
    selected_input = pd.DataFrame.copy(input_gdf.loc[input_gdf.index.to_series().isin(joint_geodf.index.values), :])
    selected_fsaid = pd.DataFrame.copy(rcrp_gdf.loc[rcrp_gdf.index.to_series().isin(joint_geodf.index_right.values), :])
    selected_fsaid['landcapability'] = 1
    selected_fsaid.loc[selected_fsaid['land_cap'] != 'Not Rated', 'landcapability'] = \
        selected_fsaid.loc[selected_fsaid['land_cap'] != 'Not Rated', 'land_cap'].apply(lambda val: 10-int(val[:1]))
    fsaid_arr = ShapeToArray(selected_fsaid, cell_size, 'landcapability', fillval=1)
    input_gdf['soilproduct'] = 1
    input_gdf.loc[input_gdf[parcel_uniqueid].isin(selected_input[parcel_uniqueid]), 'soilproduct'] = \
        ZonalStatsRasterArray(selected_input, fsaid_arr[0], fsaid_arr[1], ['mean'], 0)['mean']
    return pd.Series(input_gdf['soilproduct'].values, index=input_gdf[parcel_uniqueid], name='rcrp_soilprdctn')


def PrimeFarmland(inputgdfsql, nrcsprimefarmsuitsql, agparcelprimefarmsuitsql):
    inputgeomix = inputgdfsql.find('geom')
    inputdorgdfsql = inputgdfsql[:inputgeomix] + lucodefield + ', ' + inputgdfsql[inputgeomix:]
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputdorgdfsql, con=connect_spatial())
    primefarm_gdf = gpd.GeoDataFrame.from_postgis(sql=primefarm_data, con=connect_spatial())

    input_gdf['geom'] = input_gdf.centroid
    primefarm_sdf = pd.read_sql(nrcsprimefarmsuitsql, connect_class())
    ReclassifyByValue(primefarm_gdf, 'frmlndcl', 'frmlnd_suit',
                      dict(zip(primefarm_sdf.iloc[:, 0], primefarm_sdf.iloc[:, 1])), novalue=1)
    input_gdf['frmlnd_suit'] = Identity(input_gdf, primefarm_gdf, ['frmlnd_suit'])['frmlnd_suit']
    agparcel_sdf = pd.read_sql(agparcelprimefarmsuitsql, connect_class())
    ReclassifyByValue(input_gdf, 'doruc', 'agparcel_suit',
                      dict(zip(agparcel_sdf.iloc[:, 0], agparcel_sdf.iloc[:, 1])), novalue=1)
    return pd.Series(input_gdf[['frmlnd_suit', 'agparcel_suit']].max(axis=1).values,
                     index=input_gdf[parcel_uniqueid], name='rcrp_primefrm')


def RowCropsMarket(inputgdfsql):
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    city_gdf = gpd.GeoDataFrame.from_postgis(sql=citypop_data, con=connect_spatial())
    citypop_ddf = pd.read_sql("SELECT city_size, pop_lower, pop_upper FROM def_citybypopsize", connect_class())
    large_lower, large_upper = citypop_ddf.loc[citypop_ddf['city_size'] == 'large',
                                               ['pop_lower', 'pop_upper']].values[0]
    medium_lower, medium_upper = citypop_ddf.loc[citypop_ddf['city_size'] == 'medium',
                                                 ['pop_lower', 'pop_upper']].values[0]
    small_lower, small_upper = citypop_ddf.loc[citypop_ddf['city_size'] == 'small',
                                               ['pop_lower', 'pop_upper']].values[0]
    mini_lower, mini_upper = citypop_ddf.loc[citypop_ddf['city_size'] == 'mini',
                                             ['pop_lower', 'pop_upper']].values[0]
    large_cities = city_gdf.loc[(city_gdf['pop2010'] >= large_lower) & (city_gdf['pop2010'] < large_upper), :]
    medium_cities = city_gdf.loc[(city_gdf['pop2010'] >= medium_lower) & (city_gdf['pop2010'] < medium_upper), :]
    small_cities = city_gdf.loc[(city_gdf['pop2010'] >= small_lower) & (city_gdf['pop2010'] < small_upper), :]
    mini_cities = city_gdf.loc[(city_gdf['pop2010'] >= mini_lower) & (city_gdf['pop2010'] < mini_upper), :]
    input_gdf['large_dist'] = ToPointDistance(input_gdf, large_cities, 'euclidean')
    input_gdf['medium_dist'] = ToPointDistance(input_gdf, medium_cities, 'euclidean')
    input_gdf['small_dist'] = ToPointDistance(input_gdf, small_cities, 'euclidean')
    input_gdf['mini_dist'] = ToPointDistance(input_gdf, mini_cities, 'euclidean')
    city_wdf = pd.read_sql("SELECT class, weight FROM wgt_agmodel "
                           "WHERE land_use_type = 'row crops' AND model = 'row crops market'", connect_class(),
                           index_col='class')
    WeightedSum(input_gdf, ['large_dist', 'medium_dist', 'small_dist', 'mini_dist'], city_wdf, 'marketdist')
    LinearRescale(input_gdf, 'marketdist', 'marketsuit', input_gdf['marketdist'].values.max(),
                  input_gdf['marketdist'].values.min(), 1, 9)
    rowcrops_gdf = gpd.GeoDataFrame.from_postgis(sql=rowcrops_data, con=connect_spatial())
    selected_input = SelectByLocation(input_gdf, rowcrops_gdf)
    input_gdf.loc[input_gdf[parcel_uniqueid].isin(selected_input[parcel_uniqueid]), 'marketsuit'] = 9
    return pd.Series(input_gdf['marketsuit'].values, index=input_gdf[parcel_uniqueid], name='rcrp_market')


def ProductTransport(inputgdfsql):
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    cntbnd_gdf = gpd.GeoDataFrame.from_postgis(sql=cntbnd_data, con=connect_spatial())
    rcrp_gdf = gpd.GeoDataFrame.from_postgis(sql=rowcrops_data, con=connect_spatial())
    majrds_gdf = SelectByLocation(gpd.GeoDataFrame.from_postgis(sql=majrds_wointerstate_data, con=connect_spatial()),
                                  cntbnd_gdf)
    majhwys_gdf = SelectByLocation(gpd.GeoDataFrame.from_postgis(sql=majhwys_onlyinterstate_data, con=connect_spatial()),
                                   cntbnd_gdf)
    input_gdf['majrdsdist'] = ToLineDistance(input_gdf, majrds_gdf, cell_size, 'euclidean')
    input_gdf['majhwysdist'] = ToLineDistance(input_gdf, majhwys_gdf, cell_size, 'euclidean')
    rcrpmajrdsarr = ToLineDistance(rcrp_gdf, majrds_gdf, cell_size, 'euclidean')
    rcrpmajhwysarr = ToLineDistance(rcrp_gdf, majhwys_gdf, cell_size, 'euclidean')
    LinearRescale(input_gdf, 'majrdsdist', 'majrdsdist_suit', np.max(rcrpmajrdsarr), np.mean(rcrpmajrdsarr), 1, 9)
    LinearRescale(input_gdf, 'majhwysdist', 'majhwysdist_suit', np.max(rcrpmajhwysarr), np.mean(rcrpmajhwysarr), 1, 9)
    rdshwys_wdf = pd.read_sql("SELECT class, weight FROM wgt_agmodel "
                              "WHERE land_use_type = 'row crops' AND model = 'product transport'",
                              connect_class(), index_col='class')
    WeightedSum(input_gdf, ['majrdsdist_suit', 'majhwysdist_suit'], rdshwys_wdf, 'rcrptransport')
    return pd.Series(input_gdf['rcrptransport'].values, index=input_gdf[parcel_uniqueid], name='rcrp_transport')


def CropLandValue(inputgdfsql):
    inputgeomix = inputgdfsql.find('geom')
    inputlvgdfsql = inputgdfsql[:inputgeomix] + landvaluefield + ', ' + inputgdfsql[inputgeomix:]
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputlvgdfsql, con=connect_spatial())
    input_gdf['dolperacre'] = input_gdf['jv'] / (input_gdf.area / sqmtoacre)
    rcrpparcel_gdf = gpd.GeoDataFrame.from_postgis(sql=cropland_parcel_data, con=connect_spatial())
    rcrpparcel_gdf['dolperacre'] = rcrpparcel_gdf['jv'] / (rcrpparcel_gdf.area / sqmtoacre)
    rcrpparcel_grouped = rcrpparcel_gdf.groupby('doruc')
    rcrpmean = rcrpparcel_grouped['dolperacre'].agg(np.mean)
    rcrpmax = pd.Series(rcrpparcel_grouped['dolperacre'].agg(np.max), index=rcrpmean.index)
    rcrp_wdf = pd.read_sql("SELECT class, weight FROM wgt_agmodel "
                           "WHERE land_use_type = 'row crops' AND model = 'land value'",
                           connect_class(), index_col='class')
    input_gdf['crplandvalue'] = 0
    for idx in rcrpmean.index.values:
        input_gdf['crplandvalue'] = input_gdf['crplandvalue'] + \
                                    rcrp_wdf['weight'][idx] * LinearRescale(input_gdf, 'dolperacre',
                                                                            'dolperacre_' + idx, rcrpmax[idx],
                                                                            rcrpmean[idx], 1, 9)['dolperacre_' + idx]
    if rcrpparcel_grouped.ngroups < len(rcrp_wdf):
        return pd.Series(LinearRescale(input_gdf, 'crplandvalue', 'croplandvalue',
                                       input_gdf['crplandvalue'].values.min(),
                                       input_gdf['crplandvalue'].values.max(), 1, 9)['croplandvalue'].values,
                         index=input_gdf[parcel_uniqueid], name='rcrp_landvalue')
    else:
        return pd.Series(input_gdf['crplandvalue'].values, index=input_gdf[parcel_uniqueid], name='rcrp_landvalue')


def AgricultureLandUse(inputgeodfsql, agtype):
    inputgeomix = inputgeodfsql.find('geom')
    inputdorgdfsql = inputgeodfsql[:inputgeomix] + lucodefield + ', ' + inputgeodfsql[inputgeomix:]
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputdorgdfsql, con=connect_spatial())
    aglanduse_sdf = pd.read_sql(
        "SELECT class, suit_score FROM suit_agmodel_bycase "
        "WHERE land_use_type = '{}' AND model = 'land use'".format(agtype),
        connect_class())
    agludict = {'livestock high intensity': 'lshi',
                'livestock low intensity': 'lsli',
                'specialty farm': 'sfrm',
                'nursery': 'nsry',
                'timber': 'tmbr'}
    outname = agludict[agtype] + '_lnduse'
    ReclassifyByValue(input_gdf, 'doruc', outname,
                      dict(zip(aglanduse_sdf.iloc[:, 0], aglanduse_sdf.iloc[:, 1])), novalue=1)
    return pd.Series(input_gdf[outname].values, index=input_gdf[parcel_uniqueid], name=outname)


def LivestockOpenWater(inputgeodfsql, hililstk):
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgeodfsql, con=connect_spatial())
    cntbnd_gdf = gpd.GeoDataFrame.from_postgis(sql=cntbnd_data, con=connect_spatial())
    flowline_gdf = SelectByLocation(
        gpd.GeoDataFrame.from_postgis(sql=nhdflowline_data, con=connect_spatial()), cntbnd_gdf)
    waterbody_gdf = SelectByLocation(
        gpd.GeoDataFrame.from_postgis(sql=nhdwaterbody_data, con=connect_spatial()), cntbnd_gdf)
    input_gdf['flowlinedist'] = ToLineDistance(input_gdf, flowline_gdf, cell_size, 'euclidean')
    input_gdf['waterbodydist'] = ToPolygonDistance(input_gdf, waterbody_gdf, cell_size, 'euclidean')
    flowline_suit = "SELECT lower, upper, suit_score FROM suit_agmodel_byinterval " \
                    "WHERE land_use_type = 'livestock' AND model = 'open water water body'"
    waterbody_suit = "SELECT lower, upper, suit_score FROM suit_agmodel_byinterval " \
                     "WHERE land_use_type = 'livestock' AND model = 'open water flow line'"
    input_gdf['flowline_suit'] = RescaleByInterval(
        input_gdf, pd.read_sql(flowline_suit, con=connect_class()), 'flowlinedist')
    input_gdf['waterbody_suit'] = RescaleByInterval(
        input_gdf, pd.read_sql(waterbody_suit, con=connect_class()), 'waterbodydist')
    outname = 'ls{}i'.format(hililstk[:1]) + '_openwtr'
    return pd.Series(input_gdf[['flowline_suit', 'waterbody_suit']].max(axis=1).values,
                     index=input_gdf[parcel_uniqueid], name=outname)


def AgricultureAquiferRecharge(inputgeodfsql, agtype):
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgeodfsql, con=connect_spatial())
    rechrgnodata = pd.read_sql(
        "SELECT no_data_value FROM def_clipmodel WHERE raster_name = '{}'".format(rechargeraster),
        connect_class()).iloc[0, 0]
    arr_data = PostgresRasterToArray(rechargeraster, rasterconn, rechrgnodata)
    input_gdf['rechrg'] = ZonalStatsRasterArray(input_gdf, arr_data[0], arr_data[1], ['mean'])['mean']
    minvalue = np.min(input_gdf['rechrg'])
    maxvalue = np.max(input_gdf['rechrg'])
    LinearRescale(input_gdf, 'rechrg', 'rechrg_suit', minvalue, maxvalue, 1, 9)
    agludict = {'livestock high intensity': 'lshi',
                'livestock low intensity': 'lsli',
                'specialty farm': 'sfrm'}
    outname = agludict[agtype] + '_rechrg'
    return pd.Series(input_gdf['rechrg_suit'].values, index=input_gdf[parcel_uniqueid], name=outname)


def AgricultureSoil(inputgdfsql, agtype):
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    input_gdf['geom'] = input_gdf.centroid
    soil_gdf = gpd.GeoDataFrame.from_postgis(sql=soil_data, con=connect_spatial())
    soildrain_sdf = pd.read_sql(
        "SELECT class, suit_score from suit_agmodel_bycase "
        "WHERE land_use_type = '{}' AND model = 'soil drainage'".format(agtype), connect_class())
    ReclassifyByValue(soil_gdf, 'drainagecl', 'drain_suit',
                      dict(zip(soildrain_sdf.iloc[:, 0], soildrain_sdf.iloc[:, 1])), novalue=1)
    agludict = {'livestock high intensity': 'lshi',
                'livestock low intensity': 'lsli',
                'specialty farm': 'sfrm',
                'timber': 'tmbr'}
    outname = agludict[agtype] + '_soil'
    return pd.Series(Identity(input_gdf, soil_gdf, ['drain_suit'])['drain_suit'].values,
                     index=input_gdf[parcel_uniqueid], name=outname).fillna(1)


def LivestockExtUrban(inputgeodfsql):
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgeodfsql, con=connect_spatial())
    exturb_gdf = gpd.GeoDataFrame.from_postgis(sql=exturb_parcel_data, con=connect_spatial())
    exturb_gdf['geom'] = exturb_gdf.centroid
    lshiparcel_gdf = gpd.GeoDataFrame.from_postgis(sql=intensag_parcel_data, con=connect_spatial())
    lshiparcel_gdf['exturbdist'] = ToPointDistance(lshiparcel_gdf, exturb_gdf, 'euclidean')
    input_gdf['exturbdist'] = ToPointDistance(input_gdf, exturb_gdf, 'euclidean')
    dist_mean = np.mean(lshiparcel_gdf['exturbdist'])
    dist_max = np.max(lshiparcel_gdf['exturbdist'])
    LinearRescale(input_gdf, 'exturbdist', 'exturb_suit', dist_max, dist_mean, 1, 9)
    return pd.Series(input_gdf['exturb_suit'].values, index=input_gdf[parcel_uniqueid], name='lshi_exturb')


def AgricultureMarket(inputgeodfsql, agtype, markettype):
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgeodfsql, con=connect_spatial())
    city_gdf = gpd.GeoDataFrame.from_postgis(sql=citypop_data, con=connect_spatial())
    citypopdf = pd.read_sql("SELECT city_size, pop_lower, pop_upper FROM def_citybypopsize", connect_class())
    if markettype == 'local':
        lvl1, lvl2, lvl1dist, lvl2dist, mktname = ('small', 'mini', 'smalldist', 'minidist', 'local market')
    else:
        lvl1, lvl2, lvl1dist, lvl2dist, mktname = ('large', 'medium', 'largedist', 'mediumdist', 'regional market')
    lvl1_lower, lvl1_upper = citypopdf.loc[citypopdf['city_size'] == lvl1, ['pop_lower', 'pop_upper']].values[0]
    lvl1city_gdf = city_gdf.loc[(city_gdf['pop2010'] >= lvl1_lower) & (city_gdf['pop2010'] < lvl1_upper), :]
    lvl2_lower, lvl2_upper = citypopdf.loc[citypopdf['city_size'] == lvl2, ['pop_lower', 'pop_upper']].values[0]
    lvl2city_gdf = city_gdf.loc[(city_gdf['pop2010'] >= lvl2_lower) & (city_gdf['pop2010'] < lvl2_upper), :]
    agludict = {'livestock high intensity': ['lshi', lshi_parcel_data],
                'livestock low intensity': ['lsli', lsli_parcel_data],
                'specialty farm': ['sfrm', sfrm_parcel_data],
                'nursery': ['nsry', nsry_parcel_data],
                'timber': ['tmbr', tmbr_parcel_data]}
    agparcel_gdf = gpd.GeoDataFrame.from_postgis(sql=agludict[agtype][1], con=connect_spatial())
    agparcel_gdf[lvl1dist] = ToPointDistance(agparcel_gdf, lvl1city_gdf, 'euclidean')
    agparcel_gdf[lvl2dist] = ToPointDistance(agparcel_gdf, lvl2city_gdf, 'euclidean')
    lvl1_max = np.max(agparcel_gdf[lvl1dist])
    lvl1_mean = np.mean(agparcel_gdf[lvl1dist])
    lvl2_max = np.max(agparcel_gdf[lvl2dist])
    lvl2_mean = np.mean(agparcel_gdf[lvl2dist])
    input_gdf[lvl1dist] = ToPointDistance(input_gdf, lvl1city_gdf, 'euclidean')
    input_gdf[lvl2dist] = ToPointDistance(input_gdf, lvl2city_gdf, 'euclidean')
    LinearRescale(input_gdf, lvl1dist, lvl1dist + '_suit', lvl1_max, lvl1_mean, 1, 9)
    LinearRescale(input_gdf, lvl2dist, lvl2dist + '_suit', lvl2_max, lvl2_mean, 1, 9)
    lvl1lvl2_wdf = pd.read_sql(
        "SELECT class, weight FROM wgt_agmodel WHERE land_use_type = '{}' AND model = '{}'".format(agtype, mktname),
        connect_class(), index_col='class')
    outname = agludict[agtype][0] + '_' + markettype + 'mkt'
    WeightedSum(input_gdf, [lvl1dist+'_suit', lvl2dist+'_suit'], lvl1lvl2_wdf, outname)
    return pd.Series(input_gdf[outname].values, index=input_gdf[parcel_uniqueid], name=outname)


def AgricultureMajorRoads(inputgeodfsql, agtype):
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgeodfsql, con=connect_spatial())
    cntbnd_gdf = gpd.GeoDataFrame.from_postgis(sql=cntbnd_data, con=connect_spatial())
    majrds_gdf = SelectByLocation(
        gpd.GeoDataFrame.from_postgis(sql=majrds_data, con=connect_spatial()), cntbnd_gdf)
    majrds_arr = ShapeToArray(majrds_gdf, cell_size)
    input_gdf['majrdsdist'] = ToLineDistance(input_gdf, majrds_gdf, cell_size, 'manhattan', majrds_arr)
    agludict = {'livestock high intensity': ['lshi', lshi_parcel_data],
                'livestock low intensity': ['lsli', lsli_parcel_data],
                'specialty farm': ['sfrm', sfrm_parcel_data],
                'nursery': ['nsry', nsry_parcel_data],
                'timber': ['tmbr', tmbr_parcel_data]}
    agparcel_gdf = gpd.GeoDataFrame.from_postgis(sql=agludict[agtype][1], con=connect_spatial())
    agparcel_gdf['majrdsdist'] = ToLineDistance(agparcel_gdf, majrds_gdf, cell_size, 'manhattan', majrds_arr)
    distmax = np.max(agparcel_gdf['majrdsdist'])
    distmean = np.mean(agparcel_gdf['majrdsdist'])
    LinearRescale(input_gdf, 'majrdsdist', 'majrdsdist_suit', distmax, distmean, 1, 9)
    outname = agludict[agtype][0] + '_majrds'
    return pd.Series(input_gdf['majrdsdist_suit'].values, index=input_gdf[parcel_uniqueid], name=outname)


def AgricultureLandValue(inputgdfsql, agtype):
    inputgeomix = inputgdfsql.find('geom')
    inputlvgdfsql = inputgdfsql[:inputgeomix] + landvaluefield + ', ' + inputgdfsql[inputgeomix:]
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputlvgdfsql, con=connect_spatial())
    input_gdf['dolperacre'] = input_gdf[landvaluefield] / (input_gdf.area / sqmtoacre)
    agludict = {'livestock high intensity': ['lshi', lshi_parcel_data],
                'livestock low intensity': ['lsli', lsli_parcel_data],
                'specialty farm': ['sfrm', sfrm_parcel_data],
                'nursery': ['nsry', nsry_parcel_data],
                'timber': ['tmbr', tmbr_parcel_data]}
    aggeomix = agludict[agtype][1].find('geom')
    aglvgdfsql = agludict[agtype][1][:aggeomix] + landvaluefield + ', ' + agludict[agtype][1][aggeomix:]
    agparcel_gdf = gpd.GeoDataFrame.from_postgis(sql=aglvgdfsql, con=connect_spatial())
    agparcel_gdf['dolperacre'] = agparcel_gdf[landvaluefield] / (agparcel_gdf.area / sqmtoacre)
    jvmax = np.max(agparcel_gdf['dolperacre'])
    jvmean = np.mean(agparcel_gdf['dolperacre'])
    LinearRescale(input_gdf, 'dolperacre', 'dolperacre_suit', jvmax, jvmean, 1, 9)
    outname = agludict[agtype][0] + '_landvalue'
    return pd.Series(input_gdf['dolperacre_suit'].values, index=input_gdf[parcel_uniqueid], name=outname)


def SpecialtyOpenWater(inputgdfsql):
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    cntbnd_gdf = gpd.GeoDataFrame.from_postgis(sql=cntbnd_data, con=connect_spatial())
    flowline_gdf = SelectByLocation(
        gpd.GeoDataFrame.from_postgis(sql=nhdflowline_data, con=connect_spatial()), cntbnd_gdf)
    waterbodyix = nhdwaterbody_data.find('DESCRIPT')
    waterbodysql = nhdwaterbody_data[:waterbodyix] + \
        'areasqkm >= {} AND '.format(10 * (sqmtoacre / 1000000)) + \
        nhdwaterbody_data[waterbodyix:]
    waterbody_gdf = SelectByLocation(
        gpd.GeoDataFrame.from_postgis(sql=waterbodysql, con=connect_spatial()), cntbnd_gdf)
    input_gdf['flowlinedist'] = ToLineDistance(input_gdf, flowline_gdf, cell_size, 'euclidean')
    input_gdf['waterbodydist'] = ToPolygonDistance(input_gdf, waterbody_gdf, cell_size, 'euclidean')
    input_gdf['gfchabowdist'] = ToRasterDistance(input_gdf, gfchabraster, rasterconn, gfchab_ow_value, 'euclidean')
    input_gdf['owdist'] = input_gdf[['flowlinedist', 'waterbodydist', 'gfchabowdist']].min(axis=1)
    owsuitsql = "SELECT lower, upper, suit_score FROM suit_agmodel_byinterval " \
                "WHERE model = 'open water' AND land_use_type = 'specialty farm'"
    owdist_sdf = pd.read_sql(owsuitsql, connect_class())
    return pd.Series(RescaleByInterval(input_gdf, owdist_sdf, 'owdist').values,
                     index=input_gdf[parcel_uniqueid], name='sfrm_openwtr')


def SpecialtyProcessingPlant(inputgdfsql):
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputgdfsql, con=connect_spatial())
    fdprcss_gdf = gpd.GeoDataFrame.from_postgis(sql=foodprocess_parcel_data, con=connect_spatial())
    orchardsql = "SELECT geom FROM vector.parcel2015_orange WHERE doruc ='066'"
    orchard_gdf = gpd.GeoDataFrame.from_postgis(sql=orchardsql, con=connect_spatial())
    orchard_gdf['fdprcssdist'] = ToPointDistance(orchard_gdf, fdprcss_gdf, 'euclidean')
    distmax = np.max(orchard_gdf['fdprcssdist'])
    distmean = np.mean(orchard_gdf['fdprcssdist'])
    input_gdf['fdprcssdist'] = ToPointDistance(input_gdf, fdprcss_gdf, 'euclidean')
    LinearRescale(input_gdf, 'fdprcssdist', 'fdprcssdist_suit', distmax, distmean, 1, 9)
    return pd.Series(input_gdf['fdprcssdist_suit'].values, index=input_gdf[parcel_uniqueid], name='sfrm_fdprcss')


def AgricultureParcelSize(inputgdfsql, agtype):
    inputgeomix = inputgdfsql.find('geom')
    inputlugdfsql = inputgdfsql[:inputgeomix] + lucodefield + ', ' + inputgdfsql[inputgeomix:]
    input_gdf = gpd.GeoDataFrame.from_postgis(sql=inputlugdfsql, con=connect_spatial())
    agludict = {'nursery': 'nsry',
                'timber': 'tmbr'}
    outname = agludict[agtype] + '_parsize'
    input_gdf['areaacre'] = input_gdf.area / sqmtoacre
    agparsize_sdf = pd.read_sql("SELECT lower, upper, suit_score FROM suit_agmodel_byinterval "
                                "WHERE land_use_type = '{}' AND model = 'parcel size'".format(agtype),
                                connect_class())
    input_gdf['parsize_suit'] = RescaleByInterval(input_gdf, agparsize_sdf, 'areaacre', fillnaval=1)
    niag_ddf = pd.read_sql("SELECT doruc FROM def_parcel WHERE classification = 'non-incentive agriculture'",
                           connect_class())
    input_gdf.loc[~input_gdf['doruc'].isin(niag_ddf['doruc'].values), 'parsize_suit'] = 1
    if agtype == 'nursery':
        return pd.Series(input_gdf['parsize_suit'].values, index=input_gdf[parcel_uniqueid], name=outname)
    else:
        tmbr_ddf = pd.read_sql("SELECT doruc FROM def_parcel WHERE classification = 'timber'", connect_class())
        input_gdf.loc[input_gdf['doruc'].isin(tmbr_ddf['doruc'].values), 'parsize_suit'] = 9
        return pd.Series(input_gdf['parsize_suit'].values, index=input_gdf[parcel_uniqueid], name=outname)


def AgricultureGoal(agtype, agseriesdict):
    physical_wgt = pd.read_sql(
        "SELECT sublevel_name, weight FROM wgt_lucis "
        "WHERE land_use_type = '{}' AND level_name = 'physical'".format(agtype),
        connect_class(), index_col='sublevel_name'
    )['weight']
    physical_series = pd.Series(0, index=next(iter(agseriesdict.values())).index)
    for item in physical_wgt.index.values:
        physical_series = agseriesdict[agtype + '_' + item] * physical_wgt[item] + physical_series
    if agtype in ('lshi', 'lsli', 'sfrm'):
        physical_series = np.maximum(agseriesdict[agtype + '_lnduse'], physical_series)
    else:
        pass
    proximity_wgt = pd.read_sql(
        "SELECT sublevel_name, weight FROM wgt_lucis "
        "WHERE land_use_type = '{}' AND level_name = 'proximity'".format(agtype),
        connect_class(), index_col='sublevel_name'
    )['weight']
    proximity_series = pd.Series(0, index=next(iter(agseriesdict.values())).index)
    for item in proximity_wgt.index.values:
        proximity_series = agseriesdict[agtype + '_' + item] * proximity_wgt[item] + proximity_series
    goal_wgt = pd.read_sql(
        "SELECT sublevel_name, weight FROM wgt_lucis "
        "WHERE land_use_type = '{}' AND level = 'goal'".format(agtype),
        connect_class(), index_col='sublevel_name'
    )['weight']
    goal_series = (physical_series * goal_wgt['physical'] +
                   proximity_series * goal_wgt['proximity'] +
                   agseriesdict[agtype + '_landvalue'] * goal_wgt['landvalue'])
    return goal_series.rename(agtype + '_goal')


def AgricultureFinal(paraoutput):
    agout_dict = {item.name: item for item in paraoutput if item.name.split('_')[0] in lucisAgLuCode.keys()}
    rcrpphysical_series = agout_dict['rcrp_primefrm'].where(agout_dict['rcrp_primefrm'] == 9,
                                                            agout_dict['rcrp_soilprdctn'])
    rcrpproximity_wgt = pd.read_sql(
        "SELECT sublevel_name, weight FROM wgt_lucis "
        "WHERE land_use_type = 'rcrp' AND level_name = 'proximity'",
        connect_class(), index_col='sublevel_name'
    )['weight']
    rcrpproximity_series = (agout_dict['rcrp_market'] * rcrpproximity_wgt['market'] +
                            agout_dict['rcrp_transport'] * rcrpproximity_wgt['transport'])
    rcrpgoal_wgt = pd.read_sql(
        "SELECT sublevel_name, weight FROM wgt_lucis "
        "WHERE land_use_type = 'rcrp' AND level = 'goal'",
        connect_class(), index_col='sublevel_name'
    )['weight']
    rcrpgoal_series = (rcrpphysical_series * rcrpgoal_wgt['physical'] +
                       rcrpproximity_series * rcrpgoal_wgt['proximity'] +
                       agout_dict['rcrp_landvalue'] * rcrpgoal_wgt['landvalue'])
    lshiseries_dict = {item: agout_dict[item] for item in agout_dict.keys() if item.split('_')[0] == 'lshi'}
    lshigoal_series = AgricultureGoal('lshi', lshiseries_dict)
    lsliseries_dict = {item: agout_dict[item] for item in agout_dict.keys() if item.split('_')[0] == 'lsli'}
    lsligoal_series = AgricultureGoal('lsli', lsliseries_dict)
    lsgoal_wgt = pd.read_sql(
        "SELECT sublevel_name, weight FROM wgt_lucis "
        "WHERE land_use_type = 'ls' AND level = 'goal'",
        connect_class(), index_col='sublevel_name'
    )['weight']
    lsgoal_series = lshigoal_series * lsgoal_wgt['lshi'] + lsligoal_series * lsgoal_wgt['lsli']
    lsdoruc = pd.read_sql(
        "SELECT doruc FROM def_parcel "
        "WHERE classification IN ('livestock high intensity', 'livestock low intensity')",
        connect_class()
    )['doruc'].values
    exsturbdoruc = pd.read_sql(
        "SELECT doruc FROM def_parcel "
        "WHERE classification = 'existing urban'",
        connect_class()
    )['doruc'].values
    doruc_df = pd.read_sql("SELECT {}, {} FROM vector.parcel2015_orange".format(parcel_uniqueid, lucodefield),
                           connect_spatial())
    lsgoal_df = lsgoal_series.to_frame('ls_goal').merge(doruc_df, on=parcel_uniqueid)
    lsgoal_df.loc[lsgoal_df['doruc'].isin(lsdoruc), 'ls_goal'] = 9
    lsgoal_df.loc[lsgoal_df['doruc'].isin(exsturbdoruc), 'ls_goal'] = 1
    lsgoal_series = pd.Series(lsgoal_df['ls_goal'].values, index=lsgoal_df[parcel_uniqueid])
    sfrmseries_dict = {item: agout_dict[item] for item in agout_dict.keys() if item.split('_')[0] == 'sfrm'}
    sfrmgoal_series = AgricultureGoal('sfrm', sfrmseries_dict)
    sfrmgoal_df = sfrmgoal_series.to_frame('sfrm_goal').merge(doruc_df, on=parcel_uniqueid)
    sfrmgoal_df.loc[sfrmgoal_df['doruc'].isin(exsturbdoruc), 'sfrm_goal'] = 1
    sfrmgoal_series = pd.Series(sfrmgoal_df['sfrm_goal'].values, index=sfrmgoal_df[parcel_uniqueid])
    nsryseries_dict = {item: agout_dict[item] for item in agout_dict.keys() if item.split('_')[0] == 'nsry'}
    nsrygoal_series = AgricultureGoal('nsry', nsryseries_dict)
    nsrygoal_df = nsrygoal_series.to_frame('nsry_goal').merge(doruc_df, on=parcel_uniqueid)
    nsrygoal_df.loc[nsrygoal_df['doruc'].isin(exsturbdoruc), 'nsry_goal'] = 1
    nsrygoal_series = pd.Series(nsrygoal_df['nsry_goal'].values, index=nsrygoal_df[parcel_uniqueid])
    tmbrseries_dict = {item: agout_dict[item] for item in agout_dict.keys() if item.split('_')[0] == 'tmbr'}
    tmbrgoal_series = AgricultureGoal('tmbr', tmbrseries_dict)
    tmbrgoal_df = tmbrgoal_series.to_frame('tmbr_goal').merge(doruc_df, on=parcel_uniqueid)
    tmbrgoal_df.loc[tmbrgoal_df['doruc'].isin(exsturbdoruc), 'tmbr_goal'] = 1
    tmbrgoal_series = pd.Series(tmbrgoal_df['tmbr_goal'].values, index=tmbrgoal_df[parcel_uniqueid])
    agfinal_wgt = pd.read_sql(
        "SELECT sublevel_name, weight FROM wgt_lucis "
        "WHERE land_use_type = 'ag' AND level = 'final'",
        connect_class(), index_col='sublevel_name'
    )['weight']
    agfinal_series = (rcrpgoal_series * agfinal_wgt['rcrp'] +
                      lsgoal_series * agfinal_wgt['ls'] +
                      sfrmgoal_series * agfinal_wgt['sfrm'] +
                      nsrygoal_series * agfinal_wgt['nsry'] +
                      tmbrgoal_series * agfinal_wgt['tmbr'])
    return agfinal_series.rename('ag_final')
