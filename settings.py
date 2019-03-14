import pandas as pd
from psycopg2 import connect

studycounty = 'ORANGE'
countyfips = '095'

data_crs = {'ellps': 'GRS80', 'lat_0': 24, 'lat_1': 24, 'lat_2': 31.5, 'lon_0': -84, 'no_defs': True,
            'proj': 'aea', 'towgs84': '0,0,0,0,0,0,0', 'units': 'm', 'x_0': 400000, 'y_0': 0}

db_host = 'localhost'
db_user = 'postgres'
db_password = 'Ccj6261302'
db_name_spatial = 'lucis_spatialdata'
db_name_class = 'lucis_classification'

gfchabraster = 'gfchab_orange'
rechargeraster = 'recharge_orange'
cell_size = 30
gfchab_ow_value = 27
sqmtoacre = 4046.86
sqkmtoacre = 0.00404686
yearbltfield = 'actyrblt'
landvaluefield = 'jv'
lucodefield = 'doruc'
res_growthrange = {'g8090': [1980, 1990, 0.8], 'g9000': [1990, 2000, 0.15], 'g0010': [2000, 2010, 0.05]}
lucisAgLuCode = {'rcrp': 'row crops', 'lshi': 'livestock high intensity', 'lsli': 'livestock low intensity',
                 'sfrm': 'specialty farm', 'nsry': 'nursery', 'tmbr': 'timber'}
lucisConLuCode = {'con': 'conservation'}
lucisUrbLuCode = {'mf': 'multi-family', 'sf': 'single-family', 'com': 'commercial', 'ret': 'retail',
                  'ind': 'industrial', 'ser': 'service', 'ins': 'institutional', 'ent': 'entertainment'}
lucisAllLuCode = {**lucisAgLuCode, **lucisConLuCode, **lucisUrbLuCode}

noise_barrier_def = "SELECT type, distance FROM def_noisebarrier"

rasterconn = "PG:host='{}' port='5432' " \
             "dbname='{}' user='{}' password='{}' " \
             "schema='raster' mode='2'".format(db_host, db_name_spatial, db_user, db_password)


def connect_spatial():
    return connect(database=db_name_spatial, user=db_user, password=db_password)


def connect_class():
    return connect(database=db_name_class, user=db_user, password=db_password)


def ParcelDefSQL(parceltable, parcelclass, fieldnames=None):
    dorucseries = pd.read_sql("SELECT doruc FROM def_parcel WHERE classification = '{}'".format(parcelclass),
                              con=connect_class())
    if fieldnames is None:
        return "SELECT geom FROM {} WHERE doruc IN {}".format(parceltable, "('" + "', '".join(dorucseries.doruc.values) + "')")
    else:
        if isinstance(fieldnames, list):
            return "SELECT {}, geom FROM {} WHERE doruc IN {}".format(', '.join(fieldnames), parceltable, "('" + "', '".join(dorucseries.doruc.values) + "')")
        else:
            return "SELECT {}, geom FROM {} WHERE doruc IN {}".format(fieldnames, parceltable, "('" + "', '".join(dorucseries.doruc.values) + "')")


parcel = 'vector.parcel2015_orange'     # county level data
parcel_uniqueid = 'gcid'
vr_parcel_data = ParcelDefSQL(parcel, 'vacant residential', parcel_uniqueid)
sf_parcel_data = ParcelDefSQL(parcel, 'single-family', parcel_uniqueid)
mf_parcel_data = ParcelDefSQL(parcel, 'multi-family', parcel_uniqueid)
ind_parcel_data = ParcelDefSQL(parcel, 'industrial', parcel_uniqueid)
ret_parcel_data = ParcelDefSQL(parcel, 'retail', parcel_uniqueid)
ent_parcel_data = ParcelDefSQL(parcel, 'entertainment', parcel_uniqueid)
com_parcel_data = ParcelDefSQL(parcel, 'commercial', parcel_uniqueid)
ins_parcel_data = ParcelDefSQL(parcel, 'institutional', parcel_uniqueid)
ser_parcel_data = ParcelDefSQL(parcel, 'service', parcel_uniqueid)
res_parcel_data = ParcelDefSQL(parcel, 'residential')
racetrack_parcel_data = ParcelDefSQL(parcel, 'race track')
intensag_parcel_data = ParcelDefSQL(parcel, 'intensive agriculture', parcel_uniqueid)
intensindutil_parcel_data = ParcelDefSQL(parcel, 'intensive industry and utility')
cropland_parcel_data = ParcelDefSQL(parcel, 'cropland', [lucodefield, landvaluefield])
exturb_parcel_data = ParcelDefSQL(parcel, 'existing urban')
lshi_parcel_data = ParcelDefSQL(parcel, 'livestock high intensity')
lsli_parcel_data = ParcelDefSQL(parcel, 'livestock low intensity')
sfrm_parcel_data = ParcelDefSQL(parcel, 'specialty farm')
nsry_parcel_data = ParcelDefSQL(parcel, 'nursery')
tmbr_parcel_data = ParcelDefSQL(parcel, 'timber')
foodprocess_parcel_data = ParcelDefSQL(parcel, 'food processing')

airport_noise_data = "SELECT * FROM vector.airports_2015 WHERE COUNTY = '{}' AND (ISINTERNAT = 'Y' OR HASMILLAND = 'Y' OR HASTOWER = 'Y')".format(studycounty)
majrds_wointerstate_data = "SELECT geom FROM vector.majrds_oct16 WHERE FUNCLASS NOT IN ('1', '11')"
majhwys_onlyinterstate_data = "SELECT geom FROM vector.majhwys_oct16 WHERE FUNCLASS IN ('1', '11')"
rail_noise_data = "SELECT geom FROM vector.rails_2016 WHERE NET in ('A', 'R', 'X')"
railxing_noise_data = "SELECT geom FROM vector.rail_xing_2015"
soil_data = "SELECT DRAINAGECL, CORCON, CORSTEEL, geom FROM vector.nrcs_soils_nov15_orange"  # county level data
floodsoil_data = "SELECT flodfreqdc, geom FROM vector.nrcs_soils_nov15_orange" # county level data
sewagetrt_data = "SELECT geom FROM vector.sewtrt"

superfund_data = "SELECT geom FROM vector.epasuperfund_jun17 WHERE COUNTY = '{}'".format(studycounty)
statehazsite_data = "SELECT geom FROM vector.state_cleanup_sites_oct17 WHERE COUNTY = '{}'".format(studycounty)

highschool_data = "SELECT geom FROM vector.gc_schools_sep17 WHERE type IN ('COMBINATION JR. HIGH & SENIOR HIGH', 'SENIOR HIGH') AND low_grade IN ('10', '11', '12')"
middleschool_data = "SELECT geom FROM vector.gc_schools_sep17 WHERE high_grade IN ('07', '08')"
primaryschool_data = "SELECT geom FROM vector.gc_schools_sep17 WHERE high_grade IN ('01', '02', '03', '04', '05', '06', 'PK', 'KG')"
fireresque_data = "SELECT geom FROM vector.gc_firestat_feb13 WHERE county = '{}' AND type IN ('FIRE STATION', 'RESCUE STATION', 'FIRE STATION AND RESCUE STATION')".format(studycounty)
police_data = "SELECT geom FROM vector.gc_lawenforce_dec12 WHERE COUNTY = '{}' AND type IN ('FDLE', 'SHERIFF\''S OFFICE', 'POLICE DEPARTMENT', 'LAW ENFORCEMENT')".format(studycounty)
hospital_data = "SELECT geom FROM vector.gc_hospitals_sep17 WHERE COUNTY = '{}'".format(studycounty)

majrds_data = "SELECT geom FROM vector.majrds_oct16"
cntbnd_data = "SELECT geom FROM vector.cntbnd_sep15 WHERE NAME = '{}'".format(studycounty)
flma_data = "SELECT geom FROM vector.flma_jan18"
parks_data = "SELECT geom FROM vector.gc_parks_sep17 WHERE COUNTY = '{}'".format(studycounty)
nhdwaterbody_data = "SELECT geom FROM vector.nhd24waterbody_feb16 WHERE DESCRIPT IN ('LAKE/POND', 'RESERVOIR')"
prison_data = "SELECT geom FROM vector.gc_correctional_sep17 WHERE type IN ('CORRECTIONAL INSTITUTIONS', 'JAIL', 'FEDERAL CORRECTIONAL INSTITUTION')"
plss_data = "SELECT geom FROM vector.plss WHERE LOWER(county) = '{}' AND DESCRIPT ~ '^[0-9]'".format(studycounty.lower())
watertreat_data = "SELECT geom FROM vector.wtreat"
mgbank_data = "SELECT geom FROM vector.mgbank_apr17"

rowcrops_data = "SELECT land_cap, geom FROM vector.fsaid5_2016_alg WHERE LOWER(county) = '{}' AND crop2016 NOT IN ('Aquaculture', 'Citrus', 'Greenhouse/Nursery', 'Livestock')".format(studycounty.lower())
primefarm_data = "SELECT frmlndcl, geom FROM vector.nrcs_soils_nov15_orange"

# lsad10: 2010 Census legal/statistical area description code for place
# 25 - city; 43 - town
# pcicbsa10: 2010 Census metropolitan or micropolitan statistical area principal city indicator
# Y - Is a principal city of a CBSA
citypop_data = "SELECT pop2010, geom FROM vector.cenplace2010_aug11 WHERE pcicbsa10 = 'Y' OR lsad10 IN ('25', '43')"

nhdflowline_data = "SELECT geom FROM vector.nhd24flowline_dec17 WHERE descript IN ('COASTLINE', 'STREAM/RIVER', 'UNDERGROUND CONDUIT')"


