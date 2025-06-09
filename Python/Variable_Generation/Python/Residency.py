# -*- coding: utf-8 -*-
"""
Computing Residency varibles in Idealist Varible Generation.

author: ruppert20
"""
import pandas as pd
import numpy as np
from .Utilities.Logging.log_messages import log_print_email_message as logm


def generate_residency_variables(df: pd.DataFrame, zcta_df: pd.DataFrame, zipcoord: pd.DataFrame, **logging_kwargs) -> pd.DataFrame:
    """
    Generate All residency Variables.

    Actions
    -------
    1. Extract Five Digit Zipcode
    2. Find the closest zipcode in the ZCTA table to annoate residency characteristics
    3. Add residency characteristics
    4. Calculate distance from facilty to zip

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame must contain following columns:
            *zip.
    zcta_df: pandas.DataFrame
        DataFrame corresponding to "ZCTA.csv"
        Input DataFrame must contain following columns:
            * zip
            * total
            * rural
            * median_income
            * perc_below_poverty
            * prop_black
            * prop_hisp
    zipcoord : pd.DataFrame
        Input DataFrame must contain following columns:
            *postal_code
            *latitude
            *longitude

    Returns
    -------
    df : pd.DataFrame
        DESCRIPTION.

    """
    # extract zip 5
    df.loc[:, 'zip_5'] = df.zip.str.extract(r'([0-9]{5})')[0].values
    df.loc[:, 'zip_9'] = df.zip.str.extract(r'([0-9]{9})|([0-9]{5}-[0-9]{4})')[0].str.replace('-', '').values
    df.drop(columns=['zip'], inplace=True)

    # identify zipcodes in zcta_df
    zip_mask: pd.Series = df.zip_5.isin(zcta_df.zip.unique())
    df.loc[zip_mask, 'zcta_zip'] = df.loc[zip_mask, 'zip_5'].copy()

    # identify zipcodes not in zcta and find closest match
    zcta_coords: pd.DataFrame = zipcoord[zipcoord.postal_code.isin(zcta_df.zip)].reset_index(drop=True)
    missing_zips: list = df.zip_5[df.zcta_zip.isnull() & df.zip_5.notnull()].tolist()
    if len(missing_zips) > 0:
        logm(message=f'Found {len(missing_zips)}', **logging_kwargs)
    for tzip in missing_zips:
        logm(message=f'Finding Closes Neighboring zip to {tzip}', **logging_kwargs)
        lat: pd.Series = zipcoord.latitude[zipcoord.postal_code == tzip]
        if lat.shape[0] > 0:
            lat: float = lat.iloc[0]
            long: float = zipcoord.longitude[zipcoord.postal_code == tzip].iloc[0]
            closest_zip: str = zcta_coords.loc[np.argmin(np.sqrt((zcta_coords.latitude - lat)**2
                                                                 + (zcta_coords.longitude - long)**2)), 'postal_code']
        else:
            closest_zip: str = 'missing'
        matched_zip_mask: pd.Series = (df.zip_5 == tzip)
        df.loc[matched_zip_mask, 'zcta_zip'] = closest_zip

    # integrate residency varaibles from zcta table
    df = df.merge(zcta_df, left_on='zcta_zip', right_on='zip', how='left')\
        .drop(columns=['zcta_zip', 'zcta', 'zip_y', 'zip', 'zip_x'], errors='ignore')

    # calculate distance from home to facilty in miles
    unique_zips: pd.DataFrame = df.loc[df.zip_5.isin(zipcoord.postal_code),
                                       ['zip_5', 'facility_zip']].drop_duplicates().copy()\
        .merge(zipcoord[['postal_code', 'latitude', 'longitude']]
               .rename(columns={'postal_code': 'zip_5',
                                'latitude': 'zip_lat',
                                'longitude': 'zip_long'}),
               on='zip_5',
               how='left')\
        .merge(zipcoord[['postal_code', 'latitude', 'longitude']]
               .rename(columns={'postal_code': 'facility_zip',
                                'latitude': 'fac_lat',
                                'longitude': 'fac_long'}),
               on='facility_zip',
               how='left')
    logm(message=f'Calculating Distance Between zipcodes and the respetive facilities for {unique_zips.shape[0]} zip codes', **logging_kwargs)
    unique_zips['distance_to_facility'] = haversine_distance(x=unique_zips[['zip_lat', 'zip_long']].values,
                                                             y=unique_zips[['fac_lat', 'fac_long']].values)

    # unique_zips.apply(lambda row: haversine_distance(x=row[['zip_lat', 'zip_long']].values,
    #                                                  y=row[['fac_lat', 'fac_long']].values), axis=1)
    df = df.merge(unique_zips[['distance_to_facility', 'zip_5', 'facility_zip']], on=['zip_5', 'facility_zip'], how='left')

    return df


def haversine_distance(x, y):
    """Calculate Haversine (great circle) distance.

    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)

    Parameters
    ----------
    x : array, shape=(n_samples, 2)
      the first list of coordinates (degrees)
    y : array: shape=(n_samples, 2)
      the second list of coordinates (degress)

    Returns
    -------
    d : array, shape=(n_samples,)
      the distance between corrdinates (miles)

    References
    ----------
    https://en.wikipedia.org/wiki/Great-circle_distance
    """
    EARTH_RADIUS = 6371.009

    x_rad = np.radians(x)
    y_rad = np.radians(y)

    d = y_rad - x_rad

    dlat, dlon = d.T
    x_lat = x_rad[:, 0]
    y_lat = y_rad[:, 0]

    a = (
        np.sin(dlat / 2.0) ** 2
        + np.cos(x_lat) * np.cos(y_lat) * np.sin(dlon / 2.0) ** 2
    )

    c = 2 * np.arcsin(np.sqrt(a))
    return EARTH_RADIUS * c / 1.609
