"""
ORM Module for Geotechnical Data Management

This module defines an object-relational mapping (ORM) for managing geotechnical data
related to Standard Penetration Tests (SPT), Cone Penetration Tests (CPT), shear wave
velocity profiles, and soil measurements. It uses the `peewee` library to interact
with a SQLite database and provides models and query functions for geotechnical records.
"""

from typing import Optional

from collections.abc import Iterator
from peewee import (
    JOIN,
    CompositeKey,
    DateField,
    FloatField,
    ForeignKeyField,
    IntegerField,
    Model,
    SqliteDatabase,
    TextField,
    fn,
)

db = SqliteDatabase("geodata.db")


class BaseModel(Model):
    """
    Base model for all database models.
    """

    class Meta:
        database = db


class Region(BaseModel):
    """
    Represents a region in the database.
    """

    region_id = IntegerField(primary_key=True)
    """int: The unique identifier for the region."""

    name = TextField()
    """str: The name of the region."""


class City(BaseModel):
    """
    Represents a city in the database.
    """

    city_id = IntegerField(primary_key=True)
    """int: The unique identifier for the city."""

    name = TextField()
    """str: The name of the city."""


class District(BaseModel):
    """
    Represents a district in the database.
    """

    district_id = IntegerField(primary_key=True)
    """int: The unique identifier for the district."""

    name = TextField()
    """str: The name of the district."""


class Suburb(BaseModel):
    """
    Represents a suburb in the database.
    """

    suburb_id = IntegerField(primary_key=True)
    """int: The unique identifier for the suburb."""

    name = TextField()
    """str: The name of the suburb."""


class NZGDRecord(BaseModel):
    """
    Represents a New Zealand Geotechnical Database (NZGD) record.
    """

    nzgd_id = IntegerField(primary_key=True)
    """int: The unique identifier for the NZGD record."""

    original_reference = TextField()
    """str: The original reference for the record."""

    investigation_date = DateField(formats=["%Y-%m-%d"])
    """date: The date the investigation was conducted."""

    published_date = DateField(formats=["%Y-%m-%d"])
    """date: The date the record was published."""

    latitude = FloatField()
    """float: The latitude coordinate of the investigation location."""

    longitude = FloatField()
    """float: The longitude coordinate of the investigation location."""

    region_id = ForeignKeyField(Region, backref="region")
    """int: The foreign key referencing the region."""

    district_id = ForeignKeyField(District, backref="district")
    """int: The foreign key referencing the district."""

    city_id = ForeignKeyField(City, backref="city")
    """int: The foreign key referencing the city."""

    suburb_id = ForeignKeyField(Suburb, backref="suburb")
    """int: The foreign key referencing the suburb."""


class SPTReport(BaseModel):
    """
    Represents a Standard Penetration Test (SPT) report.
    """

    borehole_id = IntegerField(primary_key=True)
    """int: The unique identifier for the borehole."""

    nzgd_id = ForeignKeyField(NZGDRecord, backref="spt_reports")
    """int: The foreign key referencing the associated NZGD record."""

    borehole_file = TextField()
    """str: The file associated with the borehole."""

    efficiency = FloatField()
    """float: The efficiency of the test."""

    borehole_diameter = FloatField()
    """float: The diameter of the borehole."""


class SPTMeasurements(BaseModel):
    """
    Represents measurements for a Standard Penetration Test (SPT).
    """

    id = IntegerField(primary_key=True)
    """int: The unique identifier for the measurement."""

    borehole_id = ForeignKeyField(SPTReport, backref="measurements")
    """int: The foreign key referencing the associated SPT report."""

    depth = FloatField()
    """float: The depth at which the measurement was taken."""

    n = IntegerField()
    """int: The N-value of the measurement."""


class VelocityProfiles(BaseModel):
    """
    Represents a velocity profile.
    """

    profile_id = IntegerField(primary_key=True)
    """int: The unique identifier for the velocity profile."""

    nzgd_id = ForeignKeyField(NZGDRecord, backref="velocity_profiles")
    """int: The foreign key referencing the associated NZGD record."""

    profile_file = TextField()
    """str: The file associated with the velocity profile."""


class VsMeasurements(BaseModel):
    """
    Represents shear wave velocity (Vs) measurements.
    """

    measurement_id = IntegerField(primary_key=True)
    """int: The unique identifier for the Vs measurement."""

    profile_id = ForeignKeyField(VelocityProfiles, backref="vs_measurements")
    """int: The foreign key referencing the associated velocity profile."""

    depth = FloatField()
    """float: The depth at which the Vs measurement was taken."""

    vs = FloatField()
    """float: The shear wave velocity at the specified depth."""


class CPTReport(BaseModel):
    """
    Represents a Cone Penetration Test (CPT) report.
    """

    cpt_id = IntegerField(primary_key=True)
    """int: The unique identifier for the CPT report."""

    nzgd_id = ForeignKeyField(NZGDRecord, backref="cpt_reports")
    """int: The foreign key referencing the associated NZGD record."""

    cpt_file = TextField()
    """str: The file associated with the CPT report."""


class CPTMeasurements(BaseModel):
    """
    Represents measurements for a Cone Penetration Test (CPT).
    """

    measurement_id = IntegerField(primary_key=True)
    """int: The unique identifier for the measurement."""

    cpt_id = ForeignKeyField(CPTReport, backref="measurements")
    """int: The foreign key referencing the associated CPT report."""

    depth = FloatField()
    """float: The depth at which the measurement was taken."""

    qc = FloatField()
    """float: The cone resistance at the specified depth."""

    fs = FloatField()
    """float: The sleeve friction at the specified depth."""

    u2 = FloatField()
    """float: The pore water pressure at the specified depth."""


class SoilMeasurements(BaseModel):
    """
    Represents soil measurements.
    """

    measurement_id = IntegerField(primary_key=True)
    """int: The unique identifier for the soil measurement."""

    report_id = ForeignKeyField(SPTReport, backref="soil_measurements")
    """int: The foreign key referencing the associated SPT report."""

    top_depth = FloatField()
    """float: The top depth of the soil layer."""


class SoilTypes(BaseModel):
    """
    Represents soil types.
    """

    id = IntegerField(primary_key=True)
    """int: The unique identifier for the soil type."""

    name = TextField()
    """str: The name of the soil type."""


class SoilMeasurementSoilType(BaseModel):
    """
    Represents a junction table for soil measurements and soil types.
    """

    soil_measurement_id = ForeignKeyField(SoilMeasurements, backref="soil_types")
    """int: The foreign key referencing the soil measurement."""

    soil_type_id = ForeignKeyField(SoilTypes, backref="measurements")
    """int: The foreign key referencing the soil type."""

    class Meta:
        primary_key = CompositeKey("soil_measurement_id", "soil_type_id")


class CPTMaxDepth(BaseModel):
    cpt_id = ForeignKeyField(
        CPTReport, primary_key=True, backref="max_depth", on_delete="CASCADE"
    )  # cpt_id as both PK and FK
    max_depth = FloatField()


def search_spt_reports(
    borehole_id: Optional[int] = None,
    min_efficiency: Optional[float] = None,
    max_efficiency: Optional[float] = None,
    min_diameter: Optional[float] = None,
    max_diameter: Optional[float] = None,
    nzgd_id: Optional[int] = None,
    original_reference: Optional[str] = None,
    max_measurement_depth: Optional[float] = None,
    min_measurement_depth: Optional[float] = None,
    region: Optional[str] = None,
    district: Optional[str] = None,
    city: Optional[str] = None,
    suburb: Optional[str] = None,
) -> Iterator[SPTReport]:
    """
    Search for SPT (Standard Penetration Test) reports based on various filters.

    Parameters
    ----------
    borehole_id : int, optional
        Specific borehole ID to filter results.
    min_efficiency : float, optional
        Minimum efficiency value for the SPT report.
    max_efficiency : float, optional
        Maximum efficiency value for the SPT report.
    min_diameter : float, optional
        Minimum borehole diameter.
    max_diameter : float, optional
        Maximum borehole diameter.
    nzgd_id : int, optional
        ID of the associated NZGDRecord to filter results.
    original_reference : str, optional
        Filter by a substring of the NZGDRecord's original reference.
    max_measurement_depth : float, optional
        Maximum depth of measurements in the SPT report.
    min_measurement_depth : float, optional
        Minimum depth of measurements in the SPT report.
    region_name : str, optional
        Name of the region to filter results.
    district_name : str, optional
        Name of the district to filter results.
    city_name : str, optional
        Name of the city to filter results.
    suburb_name : str, optional
        Name of the suburb to filter results.

    Returns
    -------
    Iterator of SPTReport
        A Iterator of `SPTReport` objects that match the specified criteria.
    """
    # Start with SPTReport and join related NZGDRecord
    query = SPTReport.select(SPTReport).join(NZGDRecord, JOIN.INNER)

    # Apply filters for SPTReport fields
    if borehole_id is not None:
        query = query.where(SPTReport.borehole_id == borehole_id)
    if min_efficiency is not None:
        query = query.where(SPTReport.efficiency >= min_efficiency)
    if max_efficiency is not None:
        query = query.where(SPTReport.efficiency <= max_efficiency)
    if min_diameter is not None:
        query = query.where(SPTReport.borehole_diameter >= min_diameter)
    if max_diameter is not None:
        query = query.where(SPTReport.borehole_diameter <= max_diameter)
    if nzgd_id is not None:
        query = query.where(NZGDRecord.nzgd_id == nzgd_id)
    if original_reference is not None:
        query = query.where(NZGDRecord.original_reference.contains(original_reference))
    if region is not None:
        query = (
            query.switch(NZGDRecord)
            .join(Region, JOIN.INNER, on=(NZGDRecord.region_id == Region.region_id))
            .where(Region.name == region)
        )
    if district is not None:
        query = (
            query.switch(NZGDRecord)
            .join(
                District,
                JOIN.INNER,
                on=(NZGDRecord.district_id == District.district_id),
            )
            .where(District.name == district)
        )
    if city is not None:
        query = (
            query.switch(NZGDRecord)
            .join(City, JOIN.INNER, on=(NZGDRecord.city_id == City.city_id))
            .where(City.name == city)
        )
    if suburb is not None:
        query = (
            query.switch(NZGDRecord)
            .join(Suburb, JOIN.INNER, on=(NZGDRecord.suburb_id == Suburb.suburb_id))
            .where(Suburb.name == suburb)
        )
    # Apply filter for maximum measurement depth
    if max_measurement_depth or min_measurement_depth:
        query = (
            query.switch(SPTReport)
            .join(SPTMeasurements, JOIN.INNER)
            .group_by(SPTReport.borehole_id)
        )
        if max_measurement_depth:
            query = query.having(fn.MAX(SPTMeasurements.depth) <= max_measurement_depth)
        if min_measurement_depth:
            query = query.having(fn.MAX(SPTMeasurements.depth) >= min_measurement_depth)

    # Execute query and return results
    return iter(query)


def search_cpt_reports(
    cpt_id: Optional[int] = None,
    nzgd_id: Optional[int] = None,
    original_reference: Optional[str] = None,
    region: Optional[str] = None,
    district: Optional[str] = None,
    city: Optional[str] = None,
    suburb: Optional[str] = None,
    max_measurement_depth: Optional[float] = None,
    min_measurement_depth: Optional[float] = None,
) -> Iterator[CPTReport]:
    """
    Search for CPT (Cone Penetration Test) reports based on various filters.

    Parameters
    ----------
    cpt_id : int, optional
        Specific CPT report ID to filter results.
    nzgd_id : int, optional
        ID of the associated NZGDRecord to filter results.
    original_reference : str, optional
        Filter by a substring of the NZGDRecord's original reference.
    region : str, optional
        Name of the region to filter results.
    district_name : str, optional
        Name of the district to filter results.
    city_name : str, optional
        Name of the city to filter results.
    suburb : str, optional
        Name of the suburb to filter results.
    max_measurement_depth : float, optional
        Maximum depth of measurements in the CPT report.
    min_measurement_depth : float, optional
        Minimum depth of measurements in the CPT report.

    Returns
    -------
    Iterator of CPTReport
        A Iterator of `CPTReport` objects that match the specified criteria.
    """
    # Start with CPTReport and join related NZGDRecord
    query = CPTReport.select(CPTReport).join(NZGDRecord, JOIN.INNER)

    # Apply filters for CPTReport fields
    if cpt_id is not None:
        query = query.where(CPTReport.cpt_id == cpt_id)
    if nzgd_id is not None:
        query = query.where(NZGDRecord.nzgd_id == nzgd_id)
    if original_reference is not None:
        query = query.where(NZGDRecord.original_reference.contains(original_reference))

    # Apply filters for location-related fields
    if region is not None:
        query = (
            query.switch(NZGDRecord)
            .join(Region, JOIN.INNER, on=(NZGDRecord.region_id == Region.region_id))
            .where(Region.name == region)
        )
    if district is not None:
        query = (
            query.switch(NZGDRecord)
            .join(
                District,
                JOIN.INNER,
                on=(NZGDRecord.district_id == District.district_id),
            )
            .where(District.name == district)
        )
    if city is not None:
        query = (
            query.switch(NZGDRecord)
            .join(City, JOIN.INNER, on=(NZGDRecord.city_id == City.city_id))
            .where(City.name == city)
        )
    if suburb is not None:
        query = (
            query.switch(NZGDRecord)
            .join(Suburb, JOIN.INNER, on=(NZGDRecord.suburb_id == Suburb.suburb_id))
            .where(Suburb.name == suburb)
        )

    # Apply filter for measurement depth
    if max_measurement_depth or min_measurement_depth:
        query = (
            query.switch(CPTReport)
            .join(CPTMaxDepth, JOIN.INNER)
            .group_by(CPTReport.cpt_id)
        )
        if max_measurement_depth:
            query = query.having((CPTMaxDepth.max_depth) <= max_measurement_depth)
        if min_measurement_depth:
            query = query.having((CPTMaxDepth.max_depth) >= min_measurement_depth)

    # Execute query and return results
    return iter(query)


def initialize_db():
    with db:
        db.create_tables(
            [
                CPTMeasurements,
                CPTReport,
                NZGDRecord,
                Region,
                SPTMeasurements,
                SPTReport,
                SoilMeasurementSoilType,
                SoilMeasurements,
                SoilTypes,
                VelocityProfiles,
                VsMeasurements,
                City,
                District,
                Suburb,
            ]
        )
