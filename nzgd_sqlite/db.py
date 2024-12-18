"""
Dataclass Module for Geotechnical Data Management

This module defines a set of dataclasses for managing geotechnical data related to
Standard Penetration Tests (SPT), Cone Penetration Tests (CPT), shear wave velocity
profiles, and soil measurements.
"""

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Optional, Self

import pandas as pd
from intervaltree import Interval, IntervalTree

from nzgd_sqlite import orm


# Enum: SoilTypes
class SoilTypesEnum(Enum):
    """
    Enum representing different types of soil.
    """

    SAND = "SAND"
    SILT = "SILT"
    CLAY = "CLAY"
    GRAVEL = "GRAVEL"
    BOULDERS = "BOULDERS"


@dataclass
class NZGDRecord:
    """
    A data class representing a New Zealand Geotechnical Database (NZGD) record.
    """

    nzgd_id: int
    """int: The unique identifier for the NZGD record."""

    original_reference: str
    """str: The original reference for the record."""

    investigation_date: date
    """date: The date the investigation was conducted."""

    published_date: date
    """date: The date the record was published."""

    latitude: float
    """float: The latitude coordinate of the investigation location."""

    longitude: float
    """float: The longitude coordinate of the investigation location."""

    region: str
    """str: The region of the investigation location."""

    city: str
    """str: The city of the investigation location."""

    district: str
    """str: The district of the investigation location."""

    suburb: str
    """str: The suburb of the investigation location."""

    @classmethod
    def from_orm(cls, record: orm.NZGDRecord) -> Self:
        """
        Create an NZGDRecord instance from an ORM record.

        Parameters
        ----------
        record : orm.NZGDRecord
            The ORM record to convert.

        Returns
        -------
        NZGDRecord
            The corresponding NZGDRecord instance.
        """
        return cls(
            nzgd_id=record.nzgd_id,
            original_reference=record.original_reference,
            investigation_date=record.investigation_date,
            published_date=record.published_date,
            latitude=record.latitude,
            longitude=record.longitude,
            region=record.region.name,
            district=record.district.name,
            city=record.city.name,
            suburb=record.suburb.name,
        )


@dataclass
class SPTReport:
    """
    A data class representing a Standard Penetration Test (SPT) report.
    """

    borehole_id: int
    """int: The unique identifier for the borehole."""

    borehole_file: str
    """str: The file associated with the borehole."""

    efficiency: float
    """float: The efficiency of the test."""

    borehole_diameter: float
    """float: The diameter of the borehole."""

    nzgd_record: NZGDRecord
    """NZGDRecord: The NZGD record associated with the report."""

    measurements: pd.DataFrame = field(default_factory=pd.DataFrame)
    """pd.DataFrame: A DataFrame containing SPT measurements."""

    soil_measurements: IntervalTree = field(default_factory=IntervalTree)
    """IntervalTree: An IntervalTree containing soil measurements."""

    @classmethod
    def from_orm(cls, report: orm.SPTReport) -> Self:
        """
        Create an SPTReport instance from an ORM report.

        Parameters
        ----------
        report : orm.SPTReport
            The ORM report to convert.

        Returns
        -------
        SPTReport
            The corresponding SPTReport instance.
        """
        # Create a DataFrame for SPT measurements
        measurements_data = [
            {"depth": m.depth, "n_value": m.n}
            for m in sorted(report.measurements, key=lambda x: x.depth)
        ]
        measurements_df = pd.DataFrame(measurements_data)

        # Create an IntervalTree for soil measurements
        soil_measurements_tree = IntervalTree()
        for s in sorted(report.soil_measurements, key=lambda x: x.top_depth):
            for soil_type in s.soil_types:
                soil_measurements_tree.add(Interval(s.top_depth, s.bottom_depth, s))

        return cls(
            borehole_id=report.borehole_id,
            borehole_file=report.borehole_file,
            efficiency=report.efficiency,
            borehole_diameter=report.borehole_diameter,
            measurements=measurements_df,
            soil_measurements=soil_measurements_tree,
            nzgd_record=NZGDRecord.from_orm(report.nzgd_id),
        )


@dataclass
class CPTReport:
    """
    A data class representing a Cone Penetration Test (CPT) report.
    """

    cpt_id: int
    """int: The unique identifier for the CPT."""

    cpt_file: str
    """str: The file associated with the CPT."""

    measurements: pd.DataFrame = field(default_factory=pd.DataFrame)
    """pd.DataFrame: A DataFrame containing CPT measurements."""

    @classmethod
    def from_orm(cls, report: orm.CPTReport) -> Self:
        """
        Create a CPTReport instance from an ORM report.

        Parameters
        ----------
        report : orm.CPTReport
            The ORM report to convert.

        Returns
        -------
        CPTReport
            The corresponding CPTReport instance.
        """
        # Create a DataFrame for CPT measurements
        measurements_data = [
            {"depth": m.depth, "qc": m.qc, "fs": m.fs, "u2": m.u2}
            for m in sorted(report.measurements, key=lambda x: x.depth)
        ]
        measurements_df = pd.DataFrame(measurements_data)

        return cls(
            cpt_id=report.cpt_id,
            cpt_file=report.cpt_file,
            measurements=measurements_df,
        )


@dataclass
class VelocityProfile:
    """
    A data class representing a velocity profile.
    """

    profile_id: int
    """int: The unique identifier for the velocity profile."""

    profile_file: str
    """str: The file associated with the velocity profile."""

    vs_measurements: pd.DataFrame = field(default_factory=pd.DataFrame)
    """pd.DataFrame: A DataFrame containing velocity measurements."""

    @classmethod
    def from_orm(cls, profile: orm.VelocityProfiles) -> Self:
        """
        Create a VelocityProfile instance from an ORM profile.

        Parameters
        ----------
        profile : orm.VelocityProfiles
            The ORM profile to convert.

        Returns
        -------
        VelocityProfile
            The corresponding VelocityProfile instance.
        """
        # Create a DataFrame for Vs measurements
        vs_data = [
            {"depth": vm.depth, "vs": vm.vs}
            for vm in sorted(profile.vs_measurements, key=lambda x: x.depth)
        ]
        vs_df = pd.DataFrame(vs_data)

        return cls(
            profile_id=profile.profile_id,
            profile_file=profile.profile_file,
            vs_measurements=vs_df,
        )


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
) -> list[SPTReport]:
    """
    Search for SPT reports based on the given filters.

    Parameters
    ----------
    borehole_id : Optional[int], optional
        The borehole ID to filter by.
    min_efficiency : Optional[float], optional
        The minimum efficiency to filter by.
    max_efficiency : Optional[float], optional
        The maximum efficiency to filter by.
    min_diameter : Optional[float], optional
        The minimum borehole diameter to filter by.
    max_diameter : Optional[float], optional
        The maximum borehole diameter to filter by.
    nzgd_id : Optional[int], optional
        The NZGD ID to filter by.
    original_reference : Optional[str], optional
        The original reference to filter by.
    max_measurement_depth : Optional[float], optional
        The maximum measurement depth to filter by.
    min_measurement_depth : Optional[float], optional
        The minimum measurement depth to filter by.
    region : Optional[str], optional
        The region to filter by.

    Returns
    -------
    list[SPTReport]
        A list of SPTReport instances that match the filter criteria.
    """
    return [
        SPTReport.from_orm(report)
        for report in orm.search_spt_reports(
            borehole_id=borehole_id,
            min_efficiency=min_efficiency,
            max_efficiency=max_efficiency,
            min_diameter=min_diameter,
            max_diameter=max_diameter,
            nzgd_id=nzgd_id,
            original_reference=original_reference,
            max_measurement_depth=max_measurement_depth,
            min_measurement_depth=min_measurement_depth,
            region_name=region,
        )
    ]
