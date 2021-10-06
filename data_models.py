import os

from peewee import Model, CharField, \
    ForeignKeyField, TextField, DateTimeField, BooleanField, IntegerField, FloatField
from playhouse.postgres_ext import PostgresqlExtDatabase
from dataclasses import dataclass
import datetime
import sys


@dataclass
class Employee:
    """Representation of an employee in a company."""
    full_name: str
    first_name: str
    last_name: str
    position: str
    company: str
    email: str
    rank_score: int

    # Debug info
    search_title: str
    linkedin_url: str
    pre_snippet: str

    # For enabling the sort function and make it return highest rank first
    def __lt__(self, other):
        return self.rank_score > other.rank_score


@dataclass
class Address:
    """Address data for a company"""
    address: str
    borough: str
    line1: str
    city: str
    zip: str
    region: str
    country_code: str


@dataclass
class MapsData:
    """Holds all data that is relating to the google maps result"""
    search_position: int
    lat: float
    long: float
    rating: float
    reviews: int
    type: str
    thumbnail: str


@dataclass
class Company:
    """Representation of a single company."""

    name: str
    website: str
    address: Address
    phone: str
    employees: [Employee]
    gmaps_data: MapsData
    done: bool = False


db_pass = os.environ.get("DB_PASS")
db = PostgresqlExtDatabase("arthur", user="arthur", password=db_pass, options="-c search_path=b2b")

class BaseModel(Model):
    class Meta:
        database = db


class QueryModel(BaseModel):
    sector = CharField(default=None, null=True)
    location = CharField(default=None, null=True)
    type = CharField()
    maps_results = IntegerField(default=None, null=True)
    search_results = IntegerField(default=None, null=True)

    started_at = DateTimeField(default=datetime.datetime.utcnow)
    finished_at = DateTimeField(default=None, null=True)

    class Meta:
        table_name = 'queries'


class CompanyModel(BaseModel):
    name = CharField()
    website = CharField()
    phone = CharField(default=None, null=True)

    full_address = CharField(null=True)
    borough = CharField(null=True)
    line1 = CharField(null=True)
    city = CharField(null=True)
    zip = CharField(null=True)
    region = CharField(null=True)
    country_code = CharField(null=True)
    contact_email = CharField(null=True)
    other_emails = TextField(null=True)

    # Socials
    linkedin = CharField(null=True)
    twitter = CharField(null=True)
    facebook = CharField(null=True)
    instagram = CharField(null=True)
    youtube = CharField(null=True)

    query = ForeignKeyField(QueryModel, backref='company')

    class Meta:
        table_name = 'companies'


class EmployeeModel(BaseModel):
    full_name = CharField()
    first_name = CharField()
    last_name = CharField()
    position = CharField()
    extracted_company = CharField()
    company = ForeignKeyField(CompanyModel, backref='employees')
    email = CharField()
    rank_score = IntegerField()

    search_title = CharField()
    pre_snippet = CharField(null=True, default=None)
    linkedin_url = CharField()

    class Meta:
        table_name = 'employees'


class MapsDataModel(BaseModel):
    search_position = IntegerField()
    lat = FloatField()
    long = FloatField()
    rating = FloatField()
    reviews = IntegerField()
    type = CharField()
    thumbnail = CharField(null=True, default=None)
    company = ForeignKeyField(CompanyModel, backref='maps_data')

    class Meta:
        table_name = 'companies_maps_data'


if __name__ == "__main__":
    # Creates the tables, if not already there
    db.create_tables([QueryModel, MapsDataModel, CompanyModel, EmployeeModel])

    # Run a migration if called with 'migrate' CLI arg.
    # Needs to be run once everytime the DB struct is changed.
    if len(sys.argv) > 1 and sys.argv[1] == "migrate":
        from playhouse.migrate import *
        migrator = PostgresqlMigrator(db)
        # Migration code goes here

