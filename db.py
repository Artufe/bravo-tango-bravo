from peewee import SqliteDatabase, Model, CharField, \
    ForeignKeyField, TextField, DateTimeField, BooleanField, IntegerField, FloatField
import datetime


db = SqliteDatabase('b2b_database.db')


class BaseModel(Model):
    class Meta:
        database = db


class QueryModel(BaseModel):
    sector = CharField()
    location = CharField()
    type = CharField()
    maps_results = IntegerField(default=None, null=True)
    search_results = IntegerField(default=None, null=True)

    started_at = DateTimeField(default=datetime.datetime.utcnow)
    finished_at = DateTimeField(default=None, null=True)

    class Meta:
        table_name = 'queries'


class CompanyModel(BaseModel):
    name = CharField()
    website: CharField()
    phone: CharField()

    full_address = CharField()
    borough = CharField()
    line1 = CharField()
    city = CharField()
    zip = CharField()
    region = CharField()
    country_code = CharField()

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
    pre_snippet = CharField()
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
    thumbnail = CharField()
    company = ForeignKeyField(CompanyModel, backref='maps_data')

    class Meta:
        table_name = 'companies_maps_data'
