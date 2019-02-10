import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, sessionmaker, scoped_session
from sqlalchemy import create_engine, func

import datetime as dt
import pandas as pd

from flask import Flask, jsonify, json

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

Measurement = Base.classes.measurement
Station = Base.classes.station

# Creating a scoped session here due to issue with computer creating multiple threads to use the session
session = scoped_session(sessionmaker(bind=engine))



# Create a FLASK API
app = Flask(__name__)

# Close the session after each route uses it
@app.teardown_request
def remove_session(ex=None):
    session.remove()

@app.route("/")
def home():
    print("Server request for Home page")
    return (
        f"Home page.<br/>"
        f"These are your options:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start_date<br/>"
        f"<li>Enter start date as Year-Month-Day. Example: /api/v1.0/2017-03-27</li><br/>"
        f"/api/v1.0/start_date/end_date<br/>"
        f"<li>Enter start date and end date as Year-Month-Day. Example: /api/v1.0/2017-03-27/2017-08-03</li>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    print("Server request for precipitation")

    # Query Measurement and obtain all dates and precipitation values
    results = session.query(Measurement).all()
    all_precip = []
    for each in results:
        precip_dict = {}
        precip_dict['date'] = each.date
        precip_dict['prcp'] = each.prcp
        all_precip.append(precip_dict)

    return jsonify(all_precip)

@app.route("/api/v1.0/stations")
def stations():
    print("Server request for stations")

    # Query all stations in Station
    stations_q = session.query(Station.station).all()
    station_list = []
    for station in stations_q:
        station_dict = {}
        station_dict['station'] = station.station
        station_list.append(station_dict)

    return jsonify(station_list)

@app.route("/api/v1.0/tobs")
def tobs():
    print("Received query for tobs")

    ## Design a query to retrieve the last 12 months of precipitation data and plot the results

    # Calculate the last data point in the database
    last_measurement_query = session.query(Measurement).order_by(Measurement.date.desc()).first()
    last_date_dict = last_measurement_query.__dict__
    last_date = last_date_dict['date']
    print(f'The last date in the database is: {last_date}')

    # Calculate the date 1 year ago from the last data point in the database
    date_12_months_ago = dt.date.fromisoformat(last_date) - dt.timedelta(weeks = 52, days=1)
    print(f'The date 12 months prior to the last date in the database is: {date_12_months_ago}')
    
    # Perform a query to retrieve the data and precipitation scores
    last_twelve_precip_data = session.query(Measurement.date, Measurement.prcp).\
        order_by(Measurement.date).\
        filter(Measurement.date >= date_12_months_ago).statement

    # Save the query results as a Pandas DataFrame and set the index to the date column
    precip_data_df = pd.read_sql_query(last_twelve_precip_data, session.bind)
    # Sort the dataframe by date
    precip_data_df.columns = ['date', 'precipitation']
    precip_json = json.loads(precip_data_df.to_json(orient='records'))
    return jsonify(precip_json)

@app.route("/api/v1.0/<start>")
def calc_temps_start(start):
# This function called `calc_temps_start` will accept a start date in the format '%Y-%m-%d' and calculate Tmin, Tavg, Tmax
# for all dates greater than and equal to the start date

    """TMIN, TAVG, and TMAX for a date.
    
    Args:
        start_date (string): A date string in the format %Y-%m-%d
        
    Returns:
        TMIN, TAVE, and TMAX
    """
    temp_calcs = session.query(Measurement.date, func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        group_by(Measurement.date).\
        filter(Measurement.date >=start).all()

    print(temp_calcs)

    temp_sum=[]
    for each in temp_calcs:
        temp_sum_dict = {}
        temp_sum_dict['Date']=each[0]
        temp_sum_dict['Minimum Tempature']=each[1]
        temp_sum_dict['Average Temperature']=each[2]
        temp_sum_dict['Maximum Temperature']=each[3]
        temp_sum.append(temp_sum_dict)

    return jsonify(temp_sum)

@app.route("/api/v1.0/<start>/<end>")
def calc_temps(start, end):

# This function called `calc_temps` will accept start date and end date in the format '%Y-%m-%d' 
# and return the minimum, average, and maximum temperatures for that range of dates

    """TMIN, TAVG, and TMAX for a list of dates.
    
    Args:
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d
        
    Returns:
        TMIN, TAVE, and TMAX
    """
    temp_calcs = session.query(Measurement.date, func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        group_by(Measurement.date).\
        filter(Measurement.date >= start).filter(Measurement.date <= end).all()

    temp_sum = []
    for each in temp_calcs:
        temp_sum_dict = {}
        temp_sum_dict['Date'] = each[0]
        temp_sum_dict['Minimum Tempature']=each[1]
        temp_sum_dict['Average Temperature']=each[2]
        temp_sum_dict['Maximum Temperature']=each[3]
        temp_sum.append(temp_sum_dict)

    return jsonify(temp_sum)


if __name__ =="__main__":
    app.run(debug=True)