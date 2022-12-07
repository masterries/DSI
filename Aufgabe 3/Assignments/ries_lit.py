#build a Streamlit application, that allows exploration of the dataset.
#mongodb airbnb dataset.

#connect to mongodb
import streamlit as st
import pandas as pd
import numpy as np
import pymongo
import ssl
from pymongo import MongoClient


def load_data():
    client = MongoClient(
        'mongodb+srv://student:nRfKfRqEtgWvznFD@cluster0.gu4ru.mongodb.net')
    #load 100 entries
    db = client.get_database('sample_airbnb')

    data = pd.DataFrame(db.listingsAndReviews.find().limit(1000))
    data["price"] = data.price.astype(str).astype(float)
    return data






#def render_info():
def render_info():
    st.title('Airbnb data')
    st.header('Displaying the airbnb dataset')
    st.markdown('Using streamlit to filter and display data fetched from a mongodb database.')

#get Coordinates (NOT USED) (Not working key error)???
def get_coordinates(data):
    coordinates = []
    copy_data = data.copy()
    for i in range(len(data)-1):
        #check if exits

        if copy_data['address'][i] != None:
            address = copy_data['address'][i]
            if address is not None:
                    point = (address['location'])
                    #print(point)
                    coordinates.append(point)

    return coordinates

#draw map
def render_map(data):
#    location    = get_coordinates(data)
    df_map      = pd.DataFrame([pd.Series(point['location']["coordinates"], index=['lon', 'lat']) for point in data["address"]], columns=['lon', 'lat'])
    st.map(df_map)

#make price slider
def filter_by_price(data):
    #adding a range slider, allowing to choose values in range 
    range = st.slider('Price', int(min(data['price'])), int(max(data['price'])), (20, 400))

    return data[(data['price'].between(*range))]

#filter by property_type
def filter_by_property_type(data):
    property_type = st.sidebar.selectbox('Filter by different property types:', [''] + list(data['property_type'].drop_duplicates()))
    if property_type:
        return data[data['property_type'] == property_type]
    return data







render_info()
data = load_data()
data1 = filter_by_price(data)
data1 = filter_by_property_type(data1)


print(data1)
render_map(data1)

