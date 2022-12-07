#build a Streamlit application, that allows exploration of the dataset.
#mongodb airbnb dataset.

#connect to mongodb
import streamlit as st
import pandas as pd
import numpy as np
import pymongo
import ssl
from pymongo import MongoClient

@st.cache(allow_output_mutation=True)
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

#plot histogram of price
def plot_histogram(data):
    st.subheader('Price distribution')
    st.write('The distribution of the price of the listings')
    import plotly.express as px

    st.plotly_chart(px.histogram(data, x='price', nbins=20, title='Price distribution'))

#make a suggestion
def make_suggestion(data):
    st.subheader('Suggestion')
    st.write('The most expensive listing is:')
    st.write(data[data['price'] == max(data['price'])])

#make a suggestion by number of reviews
def make_suggestion_by_reviews(data):
    st.subheader('Suggestion')
    st.write('Our Suggestions for your next trip:')
    mostViewed = data[data['number_of_reviews'] == max(data['number_of_reviews'])]
    #display the image of the most viewed listing images -> picture_url
    img = mostViewed["images"]
    img = list(img)
    img = img[0]['picture_url']
    st.image(img)
    #st.write(mostViewed)
    name = mostViewed["name"].values[0]
    
    price = mostViewed["price"].values[0]
    st.write('The most viewed listing is: ', name, 'with a price of: ', price, '€ per night.')
    #get the description of the most viewed listing
    description = mostViewed["summary"].values[0]
    st.write('Description: ', description)
    url = mostViewed["listing_url"].values[0]
    if st.button('Book now!'):
        st.write('Redirecting to: ', url)
        import webbrowser
        webbrowser.open(url)


    









render_info()
print('loading data')
data = load_data()
print('data loaded')
data = filter_by_price(data)
data = filter_by_property_type(data)


make_suggestion_by_reviews(data)
plot_histogram(data)

make_suggestion(data)
#print(data)
render_map(data)

