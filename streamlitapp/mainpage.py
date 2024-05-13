import streamlit as st
import matplotlib.pyplot as plt
import plotly.express as px

st.header('Stock Price DASHBOARD')


sidebar=st.sidebar.title('Side Bar')

col1, col2 = st.columns(2)

# Add content to the first column
with col1:
    st.header("Column 1")
    st.write("This is the first column")

# Add content to the second column
with col2:
    st.header("Column 2")
    st.write("This is the second column")