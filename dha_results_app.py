import streamlit as st
import pandas as pd

# create pages
page = st.sidebar.radio('Pages', ['First Page', 'Second Page'])

# create dataframe
if 'df' not in st.session_state:
    d = {'col1': [1, 2], 'col2': [3, 4]}
    st.session_state['df'] = pd.DataFrame(data=d)

if page == "First Page":
    # do some stuff to the data
    st.session_state.df['col3'] = st.session_state.df['col1'] * st.session_state.df['col2']
    st.write(st.session_state.df)
elif page == 'Second Page':
    st.session_state.df['col4'] = st.session_state.df['col1'] * st.session_state.df['col2']
    st.write(st.session_state.df)