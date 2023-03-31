import streamlit as st
import pandas as pd
from snowflake.snowpark.session import Session

# create pages
page = st.sidebar.radio('Data Health Anlysis', ['Summary Stats', 'Drilldown'])

# create snowflake session
session = Session.builder.configs(**st.secrets.db_credentials).create()
st.write(session.sql('select current_warehouse(), current_database(), current_schema()').collect())

# create dataframe
# if 'df' not in st.session_state:
#     d = {'col1': [1, 2], 'col2': [3, 4]}
#     st.session_state['df'] = pd.DataFrame(data=d)

if page == "Summary Stats":
    # get stats
    snowdf = session.sql("""SELECT coalesce(sum(a.ct),0) as no_rec_invol_merge 
    FROM (SELECT count(distinct input_id) as ct 
    FROM demo_consumer.local_processing.output_batch_with_flags out 
    WHERE trim(coalesce(nsr_ekey,'')) != '' 
    AND out.batch_id in ('c94bef7d-c18b-4593-aee2-7efaef717732')
    GROUP BY trim(coalesce(nsr_ekey,'')) 
    HAVING ct > 1 ) a ;
    """).collect()
    # st.session_state.df[''] = st.session_state.df['col1'] * st.session_state.df['col2']
    # st.write(st.session_state.df)
    st.write(snowdf)
elif page == "Drilldown":
    st.session_state.df['col4'] = st.session_state.df['col1'] * st.session_state.df['col2']
    st.write(st.session_state.df)