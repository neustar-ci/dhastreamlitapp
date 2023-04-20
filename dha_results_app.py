# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
import pandas as pd
import numpy as np


# Write directly to the app
st.title("Data Health Assessment")
st.write(
    """**Welcome to TransUnion Data Health Assessment!**
    [OneTru Audience Solutions](https://www.transunion.com/solution/truaudience).
    """
)

# Get the current credentials
session = get_active_session()

#TODO: add selector in UI
app_output_keys_tbl = "test1.public.output_batch_with_flags"

# batch id selection
all_batches = session.table(app_output_keys_tbl).order_by('LAST_PROCESSED').select(["BATCH_ID"]).distinct()
selected_batch_id = st.selectbox('Select batch to perform health check on', all_batches)

# base pd dataframe
pd_df = session.table(app_output_keys_tbl).filter(col('BATCH_ID').eqNullSafe(selected_batch_id)).to_pandas()
# create tabs
tab1, tab2, tab3 = st.tabs(['Summary Stats', 'Key Frequency', 'Matchflags Knockdown'])

with tab1:
    st.subheader("Summary Stats")
    col1, col2 = st.columns([8, 2]) 
    
    # get stats
    
    # total_num_records
    total_num_records = len(pd.unique(pd_df['INPUT_ID']))
    
    # unique_ids_w_ekeys
    data = pd_df[(pd_df['NSR_EKEY'].notnull())]
    unique_ids_w_ekeys = len(pd.unique(data['INPUT_ID']))
    
    # num_rec_invol_merge 
    data = pd_df.groupby('NSR_EKEY').filter(lambda x: len(x) > 1)
    num_rec_invol_merge = len(data['NSR_EKEY'])

    # num_ekey_or_hhid_present
    data = pd_df[(pd_df['NSR_EKEY'].notnull() & pd_df['NSR_HHID'].notnull())]
    num_ekey_or_hhid_present = len(pd.unique(data['INPUT_ID']))

    # num_ekey_but_no_hhid
    data = pd_df[(pd_df['NSR_EKEY'].notnull() & pd_df['NSR_HHID'].isnull())]
    num_ekey_but_no_hhid = len(pd.unique(data['INPUT_ID']))

    # num_uniq_ekey_hhid
    data = pd_df[(pd_df['NSR_EKEY'].notnull() & pd_df['NSR_HHID'].notnull())]
    data['ekey_plus_hhid'] = data['NSR_EKEY'] + data['NSR_HHID']
    num_uniq_ekey_hhid = len(pd.unique(data['ekey_plus_hhid']))

    
    with col1:
        st.write("Number of input records ")
        st.write("Total Number of Unique IDs with Ekey ")
        st.write("Number of records involved in merge ")
        st.write("Number of records where Ekey and HouseholdIDs are populated")
        st.write("Number of records where Ekey is populated but HouseholdIDs are blank")
        st.write("Total Number of Distinct (HouseholdID + Ekey) ")
    with col2:
        st.write("{0} ({1}%)".format(total_num_records, 100))
        st.write("{0} ({1:.2f}%)".format(unique_ids_w_ekeys, 100 * unique_ids_w_ekeys/total_num_records))
        st.write("{0} ({1:.2f}%)".format(num_rec_invol_merge, 100 * num_rec_invol_merge/total_num_records))
        st.write("{0} ({1}%)".format(num_ekey_or_hhid_present, 100 * num_ekey_or_hhid_present/total_num_records))
        st.write("{0} ({1}%)".format(num_ekey_but_no_hhid, 100 * num_ekey_but_no_hhid/total_num_records))
        st.write("{0} ({1}%)".format(num_uniq_ekey_hhid, 100 * num_uniq_ekey_hhid/total_num_records))

with tab2:
    st.subheader("EKey Frequency")
    with st.container():
        sf = pd_df['NSR_EKEY'].value_counts()
        zero_count = pd_df['NSR_EKEY'].isna().sum()
        data = pd.DataFrame(sf.reset_index(name = "occurrences")).groupby('occurrences', as_index=False, sort=True).count()
        zero_row = {'occurrences': 0, 'index':zero_count}
        data = data.append(zero_row, ignore_index=True).sort_values(by='occurrences')
        data ['percent_occurrences'] = 100*(data['index']/total_num_records)
        st.bar_chart(data=data.set_index('occurrences'), y='percent_occurrences', width=850, height=500, use_container_width=True)
    st.subheader("HHID Frequency")
    with st.container():
        sf = pd_df['NSR_HHID'].value_counts()
        zero_count = pd_df['NSR_HHID'].isna().sum()
        data = pd.DataFrame(sf.reset_index(name = "occurrences")).groupby('occurrences', as_index=False, sort=True).count()
        zero_row = {'occurrences': 0, 'index':zero_count}
        data = data.append(zero_row, ignore_index=True).sort_values(by='occurrences')
        data ['percent_occurrences'] = 100*(data['index']/total_num_records)
        st.bar_chart(data=data.set_index('occurrences'), y='percent_occurrences', width=850, height=500, use_container_width=True)

with tab3:
    st.subheader("Matchflags Knockdown")
    pd_df['FNAMESCORE'] = pd_df['FNAMESCORE'].fillna(0)
    pd_df['LNAMESCORE'] = pd_df['FNAMESCORE'].fillna(0)
    pd_df['ADDRESSFLAG'] = pd_df['ADDRESSFLAG'].fillna('b')
    pd_df['PHONEFLAG'] = pd_df['PHONEFLAG'].fillna('b')
    pd_df['First_Name_Match_Flag'] = np.where(pd_df['FNAMESCORE'].astype('int').between(80, 99, inclusive='both'), 'I', 
        np.where(pd_df['FNAMESCORE'].astype('int') == 100, 'E', np.where(pd_df['FNAMESCORE'].astype('int').between(1, 79, inclusive='both'), 'N', 'b')))
    pd_df['Last_Name_Match_Flag'] = np.where(pd_df['LNAMESCORE'].astype('int').between(80, 99, inclusive='both'), 'I', 
        np.where(pd_df['FNAMESCORE'].astype('int') == 100, 'E', np.where(pd_df['LNAMESCORE'].astype('int').between(1, 79, inclusive='both'), 'N', 'b')))
    with st.container():
        
        st.bar_chart(data = pd_df['First_Name_Match_Flag'].value_counts(), width=850, height=500, use_container_width=True)
        st.bar_chart(data = pd_df['Last_Name_Match_Flag'].value_counts(), width=850, height=500, use_container_width=True)
        st.bar_chart(data = pd_df['ADDRESSFLAG'].value_counts(), width=850, height=500, use_container_width=True)
        st.bar_chart(data = pd_df['PHONEFLAG'].value_counts(), width=850, height=500, use_container_width=True)
        