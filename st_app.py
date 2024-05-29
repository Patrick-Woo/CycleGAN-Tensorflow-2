"""
# My first app
Here's our first attempt at using data to create a table:
    """

import streamlit as st
import pandas as pd
import numpy as np
#from streamlit_option_menu import option_menu
from io import StringIO
import os
#import cantools
#import can
from datetime import datetime
import time



from time import sleep

#3-0. 保存upload files to 服务器上的path
def save_uploaded_file(uploaded_file, path):
    with open(os.path.join(path, uploaded_file.name), "wb") as f:
        f.write(uploaded_file.getbuffer())
    return st.success(("Saved File:{} to "+path).format(uploaded_file.name))

#3-0.1.生成blf和dbc中header+length都匹配的blf中的header_list
# todo:这段没有使用
def parser_header_list(dbc_header_len,blf_file):
    # create un-dulicated header_set that makes blf and db matched
    header_set=set()                                    #[1071,1072]
    row=0
    with can.BLFReader(blf_file) as can_log:
        for msg in can_log:
            if (msg.arbitration_id in s_combine_db.keys()) and (msg.dlc==s_combine_db[msg.arbitration_id]):   #只对db中有的数据进行解析
                header_set.add(msg.arbitration_id)
            row+=1
    header_list = list(header_set)
    st.write('parser_header_list: '+str(time.time()))
    return (row,header_list)

# decode中的raw data需要从bytesarray2str
def bytesarray2str(bytesarrary):
    '''
    convert raw data from bytesarray to string
    '''
    str=''
    for d in bytesarrary:
        str=str+' '+hex(d)
    return str



# # 1. as sidebar menu
#   1-1. Add on_change callback
def on_change(key):
    selection = st.session_state[key]
    #st.write(f"Selection changed to {selection}")  #用来检测返回的菜单名字
    return selection   #返回selection，string类型，用来告知用户选择的是哪个menu

# 1-2.create a sidebar as menu
with st.sidebar:
    #selected接受option_menu中on_change的返回值string，用此判断在哪个page上
    #selected = option_menu("Main Menu", ["Database", "Decode", 'Analyse',"Help"],
    #                       icons=['house', 'cloud-upload', "gear", 'list-task'],
    #                       on_change=on_change, key='menu_1',menu_icon="cast", default_index=1)
    #patrick added
    selected='Database'
    
#2. design home page
if selected=='Database':
    st.title('报文库管理')
    #new_title = '<p style="font-family:sans-serif; color:Red; font-size: 30px;">Powered By Model Group</p>'
    #st.markdown(new_title, unsafe_allow_html=True)
    st.markdown('### Powered By Model Group')

    uploaded_dbc = st.file_uploader("Choose a dbc file",
                                     type=['dbc'],
                                     accept_multiple_files=False,
                                     )
    
    db_path = '/home/magna/data/big_data/database/dbc/'    

    if uploaded_dbc:
        # get file 的唯一标识id name size等
        dbc_identity = [uploaded_dbc.file_id, uploaded_dbc.name, uploaded_dbc.size]
        # st.write(uploaded_file.file_id,uploaded_file.name,uploaded_file.size)
        save_uploaded_file(uploaded_dbc, db_path)
