"""
# My first app
Here's our first attempt at using data to create a table:
    """

import streamlit as st
import pandas as pd
import numpy as np
from streamlit_option_menu import option_menu
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
    selected = option_menu("Main Menu", ["Database", "Decode", 'Analyse',"Help"],
                           icons=['house', 'cloud-upload', "gear", 'list-task'],
                           on_change=on_change, key='menu_1',menu_icon="cast", default_index=1)
    #patrick added
    #selected='Database'
    
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
'''
#3. design uploader page
if selected=='Decode':
    st.title('解析数据')
    st.markdown('### Powered By Model Group')


    # 3.1 先让用户选择dbc文件；add multi select widget for add multi-dbc
    #db_path = '/home/patrick/data/big_data/database/dbc/'   #笔记本上虚拟环境的路径
    db_path = '/home/magna/data/big_data/database/dbc/'   #wks上db路径    
    option_list = os.listdir(db_path)
    dbfs = st.multiselect(
        "Please choose a database to decode uploaded files",
        option_list
        # disabled=st.session_state.disabled#show_multi_select_widge,
        # key='multi_select'
    )

    # 3.2 解析库，得出dbc中包含的header list
    check_box_db_blf = st.checkbox('报文库选好了')
    #st.write(check_box_db_blf, dbfs)

    # 3-3.decode the current uploaded file
    

    # load database
    s_combine_db = {}
    if check_box_db_blf and dbfs:
        
        # dbf这里List中只有文件名，没有绝对路径，故此还要拼接出full_path
        dbfs_full_list = []
        for dbf_part in dbfs:
            dbf_full_path_str = db_path + dbf_part
            dbfs_full_list.append(dbf_full_path_str)

        db = cantools.db.load_file(dbfs_full_list[0])
        for file in dbfs_full_list[1:]:
            db.add_dbc_file(file)

        # 获取db中的msg.frameid and msg.length,用于确保blf与db中二者都匹配，才进行解析
        #s_combine_db = {}
        for msg in db.messages:
            dict = {msg.frame_id: msg.length}
            s_combine_db.update(dict)

        option_list = s_combine_db.keys()

        # 使用st.form来统一提交multiselect的结果，防止multi-select选择一个选项，马上rerun whole app
        with st.form('input'):
            header_list = st.multiselect(
                "请选择header/pt",
                option_list
                # st.session_state.option_list
                # disabled=st.session_state.disabled#show_multi_select_widge,
                # key='multi_select'
            )
            submit_button = st.form_submit_button(label='header选好了')

            if submit_button:
                # Check user chosen head list is not empty
                if header_list==[]:
                    st.error('WARNING: Please select header')
                # else:
                #     with st.spinner(text='Extracting information…'):
                #         sleep(3)


    #col1, col2 = st.columns(2)
    #3-1.file_loader
    #3-1-1.先做单文件上传和存储
    # todo:以后可以每天建立一个文件夹，当天传入当天文件夹，当天晚上运行spark
    uploaded_file = st.file_uploader("Choose a blf file",
                                      type=['blf','csv'],
                                      accept_multiple_files=False,
                                     )
    #path = '/tmp'  #笔记本虚拟机上blf路径
    path = '/home/magna/data/big_data/data_files/blf'   #wks上db路径
    
    if uploaded_file:
        # get file 的唯一标识id name size等
        file_identity = [uploaded_file.file_id, uploaded_file.name, uploaded_file.size]
        # st.write(uploaded_file.file_id,uploaded_file.name,uploaded_file.size)
        save_uploaded_file(uploaded_file, path)

        #开始按照dbc中的header对blf进行解析
        decoded_msg = []
        len_not_match={}
        blf_header=set()   #blf中有的header
        blf_dbc_header=list()   #header_list(dbc中存在，且被用户选择)，且在blf也存在的header

        blf_row_count=0
        with can.BLFReader(uploaded_file) as can_log:  #blf_file=uploaded_file
            for msg in can_log:
                #统计blf中的rows
                blf_row_count+=1

                #统计blf中的header
                blf_header.add(msg.arbitration_id)

                for header in header_list:
                    if (msg.arbitration_id == header):
                        if (msg.dlc == s_combine_db[msg.arbitration_id]):
                            decoded_dict = db.decode_message(msg.arbitration_id, msg.data)
                            result_dict = {}
                            for k, v in decoded_dict.items():
                                if (isinstance(v, (
                                        cantools.database.namedsignalvalue.NamedSignalValue))):  # 重要，cantools的数据类型不要用
                                    v = v.name
                                dict = {k: v}
                                result_dict.update(dict)
                            decoded_msg.append(
                                [msg.timestamp, datetime.fromtimestamp(msg.timestamp), hex(msg.arbitration_id), msg.channel,
                                 msg.dlc, bytesarray2str(msg.data), result_dict])

                        else:
                            #header匹配，但length不匹配，以后再改
                            len_not_match.update({msg.arbitration_id:msg.dlc})

        for header in header_list:
            if header in blf_header:
                blf_dbc_header.append(header)



        #st.write('below headers are not in data file: '+str(len_not_match.keys()))

        # create db save result list
        # df = pd.DataFrame(data=decoded_msg, columns=['timestamp', 'arbitration_id', 'channel', 'dlc', 'data', 'decode'])
        df = pd.DataFrame(data=decoded_msg,
                          columns=['timestamp', 'timestamp_conv',
                                   'arbitration_id', 'channel', 'dlc',
                                   'data', 'decode'])

        # todo:将df写出来，正式环境不用，后续可以加上
        # limit the preview lines
        #limit = 2000
        #st.write(df[:limit])

        ### todo:将df存入mysql，然后通过metabase进行查询
        with sqlEngine.begin() as dbConnection:  # 不需要写dbConnection.close()，会自动关闭
            for header in blf_dbc_header: #header_list
                df_header = df.loc[df['arbitration_id'] == str(hex(header)), :]

                list = []

                for index, row in df_header.iterrows():
                    d = {'pt': row['arbitration_id'], 'raw_data': row['data'], 'channel': row['channel'],
                         'dlc': row['dlc'], 'timestamp': row['timestamp'],
                         'timestamp_conv': row['timestamp_conv']}
                    row['decode'].update(d)
                    list.append(row['decode'])

                df1 = pd.DataFrame(list)
                #tableName = 'cantools_' + str(hex(header))[2:]  # substring header to delete 0x开头
                tableName = 'can_' + str(hex(header))[2:]  # substring header to delete 0x开头

                # 调用函数读写mysql
                # mysql_read_write(sqlEngine,tableName,df1)
                #mysql_read_write(dbConnection, tableName, df1)

                st.write(tableName + ' data have been transferred to big data platform.')

        #new_title = '<p style="font-family:sans-serif; color:Red; font-size: 42px;">New image</p>'
        #st.markdown(new_title, unsafe_allow_html=True)
        st.success('Summary: '+str(blf_row_count)+' rows parsed.')


# 4. design analyse page
if selected=='Analyse':
    st.title('分析文件')
    st.markdown('### Powered By Model Group')
    st.link_button("Go to Analyse Platform", "http://192.168.163.128:3000/")

# 5. design help page
if selected == 'Help':
    st.title('帮助')
    st.markdown('### Powered By Model Group')
'''





