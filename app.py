from snowflake.snowpark import Session

import snowflake.connector

import streamlit as st

from streamlit_card import card





username = ""
password = ""
account = ""
database = ""



def table_schema(username, password, account, database, schema, table):
    con = {
        "account": account,
        "user": username,
        "password": password,
        "role": "ACCOUNTADMIN",
        "warehouse": "COMPUTE_WH",
        "database": database,
        "schema": schema
    }

    session = Session.builder.configs(con).create()

    try:
        result = session.sql(f"DESCRIBE TABLE {schema}.{table}").collect()
        variables = [row["name"] for row in result]
        return variables
    except Exception as e:
        print(f"Error: {e}")




def show_tables(username, password, account, database, schema):
    con = {
        "account": account,
        "user": username,
        "password": password,
        "role": "ACCOUNTADMIN",
        "warehouse": "COMPUTE_WH",
        "database": database,
        "schema": schema
    }

    session = Session.builder.configs(con).create()
    
    try:
        result = session.sql(f"SHOW TABLES IN SCHEMA {schema}").collect()
        tables = [row[1] for row in result]
        return tables
    except Exception as e:
        print(f"Error: {e}")



def show_schemas(username, password, account):
    con = {
        "account": account,
        "user": username,
        "password": password,
        "role": "ACCOUNTADMIN",
        "warehouse": "COMPUTE_WH",
        "schema": "ACCOUNT_USAGE",
        "database": "SNOWFLAKE"
    }

    session = Session.builder.configs(con).create()
    
    try:
        result = session.sql('SELECT DISTINCT SCHEMA_NAME FROM "SNOWFLAKE"."ACCOUNT_USAGE"."SCHEMATA";').collect()
        schemas = [row[0] for row in result]
        return schemas

    except Exception as e:
        print(f"Error: {e}")



def show_databases(username, password, account):
    con = {
        "account": account,
        "user": username,
        "password": password,
        "role": "ACCOUNTADMIN",
        "warehouse": "COMPUTE_WH",
        "schema": "PUBLIC"
    }

    session = Session.builder.configs(con).create()
    
    try:
        result = session.sql("SHOW DATABASES IN ACCOUNT").collect()
        databases = [row["name"] for row in result if row["name"] not in ["SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"]]
        return databases

    except Exception as e:
        print(f"Error: {e}")






def call_stored_procedure(username, password, account, database, table):

    # Establish a connection

    con = {

        "account" : account,
        "user" : username,
        "password" : password,
        "role" : "ACCOUNTADMIN",
        "warehouse" : "COMPUTE_WH",
        "database" : database,
        "schema" : "PUBLIC" 

    }



    



    session = Session.builder.configs(con).create()



    # Call the stored procedure using Snowpark

    #result_df = session.execute("CALL YourStoredProcedure()")



    df = session.sql(f"SELECT * FROM {table}")



    st.table(df)



    # Fetch the result set

    #result = cursor.fetchall()



    # Print the results

    # for row in result:

    #     st.write(row)



    # Close the cursor and connection





def call_data_procedure(variables, values, username, password, account, database, table):

    if username=="" or password=="" or account=="" or database=="":
        st.write("Value empty")

    else:
        # Establish a connection
        con = snowflake.connector.connect(
            account=account,
            user=username,
            password=password,
            role="ACCOUNTADMIN",
            warehouse="COMPUTE_WH",
            database=database,
            schema="PUBLIC" 
        )

        cursor = con.cursor()

        # Execute the SQL statement to insert the data
        insert_query = f"INSERT INTO {table} ({', '.join(variables)}) VALUES ({', '.join(['%s' for _ in variables])})"
        cursor.execute(insert_query, values)

        # Commit the transaction
        con.commit()


        st.write("Successfully inserted")





# Streamlit app
def main():



    if 'authentication_status' not in st.session_state:
        st.session_state['authentication_status'] = False

    if st.session_state['authentication_status']:
        welcome()
    else:
        login()

def hello():
    st.write("Hello")

def welcome():

    username = st.session_state.get('username', '')
    password = st.session_state.get('password', '')
    account = st.session_state.get('account', '')
    database = st.session_state.get('database', '')
    schema = st.session_state.get('schema', '')
    table = st.session_state.get('table', '')
    

    st.title("Call Stored Procedure from Snowflake")



    #databases
    database_list  = show_databases(username, password, account)

    selected_database = st.selectbox("Select a database", database_list)

    #schemas
    schemas_list = show_schemas(username, password, account)

    selected_schemas = st.selectbox("Select a Schema", schemas_list)

    #tables
    table_list = show_tables(username, password, account, database, schema)

    selected_table = st.selectbox("Select a Table", table_list)



    st.session_state['database'] = selected_database
    st.session_state['schema'] = selected_schemas
    st.session_state['table'] = selected_table

    st.markdown(
        """<style>
        div.stButton > button:first-child {
            background-color: black;
            color: white;
            font-size: 20px;
            border-radius: 10px;
        }
        </style>""",
        unsafe_allow_html=True
    )
        
    

    st.write("Click the button to call the stored procedure")
    if st.button("Call Procedure"):
        call_stored_procedure(username, password, account, database, table)


    with st.container():

        variables = table_schema(username, password, account, database, schema, table)
        values=[]

        for variable in variables:
            value = st.text_input(variable)
            values.append(value)
        

        if st.button("Insert Data"):
            call_data_procedure(variables, values, username, password, account, database, table)

        if st.button("Function Call"):
            hello()






def login():

    
    page_bg_img = f"""
<style>

[data-testid="stAppViewContainer"] {{
background-color: white;
background-position: center; 
background-repeat: no-repeat;
background-attachment: fixed;
}}

input[type="text"], input[type="password"] {{
            background-color: white;
            color: black;
            border: 3px solid grey;
            padding: 8px 12px;
            border-radius: 10px;
        }}


</style>
"""
    
    st.markdown(page_bg_img, unsafe_allow_html=True)
    
    st.title("Login Page")  


    with st.container():
        
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        account = st.text_input("Account-name")

        submit_button = st.button("Login")

        if submit_button:
            st.session_state['username'] = username
            st.session_state['password'] = password
            st.session_state['account'] = account
            st.session_state['authentication_status'] = True




if __name__ == "__main__":
    main()