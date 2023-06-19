from snowflake.snowpark import Session

import snowflake.connector

import streamlit as st




username = ""
password = ""
account = ""
database = ""

def call_stored_procedure(username, password, account, database):

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



    df = session.sql("SELECT * FROM MYTABLE")



    st.table(df)



    # Fetch the result set

    #result = cursor.fetchall()



    # Print the results

    # for row in result:

    #     st.write(row)



    # Close the cursor and connection





def call_example(username, password, account, database):

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



    df = session.sql("SELECT NAME FROM MYTABLE WHERE NAME LIKE 'Example%'")



    st.table(df)










def call_data_procedure(name, pet, username, password, account, database):

    if name=="" or pet=="" or username=="" or password=="" or account=="" or database=="":
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
        insert_query = f"INSERT INTO MYTABLE (NAME, PET) VALUES ('{name}', '{pet}')"
        cursor.execute(insert_query)

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

    st.title("Call Stored Procedure from Snowflake")

    st.write("Click the button to call the stored procedure")

    if st.button("Call Procedure"):
        call_stored_procedure(username, password, account, database)

    if st.button("Call Names with 'Example' prefix"):
        call_example(username, password, account, database)

    with st.container():
        col1, col2 = st.columns(2)

        with col1:
            name = st.text_input("Name")

        with col2:
            pet = st.text_input("Pet")

        if st.button("Insert Data"):
            call_data_procedure(name, pet, username, password, account, database)

        if st.button("Function Call"):
            hello()



def login():
    st.markdown("""
    <style>
        [data-testid=stSidebar] {
            background-image: linear-gradient(white, grey);
        }
    </style>
    """, unsafe_allow_html=True)

    username = st.text_input("Your Username")
    password = st.text_input("Your Password", type="password")
    account = st.text_input("Your Account-name")
    database = st.text_input("Your Database-name")

    submit_button = st.button("Set Parameters")


    if submit_button:
        st.session_state['username'] = username
        st.session_state['password'] = password
        st.session_state['account'] = account
        st.session_state['database'] = database
        st.session_state['authentication_status'] = True




if __name__ == "__main__":
    main()